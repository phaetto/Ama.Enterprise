namespace Ama.Enterprise.P2p.Services.Algorithms;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Orchestrates the Gossip protocol algorithm across multiple meshes.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="GossipAlgorithm"/> class.
/// </remarks>
public sealed class GossipAlgorithm : IP2pAlgorithm, IDisposable
{
    private readonly IServiceProvider serviceProvider;
    private readonly IEnumerable<P2pMeshMetadata> meshes;
    private readonly IOptionsMonitor<GossipOptions> gossipOptionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly ILogger<GossipAlgorithm> logger;

    private readonly ConcurrentDictionary<string, ProtocolState> activeMeshes = new();

    private readonly Meter meter;
    private readonly Counter<long> messagesSentCounter;
    private readonly Counter<long> messagesReceivedCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    public GossipAlgorithm(
        IServiceProvider serviceProvider,
        IEnumerable<P2pMeshMetadata> meshes,
        IOptionsMonitor<GossipOptions> gossipOptionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        ILogger<GossipAlgorithm> logger)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.meshes = meshes ?? throw new ArgumentNullException(nameof(meshes));
        this.gossipOptionsMonitor = gossipOptionsMonitor ?? throw new ArgumentNullException(nameof(gossipOptionsMonitor));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var meterFactory = serviceProvider.GetService<IMeterFactory>();
        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.Gossip") 
            ?? new Meter("Ama.Enterprise.P2p.Gossip");

        this.messagesSentCounter = this.meter.CreateCounter<long>(
            "p2p.gossip.messages_sent", 
            "messages", 
            "Total messages dispatched through gossip");
            
        this.messagesReceivedCounter = this.meter.CreateCounter<long>(
            "p2p.gossip.messages_received", 
            "messages", 
            "Total inbound messages received by gossip");
            
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>(
            "p2p.gossip.payload_bytes", 
            "bytes", 
            "Size of outbound payload in bytes");

        this.meter.CreateObservableGauge(
            "p2p.gossip.queue_size", 
            () => activeMeshes.Select(kvp => new Measurement<int>(
                kvp.Value.MessageQueue.Count, 
                new KeyValuePair<string, object?>("mesh_id", kvp.Key))), 
            "messages", 
            "Current depth of the outbound network queue");
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var mesh in meshes)
        {
            var meshId = mesh.MeshId;
            if (activeMeshes.ContainsKey(meshId))
            {
                continue;
            }

            var nodeOptions = nodeOptionsMonitor.Get(meshId);
            logger.LogInformation("[{MeshId}] Starting Gossip Protocol for node {NodeId}...", meshId, nodeOptions.LocalPeerId);

            var state = new ProtocolState(
                serviceProvider.GetRequiredKeyedService<ITransportRouter>(meshId),
                serviceProvider.GetRequiredKeyedService<IPeerSelector>(meshId),
                serviceProvider.GetRequiredKeyedService<IApplicationPayloadDispatcher>(meshId)
            );

            state.LoopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            state.BackgroundLoopTask = Task.Run(() => GossipLoopAsync(meshId, state, state.LoopCts.Token), state.LoopCts.Token);

            activeMeshes.TryAdd(meshId, state);

            logger.LogInformation("[{MeshId}] Gossip Protocol algorithm orchestrator started successfully.", meshId);
        }
        
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        var stopTasks = new List<Task>();
        var meshIds = activeMeshes.Keys.ToList();

        foreach (var meshId in meshIds)
        {
            if (activeMeshes.TryRemove(meshId, out var state))
            {
                logger.LogInformation("[{MeshId}] Stopping Gossip Protocol algorithm orchestrator...", meshId);

                stopTasks.Add(Task.Run(async () =>
                {
                    if (state.LoopCts is not null)
                    {
                        await state.LoopCts.CancelAsync().ConfigureAwait(false);
                    }

                    if (state.BackgroundLoopTask is not null)
                    {
                        try
                        {
                            await state.BackgroundLoopTask.ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) { }
                    }

                    state.Dispose();

                    logger.LogInformation("[{MeshId}] Gossip Protocol orchestrator stopped cleanly.", meshId);
                }, cancellationToken));
            }
        }

        await Task.WhenAll(stopTasks).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task BroadcastAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (payload.IsEmpty)
        {
            throw new ArgumentException("Payload cannot be empty.", nameof(payload));
        }

        if (payload.Length > Constants.MaximumPayloadSizeBytes)
        {
            throw new ArgumentException($"Payload exceeds maximum size of {Constants.MaximumPayloadSizeBytes} bytes.", nameof(payload));
        }

        if (activeMeshes.IsEmpty)
        {
            return Task.CompletedTask;
        }

        var dispatchTasks = new List<Task>();

        foreach (var kvp in activeMeshes)
        {
            var meshId = kvp.Key;
            var state = kvp.Value;
            var gossipOptions = gossipOptionsMonitor.Get(meshId);
            var nodeOptions = nodeOptionsMonitor.Get(meshId);

            var message = new GossipMessage(
                meshId,
                Constants.ProtocolVersion,
                Guid.NewGuid(),
                new PeerId(nodeOptions.LocalPeerId),
                gossipOptions.DefaultTimeToLive,
                payload);

            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("message_type", "broadcast") };
            messagesSentCounter.Add(1, tags);
            payloadBytesHistogram.Record(payload.Length, tags);

            logger.LogDebug("[{MeshId}] Wrapping payload and broadcasting newly mapped envelope {MessageId} locally from {NodeId}.", meshId, message.MessageId, nodeOptions.LocalPeerId);

            state.MessageQueue.Enqueue(message);

            dispatchTasks.Add(state.Dispatcher.DispatchAsync(message.MeshId, message.SenderId, message.Payload, cancellationToken));
        }

        return Task.WhenAll(dispatchTasks);
    }

    /// <inheritdoc />
    public async Task ProcessMessageAsync(IMeshMessage message, CancellationToken cancellationToken)
    {
        if (activeMeshes.TryGetValue(message.MeshId, out var state) && message is GossipMessage gossipMsg)
        {
            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", gossipMsg.MeshId) };
            messagesReceivedCounter.Add(1, tags);

            logger.LogDebug("[{MeshId}] Evaluating incoming mapped payload {MessageId} from {SenderId}. TTL: {Ttl}", gossipMsg.MeshId, gossipMsg.MessageId, gossipMsg.SenderId.Value, gossipMsg.TimeToLive);

            try
            {
                if (state.LoopCts is not null)
                {
                    await state.Dispatcher.DispatchAsync(gossipMsg.MeshId, gossipMsg.SenderId, gossipMsg.Payload, state.LoopCts.Token).ConfigureAwait(false);
                }

                if (gossipMsg.TimeToLive > 1)
                {
                    var forwardedMessage = gossipMsg with { TimeToLive = gossipMsg.TimeToLive - 1 };
                    state.MessageQueue.Enqueue(forwardedMessage);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Error pushing generic unwrapped payload into localized domain {MessageId}.", gossipMsg.MeshId, gossipMsg.MessageId);
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var state in activeMeshes.Values)
        {
            state.Dispose();
        }

        activeMeshes.Clear();
        meter.Dispose();
    }

    private async Task GossipLoopAsync(string meshId, ProtocolState state, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var options = gossipOptionsMonitor.Get(meshId);
                await Task.Delay(options.GossipInterval, cancellationToken).ConfigureAwait(false);

                await PerformGossipTickAsync(meshId, state, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] An error occurred during the algorithm network gossip tick.", meshId);
            }
        }
    }

    private async Task PerformGossipTickAsync(string meshId, ProtocolState state, CancellationToken cancellationToken)
    {
        if (state.MessageQueue.IsEmpty) return;

        var options = gossipOptionsMonitor.Get(meshId);
        var peers = (await state.PeerSelector.GetPeersAsync(options.Fanout, cancellationToken).ConfigureAwait(false)).ToList();
        
        if (peers.Count == 0) return;

        var messagesToForward = new List<GossipMessage>();
        while (state.MessageQueue.TryDequeue(out var msg))
        {
            messagesToForward.Add(msg);
        }

        var sendTasks = new List<Task>();
        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("message_type", "forward") };

        foreach (var peer in peers)
        {
            foreach (var message in messagesToForward)
            {
                var currentPeer = peer;
                var currentMessage = message;

                sendTasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        messagesSentCounter.Add(1, tags);
                        await state.TransportRouter.SendAsync(currentPeer.Endpoint, currentMessage, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "[{MeshId}] Failed to send algorithm envelope {MessageId} to peer.", meshId, currentMessage.MessageId);
                    }
                }, cancellationToken));
            }
        }

        await Task.WhenAll(sendTasks).ConfigureAwait(false);
    }

    private sealed class ProtocolState : IDisposable
    {
        public ITransportRouter TransportRouter { get; }
        public IPeerSelector PeerSelector { get; }
        public IApplicationPayloadDispatcher Dispatcher { get; }

        public ConcurrentQueue<GossipMessage> MessageQueue { get; } = new();

        public CancellationTokenSource? LoopCts { get; set; }
        public Task? BackgroundLoopTask { get; set; }

        public ProtocolState(ITransportRouter transportRouter, IPeerSelector peerSelector, IApplicationPayloadDispatcher dispatcher)
        {
            TransportRouter = transportRouter;
            PeerSelector = peerSelector;
            Dispatcher = dispatcher;
        }

        public void Dispose()
        {
            if (LoopCts is not null)
            {
                if (!LoopCts.IsCancellationRequested)
                {
                    LoopCts.Cancel();
                }

                LoopCts.Dispose();
            }
        }
    }
}