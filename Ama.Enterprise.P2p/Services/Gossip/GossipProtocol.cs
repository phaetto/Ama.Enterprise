namespace Ama.Enterprise.P2p.Services.Gossip;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Orchestrates the Gossip protocol universally across all configured meshes, managing the background sync loops,
/// message deduplication, and delegating to generic transport routing dynamically based on mesh context.
/// </summary>
public sealed class GossipProtocol : IP2pProtocol, IDisposable
{
    private readonly IServiceProvider serviceProvider;
    private readonly IEnumerable<P2pMeshMetadata> meshes;
    private readonly IOptionsMonitor<GossipOptions> gossipOptionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly ILogger<GossipProtocol> logger;

    private readonly ConcurrentDictionary<string, MeshState> activeMeshes = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="GossipProtocol"/> class.
    /// </summary>
    public GossipProtocol(
        IServiceProvider serviceProvider,
        IEnumerable<P2pMeshMetadata> meshes,
        IOptionsMonitor<GossipOptions> gossipOptionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        ILogger<GossipProtocol> logger)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.meshes = meshes ?? throw new ArgumentNullException(nameof(meshes));
        this.gossipOptionsMonitor = gossipOptionsMonitor ?? throw new ArgumentNullException(nameof(gossipOptionsMonitor));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
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

            var state = new MeshState(
                serviceProvider.GetRequiredKeyedService<ITransportRouter<GossipMessage>>(meshId),
                serviceProvider.GetRequiredKeyedService<IInboundMessageQueue<GossipMessage>>(meshId),
                serviceProvider.GetRequiredKeyedService<IPeerSelector>(meshId),
                serviceProvider.GetRequiredKeyedService<IMessageDispatcher<GossipMessage>>(meshId)
            );

            state.LoopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            state.InboundLoopTask = Task.Run(() => ProcessInboundQueueAsync(meshId, state, state.LoopCts.Token), state.LoopCts.Token);
            state.BackgroundLoopTask = Task.Run(() => GossipLoopAsync(meshId, state, state.LoopCts.Token), state.LoopCts.Token);

            activeMeshes.TryAdd(meshId, state);

            logger.LogInformation("[{MeshId}] Gossip Protocol started successfully.", meshId);
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
                logger.LogInformation("[{MeshId}] Stopping Gossip Protocol...", meshId);

                stopTasks.Add(Task.Run(async () =>
                {
                    if (state.LoopCts is not null)
                    {
                        await state.LoopCts.CancelAsync().ConfigureAwait(false);
                    }

                    if (state.InboundLoopTask is not null)
                    {
                        try
                        {
                            await state.InboundLoopTask.ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) { }
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

                    logger.LogInformation("[{MeshId}] Gossip Protocol stopped.", meshId);
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
            throw new InvalidOperationException("Gossip Protocol is not actively running for any meshes.");
        }

        var dispatchTasks = new List<Task>();

        foreach (var kvp in activeMeshes)
        {
            var meshId = kvp.Key;
            var state = kvp.Value;
            var gossipOptions = gossipOptionsMonitor.Get(meshId);
            var nodeOptions = nodeOptionsMonitor.Get(meshId);

            var message = new GossipMessage(
                Guid.NewGuid(),
                new PeerId(nodeOptions.LocalPeerId),
                gossipOptions.DefaultTimeToLive,
                payload);

            logger.LogDebug("[{MeshId}] Broadcasting new message {MessageId} locally from node {NodeId}.", meshId, message.MessageId, nodeOptions.LocalPeerId);

            state.SeenMessages.TryAdd(message.MessageId, DateTimeOffset.UtcNow);
            state.MessageQueue.Enqueue(message);

            dispatchTasks.Add(state.Dispatcher.DispatchAsync(message, cancellationToken));
        }

        return Task.WhenAll(dispatchTasks);
    }

    private async Task ProcessInboundQueueAsync(string meshId, MeshState state, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in state.InboundQueue.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                await HandleIncomingMessageAsync(meshId, state, message).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] An error occurred while processing the inbound message queue.", meshId);
        }
    }

    private async Task GossipLoopAsync(string meshId, MeshState state, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var options = gossipOptionsMonitor.Get(meshId);
                await Task.Delay(options.GossipInterval, cancellationToken).ConfigureAwait(false);

                await PerformGossipTickAsync(meshId, state, cancellationToken).ConfigureAwait(false);
                
                CleanupSeenMessages(state);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] An error occurred during the gossip tick.", meshId);
            }
        }
    }

    private async Task PerformGossipTickAsync(string meshId, MeshState state, CancellationToken cancellationToken)
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
                        await state.TransportRouter.SendAsync(currentPeer.Endpoint, currentMessage, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "[{MeshId}] Failed to send message {MessageId} to peer endpoint.", meshId, currentMessage.MessageId);
                    }
                }, cancellationToken));
            }
        }

        await Task.WhenAll(sendTasks).ConfigureAwait(false);
    }

    private async Task HandleIncomingMessageAsync(string meshId, MeshState state, GossipMessage message)
    {
        if (state.SeenMessages.TryAdd(message.MessageId, DateTimeOffset.UtcNow))
        {
            logger.LogDebug("[{MeshId}] Received new gossip message {MessageId} from {SenderId}. TTL: {Ttl}", meshId, message.MessageId, message.SenderId.Value, message.TimeToLive);

            try
            {
                if (state.LoopCts is not null)
                {
                    await state.Dispatcher.DispatchAsync(message, state.LoopCts.Token).ConfigureAwait(false);
                }

                if (message.TimeToLive > 1)
                {
                    var forwardedMessage = message with { TimeToLive = message.TimeToLive - 1 };
                    state.MessageQueue.Enqueue(forwardedMessage);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Error processing incoming message {MessageId}.", meshId, message.MessageId);
            }
        }
    }

    private void CleanupSeenMessages(MeshState state)
    {
        var threshold = DateTimeOffset.UtcNow.AddMinutes(-5);
        foreach (var kvp in state.SeenMessages)
        {
            if (kvp.Value < threshold)
            {
                state.SeenMessages.TryRemove(kvp.Key, out _);
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
    }

    private sealed class MeshState : IDisposable
    {
        public ITransportRouter<GossipMessage> TransportRouter { get; }
        public IInboundMessageQueue<GossipMessage> InboundQueue { get; }
        public IPeerSelector PeerSelector { get; }
        public IMessageDispatcher<GossipMessage> Dispatcher { get; }

        public ConcurrentDictionary<Guid, DateTimeOffset> SeenMessages { get; } = new();
        public ConcurrentQueue<GossipMessage> MessageQueue { get; } = new();

        public CancellationTokenSource? LoopCts { get; set; }
        public Task? BackgroundLoopTask { get; set; }
        public Task? InboundLoopTask { get; set; }

        public MeshState(
            ITransportRouter<GossipMessage> transportRouter,
            IInboundMessageQueue<GossipMessage> inboundQueue,
            IPeerSelector peerSelector,
            IMessageDispatcher<GossipMessage> dispatcher)
        {
            TransportRouter = transportRouter;
            InboundQueue = inboundQueue;
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