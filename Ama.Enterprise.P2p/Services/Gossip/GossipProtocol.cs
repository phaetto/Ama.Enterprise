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
/// Orchestrates the Gossip protocol algorithm across multiple meshes.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="GossipProtocol"/> class.
/// </remarks>
public sealed class GossipProtocol(
    IServiceProvider serviceProvider,
    IEnumerable<P2pMeshMetadata> meshes,
    IOptionsMonitor<GossipOptions> gossipOptionsMonitor,
    IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
    ILogger<GossipProtocol> logger) : IP2pProtocol, IDisposable
{
    private readonly IServiceProvider serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    private readonly IEnumerable<P2pMeshMetadata> meshes = meshes ?? throw new ArgumentNullException(nameof(meshes));
    private readonly IOptionsMonitor<GossipOptions> gossipOptionsMonitor = gossipOptionsMonitor ?? throw new ArgumentNullException(nameof(gossipOptionsMonitor));
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
    private readonly ILogger<GossipProtocol> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    private readonly ConcurrentDictionary<string, MeshState> activeMeshes = new();

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
                serviceProvider.GetRequiredKeyedService<ITransportRouter>(meshId),
                serviceProvider.GetRequiredKeyedService<IInboundMessageQueue<GossipMessage>>(meshId),
                serviceProvider.GetRequiredKeyedService<IPeerSelector>(meshId),
                serviceProvider.GetRequiredKeyedService<IApplicationPayloadDispatcher>(meshId),
                serviceProvider.GetRequiredKeyedService<IFailureDetector>(meshId),
                serviceProvider.GetRequiredService<IPeerRegistry>()
            );

            state.LoopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            state.InboundLoopTask = Task.Run(() => ProcessInboundQueueAsync(meshId, state, state.LoopCts.Token), state.LoopCts.Token);
            state.BackgroundLoopTask = Task.Run(() => GossipLoopAsync(meshId, state, state.LoopCts.Token), state.LoopCts.Token);
            state.HealthCheckLoopTask = Task.Run(() => HealthCheckLoopAsync(meshId, state, state.LoopCts.Token), state.LoopCts.Token);

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

                    if (state.HealthCheckLoopTask is not null)
                    {
                        try
                        {
                            await state.HealthCheckLoopTask.ConfigureAwait(false);
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

            logger.LogDebug("[{MeshId}] Wrapping payload and broadcasting newly mapped envelope {MessageId} locally from {NodeId}.", meshId, message.MessageId, nodeOptions.LocalPeerId);

            state.SeenMessages.TryAdd(message.MessageId, DateTimeOffset.UtcNow);
            state.MessageQueue.Enqueue(message);

            dispatchTasks.Add(state.Dispatcher.DispatchAsync(message.MeshId, message.SenderId, message.Payload, cancellationToken));
        }

        return Task.WhenAll(dispatchTasks);
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
            logger.LogError(ex, "[{MeshId}] An error occurred while processing the inbound algorithm message queue.", meshId);
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
                logger.LogError(ex, "[{MeshId}] An error occurred during the algorithm network gossip tick.", meshId);
            }
        }
    }

    private async Task HealthCheckLoopAsync(string meshId, MeshState state, CancellationToken cancellationToken)
    {
        var checkInterval = TimeSpan.FromSeconds(5);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(checkInterval, cancellationToken).ConfigureAwait(false);
                
                var peers = await state.PeerRegistry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
                foreach (var peer in peers)
                {
                    var health = await state.FailureDetector.EvaluatePeerHealthAsync(peer.Id, cancellationToken).ConfigureAwait(false);
                    
                    if (health == PeerStatus.Dead)
                    {
                        logger.LogInformation("[{MeshId}] Peer {PeerId} marked as Dead by failure detector. Removing from registry.", meshId, peer.Id.Value);
                        await state.PeerRegistry.RemovePeerAsync(meshId, peer.Id, cancellationToken).ConfigureAwait(false);
                    }
                    else if (health == PeerStatus.Suspect)
                    {
                        await state.PeerRegistry.AddOrUpdatePeerAsync(meshId, peer, PeerStatus.Suspect, cancellationToken).ConfigureAwait(false);
                    }
                    else 
                    {
                        await state.PeerRegistry.AddOrUpdatePeerAsync(meshId, peer, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] An error occurred during the algorithm health check tick.", meshId);
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
                        logger.LogWarning(ex, "[{MeshId}] Failed to send algorithm envelope {MessageId} to peer.", meshId, currentMessage.MessageId);
                    }
                }, cancellationToken));
            }
        }

        await Task.WhenAll(sendTasks).ConfigureAwait(false);
    }

    private async Task HandleIncomingMessageAsync(string meshId, MeshState state, GossipMessage message)
    {
        try
        {
            var token = state.LoopCts?.Token ?? default;
            await state.FailureDetector.RecordHeartbeatAsync(message.SenderId, token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to record heartbeat for peer {PeerId}.", meshId, message.SenderId.Value);
        }

        if (state.SeenMessages.TryAdd(message.MessageId, DateTimeOffset.UtcNow))
        {
            logger.LogDebug("[{MeshId}] Received new payload {MessageId} from {SenderId}. TTL: {Ttl}", meshId, message.MessageId, message.SenderId.Value, message.TimeToLive);

            try
            {
                if (state.LoopCts is not null)
                {
                    await state.Dispatcher.DispatchAsync(message.MeshId, message.SenderId, message.Payload, state.LoopCts.Token).ConfigureAwait(false);
                }

                if (message.TimeToLive > 1)
                {
                    var forwardedMessage = message with { TimeToLive = message.TimeToLive - 1 };
                    state.MessageQueue.Enqueue(forwardedMessage);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Error pushing generic unwrapped payload into localized domain {MessageId}.", meshId, message.MessageId);
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

    private sealed class MeshState(
        ITransportRouter transportRouter,
        IInboundMessageQueue<GossipMessage> inboundQueue,
        IPeerSelector peerSelector,
        IApplicationPayloadDispatcher dispatcher,
        IFailureDetector failureDetector,
        IPeerRegistry peerRegistry) : IDisposable
    {
        public ITransportRouter TransportRouter { get; } = transportRouter;
        public IInboundMessageQueue<GossipMessage> InboundQueue { get; } = inboundQueue;
        public IPeerSelector PeerSelector { get; } = peerSelector;
        public IApplicationPayloadDispatcher Dispatcher { get; } = dispatcher;
        public IFailureDetector FailureDetector { get; } = failureDetector;
        public IPeerRegistry PeerRegistry { get; } = peerRegistry;

        public ConcurrentDictionary<Guid, DateTimeOffset> SeenMessages { get; } = new();
        public ConcurrentQueue<GossipMessage> MessageQueue { get; } = new();

        public CancellationTokenSource? LoopCts { get; set; }
        public Task? BackgroundLoopTask { get; set; }
        public Task? InboundLoopTask { get; set; }
        public Task? HealthCheckLoopTask { get; set; }

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