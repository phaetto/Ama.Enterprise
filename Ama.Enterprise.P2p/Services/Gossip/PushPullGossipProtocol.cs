namespace Ama.Enterprise.P2p.Services.Gossip;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
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
/// Orchestrates the Push-Pull Gossip protocol algorithm across multiple meshes.
/// Supports deterministic anti-entropy through explicitly targeted push digest and pull request mechanics.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="PushPullGossipProtocol"/> class.
/// </remarks>
public sealed class PushPullGossipProtocol : IP2pProtocol, IDisposable
{
    private readonly IServiceProvider serviceProvider;
    private readonly IEnumerable<P2pMeshMetadata> meshes;
    private readonly IOptionsMonitor<PushPullGossipOptions> pushPullOptionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly ILogger<PushPullGossipProtocol> logger;

    private readonly ConcurrentDictionary<string, MeshState> activeMeshes = new();

    private readonly Meter meter;
    private readonly Counter<long> messagesSentCounter;
    private readonly Counter<long> messagesReceivedCounter;
    private readonly Counter<long> digestsSentCounter;
    private readonly Counter<long> pullRequestsSentCounter;
    private readonly Counter<long> directFulfillmentsCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    public PushPullGossipProtocol(
        IServiceProvider serviceProvider,
        IEnumerable<P2pMeshMetadata> meshes,
        IOptionsMonitor<PushPullGossipOptions> pushPullOptionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        ILogger<PushPullGossipProtocol> logger)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.meshes = meshes ?? throw new ArgumentNullException(nameof(meshes));
        this.pushPullOptionsMonitor = pushPullOptionsMonitor ?? throw new ArgumentNullException(nameof(pushPullOptionsMonitor));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var meterFactory = serviceProvider.GetService<IMeterFactory>();
        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.PushPullGossip") 
            ?? new Meter("Ama.Enterprise.P2p.PushPullGossip");

        this.messagesSentCounter = this.meter.CreateCounter<long>(
            "p2p.pushpull.messages_sent", 
            "messages", 
            "Total messages dispatched through push-pull gossip");
            
        this.messagesReceivedCounter = this.meter.CreateCounter<long>(
            "p2p.pushpull.messages_received", 
            "messages", 
            "Total inbound messages received by push-pull gossip");
            
        this.digestsSentCounter = this.meter.CreateCounter<long>(
            "p2p.pushpull.digests_sent", 
            "digests", 
            "Total periodic digest structures explicitly dispatched");
            
        this.pullRequestsSentCounter = this.meter.CreateCounter<long>(
            "p2p.pushpull.pull_requests_sent", 
            "requests", 
            "Total explicit pull requests targeting missing payloads");
            
        this.directFulfillmentsCounter = this.meter.CreateCounter<long>(
            "p2p.pushpull.direct_fulfillments", 
            "messages", 
            "Total missing explicit payloads fulfilled point-to-point");
            
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>(
            "p2p.pushpull.payload_bytes", 
            "bytes", 
            "Size of outbound payload in bytes");

        this.meter.CreateObservableGauge(
            "p2p.pushpull.queue_size", 
            () => activeMeshes.Select(kvp => new Measurement<int>(
                kvp.Value.MessageQueue.Count, 
                new KeyValuePair<string, object?>("mesh_id", kvp.Key))), 
            "messages", 
            "Current depth of the outbound network queue");

        this.meter.CreateObservableGauge(
            "p2p.pushpull.cache_size", 
            () => activeMeshes.Select(kvp => new Measurement<int>(
                kvp.Value.MessageCache.Count, 
                new KeyValuePair<string, object?>("mesh_id", kvp.Key))), 
            "messages", 
            "Current count of temporarily cached messages available for fulfillment");
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
            logger.LogInformation("[{MeshId}] Starting Push-Pull Gossip Protocol for node {NodeId}...", meshId, nodeOptions.LocalPeerId);

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
            state.PushPullLoopTask = Task.Run(() => PushPullLoopAsync(meshId, state, state.LoopCts.Token), state.LoopCts.Token);

            activeMeshes.TryAdd(meshId, state);

            logger.LogInformation("[{MeshId}] Push-Pull Gossip Protocol algorithm orchestrator started successfully.", meshId);
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
                logger.LogInformation("[{MeshId}] Stopping Push-Pull Gossip Protocol algorithm orchestrator...", meshId);

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

                    if (state.PushPullLoopTask is not null)
                    {
                        try
                        {
                            await state.PushPullLoopTask.ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) { }
                    }

                    state.Dispose();

                    logger.LogInformation("[{MeshId}] Push-Pull Gossip Protocol orchestrator stopped cleanly.", meshId);
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
            var options = pushPullOptionsMonitor.Get(meshId);
            var nodeOptions = nodeOptionsMonitor.Get(meshId);

            var message = new GossipMessage(
                meshId,
                Constants.ProtocolVersion,
                Guid.NewGuid(),
                new PeerId(nodeOptions.LocalPeerId),
                options.DefaultTimeToLive,
                GossipMessageType.Broadcast,
                null,
                payload);

            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("message_type", "broadcast") };
            messagesSentCounter.Add(1, tags);
            payloadBytesHistogram.Record(payload.Length, tags);

            logger.LogDebug("[{MeshId}] Wrapping payload and broadcasting newly mapped envelope {MessageId} locally from {NodeId}.", meshId, message.MessageId, nodeOptions.LocalPeerId);

            state.SeenMessages.TryAdd(message.MessageId, DateTimeOffset.UtcNow);
            state.MessageCache.TryAdd(message.MessageId, message);
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
        meter.Dispose();
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
                var options = pushPullOptionsMonitor.Get(meshId);
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

    private async Task PushPullLoopAsync(string meshId, MeshState state, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var options = pushPullOptionsMonitor.Get(meshId);
                if (!options.EnablePushPull)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
                    continue;
                }

                await Task.Delay(options.PushPullInterval, cancellationToken).ConfigureAwait(false);

                await PerformPushPullTickAsync(meshId, state, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] An error occurred during the push-pull digest tick.", meshId);
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

        var options = pushPullOptionsMonitor.Get(meshId);
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

    private async Task PerformPushPullTickAsync(string meshId, MeshState state, CancellationToken cancellationToken)
    {
        var options = pushPullOptionsMonitor.Get(meshId);
        var peers = (await state.PeerSelector.GetPeersAsync(1, cancellationToken).ConfigureAwait(false)).ToList();
        
        if (peers.Count == 0) return;

        var targetPeer = peers[0];
        var recentIds = state.MessageCache.Keys.Take(options.MaxDigestSize).ToArray();
        
        if (recentIds.Length == 0) return;

        var nodeOptions = nodeOptionsMonitor.Get(meshId);

        var digestMessage = new GossipMessage(
            meshId,
            Constants.ProtocolVersion,
            Guid.NewGuid(),
            new PeerId(nodeOptions.LocalPeerId),
            1,
            GossipMessageType.Digest,
            recentIds,
            ReadOnlyMemory<byte>.Empty
        );

        try
        {
            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
            digestsSentCounter.Add(1, tags);
            await state.TransportRouter.SendAsync(targetPeer.Endpoint, digestMessage, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to send push-pull digest boundary locally.", meshId);
        }
    }

    private async Task HandleIncomingMessageAsync(string meshId, MeshState state, GossipMessage message)
    {
        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("message_type", message.MessageType.ToString()) };
        messagesReceivedCounter.Add(1, tags);

        try
        {
            var token = state.LoopCts?.Token ?? default;
            await state.FailureDetector.RecordHeartbeatAsync(message.SenderId, token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to record heartbeat for peer {PeerId}.", meshId, message.SenderId.Value);
        }

        if (message.MessageType == GossipMessageType.Digest)
        {
            await HandleDigestAsync(meshId, state, message).ConfigureAwait(false);
            return;
        }

        if (message.MessageType == GossipMessageType.PullRequest)
        {
            await HandlePullRequestAsync(meshId, state, message).ConfigureAwait(false);
            return;
        }

        if (state.SeenMessages.TryAdd(message.MessageId, DateTimeOffset.UtcNow))
        {
            logger.LogDebug("[{MeshId}] Received new payload {MessageId} from {SenderId}. TTL: {Ttl}", meshId, message.MessageId, message.SenderId.Value, message.TimeToLive);

            state.MessageCache.TryAdd(message.MessageId, message);

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

    private async Task HandleDigestAsync(string meshId, MeshState state, GossipMessage message)
    {
        if (message.DigestIds is null || message.DigestIds.Length == 0) return;

        var missingIds = new List<Guid>();
        foreach (var id in message.DigestIds)
        {
            if (!state.SeenMessages.ContainsKey(id))
            {
                missingIds.Add(id);
            }
        }

        if (missingIds.Count > 0)
        {
            var nodeOptions = nodeOptionsMonitor.Get(meshId);
            var pullRequest = new GossipMessage(
                meshId,
                Constants.ProtocolVersion,
                Guid.NewGuid(),
                new PeerId(nodeOptions.LocalPeerId),
                1,
                GossipMessageType.PullRequest,
                missingIds.ToArray(),
                ReadOnlyMemory<byte>.Empty
            );

            var peers = await state.PeerRegistry.GetAllPeersAsync(meshId, CancellationToken.None).ConfigureAwait(false);
            var peerMatches = peers.Where(p => p.Id.Equals(message.SenderId)).ToList();
            
            if (peerMatches.Count == 0 || peerMatches[0].Endpoint is null)
            {
                logger.LogWarning("[{MeshId}] Received digest from unknown peer {PeerId}. Cannot send pull request.", meshId, message.SenderId.Value);
                return;
            }

            var peer = peerMatches[0];
            logger.LogDebug("[{MeshId}] Requesting {Count} missing payloads via PullRequest from {PeerId}.", meshId, missingIds.Count, message.SenderId.Value);
            
            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
            pullRequestsSentCounter.Add(1, tags);

            await state.TransportRouter.SendAsync(peer.Endpoint, pullRequest, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task HandlePullRequestAsync(string meshId, MeshState state, GossipMessage message)
    {
        if (message.DigestIds is null || message.DigestIds.Length == 0) return;

        var peers = await state.PeerRegistry.GetAllPeersAsync(meshId, CancellationToken.None).ConfigureAwait(false);
        var peerMatches = peers.Where(p => p.Id.Equals(message.SenderId)).ToList();

        if (peerMatches.Count == 0 || peerMatches[0].Endpoint is null)
        {
            logger.LogWarning("[{MeshId}] Received pull request from unknown peer {PeerId}.", meshId, message.SenderId.Value);
            return;
        }

        var peer = peerMatches[0];
        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };

        foreach (var requestedId in message.DigestIds)
        {
            if (state.MessageCache.TryGetValue(requestedId, out var cachedMessage))
            {
                try
                {
                    logger.LogDebug("[{MeshId}] Fulfilling PullRequest by sending payload {MessageId} directly to {PeerId}.", meshId, cachedMessage.MessageId, message.SenderId.Value);
                    
                    var directMessage = cachedMessage with { TimeToLive = 1 };
                    directFulfillmentsCounter.Add(1, tags);
                    
                    await state.TransportRouter.SendAsync(peer.Endpoint, directMessage, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "[{MeshId}] Failed to fulfill pull request for message {MessageId} to peer {PeerId}.", meshId, cachedMessage.MessageId, message.SenderId.Value);
                }
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
                state.MessageCache.TryRemove(kvp.Key, out _);
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
        public ConcurrentDictionary<Guid, GossipMessage> MessageCache { get; } = new();
        public ConcurrentQueue<GossipMessage> MessageQueue { get; } = new();

        public CancellationTokenSource? LoopCts { get; set; }
        public Task? BackgroundLoopTask { get; set; }
        public Task? InboundLoopTask { get; set; }
        public Task? HealthCheckLoopTask { get; set; }
        public Task? PushPullLoopTask { get; set; }

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