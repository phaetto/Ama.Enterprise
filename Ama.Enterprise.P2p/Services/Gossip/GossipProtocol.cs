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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Orchestrates the Gossip protocol for a specific mesh, managing the background sync loop,
/// message deduplication, and delegating to generic transport routing and inbound queueing.
/// </summary>
public sealed class GossipProtocol : IP2pProtocol, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<GossipOptions> gossipOptionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly ITransportRouter<GossipMessage> transportRouter;
    private readonly IInboundMessageQueue<GossipMessage> inboundQueue;
    private readonly IPeerSelector peerSelector;
    private readonly IMessageDispatcher<GossipMessage> dispatcher;
    private readonly ILogger<GossipProtocol> logger;

    private readonly ConcurrentDictionary<Guid, DateTimeOffset> seenMessages = new ConcurrentDictionary<Guid, DateTimeOffset>();
    private readonly ConcurrentQueue<GossipMessage> messageQueue = new ConcurrentQueue<GossipMessage>();
    
    private CancellationTokenSource? loopCts;
    private Task? backgroundLoopTask;
    private Task? inboundLoopTask;

    /// <summary>
    /// Initializes a new instance of the <see cref="GossipProtocol"/> class.
    /// </summary>
    public GossipProtocol(
        string meshId,
        IOptionsMonitor<GossipOptions> gossipOptionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        ITransportRouter<GossipMessage> transportRouter,
        IInboundMessageQueue<GossipMessage> inboundQueue,
        IPeerSelector peerSelector,
        IMessageDispatcher<GossipMessage> dispatcher,
        ILogger<GossipProtocol> logger)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.gossipOptionsMonitor = gossipOptionsMonitor ?? throw new ArgumentNullException(nameof(gossipOptionsMonitor));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.transportRouter = transportRouter ?? throw new ArgumentNullException(nameof(transportRouter));
        this.inboundQueue = inboundQueue ?? throw new ArgumentNullException(nameof(inboundQueue));
        this.peerSelector = peerSelector ?? throw new ArgumentNullException(nameof(peerSelector));
        this.dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        logger.LogInformation("[{MeshId}] Starting Gossip Protocol for node {NodeId}...", meshId, nodeOptions.LocalPeerId);

        loopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        inboundLoopTask = Task.Run(() => ProcessInboundQueueAsync(loopCts.Token), loopCts.Token);
        backgroundLoopTask = Task.Run(() => GossipLoopAsync(loopCts.Token), loopCts.Token);

        logger.LogInformation("[{MeshId}] Gossip Protocol started successfully.", meshId);
        
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("[{MeshId}] Stopping Gossip Protocol...", meshId);

        if (loopCts is not null)
        {
            await loopCts.CancelAsync().ConfigureAwait(false);
        }

        if (inboundLoopTask is not null)
        {
            try
            {
                await inboundLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }

        if (backgroundLoopTask is not null)
        {
            try
            {
                await backgroundLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }

        logger.LogInformation("[{MeshId}] Gossip Protocol stopped.", meshId);
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

        var gossipOptions = gossipOptionsMonitor.Get(meshId);
        var nodeOptions = nodeOptionsMonitor.Get(meshId);

        var message = new GossipMessage(
            Guid.NewGuid(),
            new PeerId(nodeOptions.LocalPeerId),
            gossipOptions.DefaultTimeToLive,
            payload);

        logger.LogDebug("[{MeshId}] Broadcasting new message {MessageId} locally from node {NodeId}.", meshId, message.MessageId, nodeOptions.LocalPeerId);

        seenMessages.TryAdd(message.MessageId, DateTimeOffset.UtcNow);
        messageQueue.Enqueue(message);

        return dispatcher.DispatchAsync(message, cancellationToken);
    }

    private async Task ProcessInboundQueueAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in inboundQueue.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                await HandleIncomingMessageAsync(message).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] An error occurred while processing the inbound message queue.", meshId);
        }
    }

    private async Task GossipLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var options = gossipOptionsMonitor.Get(meshId);
                await Task.Delay(options.GossipInterval, cancellationToken).ConfigureAwait(false);

                await PerformGossipTickAsync(cancellationToken).ConfigureAwait(false);
                
                CleanupSeenMessages();
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

    private async Task PerformGossipTickAsync(CancellationToken cancellationToken)
    {
        if (messageQueue.IsEmpty) return;

        var options = gossipOptionsMonitor.Get(meshId);
        var peers = (await peerSelector.GetPeersAsync(options.Fanout, cancellationToken).ConfigureAwait(false)).ToList();
        
        if (peers.Count == 0) return;

        var messagesToForward = new List<GossipMessage>();
        while (messageQueue.TryDequeue(out var msg))
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
                        await transportRouter.SendAsync(currentPeer.Endpoint, currentMessage, cancellationToken).ConfigureAwait(false);
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

    private async Task HandleIncomingMessageAsync(GossipMessage message)
    {
        if (seenMessages.TryAdd(message.MessageId, DateTimeOffset.UtcNow))
        {
            logger.LogDebug("[{MeshId}] Received new gossip message {MessageId} from {SenderId}. TTL: {Ttl}", meshId, message.MessageId, message.SenderId.Value, message.TimeToLive);

            try
            {
                if (loopCts is not null)
                {
                    await dispatcher.DispatchAsync(message, loopCts.Token).ConfigureAwait(false);
                }

                if (message.TimeToLive > 1)
                {
                    var forwardedMessage = message with { TimeToLive = message.TimeToLive - 1 };
                    messageQueue.Enqueue(forwardedMessage);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Error processing incoming message {MessageId}.", meshId, message.MessageId);
            }
        }
    }

    private void CleanupSeenMessages()
    {
        var threshold = DateTimeOffset.UtcNow.AddMinutes(-5);
        foreach (var kvp in seenMessages)
        {
            if (kvp.Value < threshold)
            {
                seenMessages.TryRemove(kvp.Key, out _);
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (loopCts is not null)
        {
            loopCts.Cancel();
            loopCts.Dispose();
        }
    }
}