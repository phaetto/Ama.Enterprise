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
/// message deduplication, and delegating to generic transport and dispatcher services.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="GossipProtocol"/> class.
/// </remarks>
public sealed class GossipProtocol(
    string meshId,
    IOptionsMonitor<GossipOptions> optionsMonitor,
    ITransport<GossipMessage> transport,
    ITransportListener<GossipMessage> listener,
    IPeerSelector peerSelector,
    IMessageDispatcher<GossipMessage> dispatcher,
    ILogger<GossipProtocol> logger) : IP2pProtocol, IDisposable
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IOptionsMonitor<GossipOptions> optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly ITransport<GossipMessage> transport = transport ?? throw new ArgumentNullException(nameof(transport));
    private readonly ITransportListener<GossipMessage> listener = listener ?? throw new ArgumentNullException(nameof(listener));
    private readonly IPeerSelector peerSelector = peerSelector ?? throw new ArgumentNullException(nameof(peerSelector));
    private readonly IMessageDispatcher<GossipMessage> dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
    private readonly ILogger<GossipProtocol> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    private readonly ConcurrentDictionary<Guid, DateTimeOffset> seenMessages = new ConcurrentDictionary<Guid, DateTimeOffset>();
    private readonly ConcurrentQueue<GossipMessage> messageQueue = new ConcurrentQueue<GossipMessage>();
    private readonly PeerId localPeerId = new PeerId(Guid.NewGuid());
    private CancellationTokenSource? loopCts;
    private Task? backgroundLoopTask;

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("[{MeshId}] Starting Gossip Protocol...", meshId);

        loopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await listener.StartListeningAsync(HandleIncomingMessageAsync, loopCts.Token).ConfigureAwait(false);

        backgroundLoopTask = Task.Run(() => GossipLoopAsync(loopCts.Token), loopCts.Token);

        logger.LogInformation("[{MeshId}] Gossip Protocol started successfully.", meshId);
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("[{MeshId}] Stopping Gossip Protocol...", meshId);

        if (loopCts is not null)
        {
            await loopCts.CancelAsync().ConfigureAwait(false);
        }

        await listener.StopListeningAsync(cancellationToken).ConfigureAwait(false);

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

        var options = optionsMonitor.Get(meshId);

        var message = new GossipMessage(
            Guid.NewGuid(),
            localPeerId,
            options.DefaultTimeToLive,
            payload);

        logger.LogDebug("[{MeshId}] Broadcasting new message {MessageId} locally.", meshId, message.MessageId);

        seenMessages.TryAdd(message.MessageId, DateTimeOffset.UtcNow);
        messageQueue.Enqueue(message);

        return dispatcher.DispatchAsync(message, cancellationToken);
    }

    private async Task GossipLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var options = optionsMonitor.Get(meshId);
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

        var options = optionsMonitor.Get(meshId);
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
                        await transport.SendAsync(currentPeer.Endpoint, currentMessage, cancellationToken).ConfigureAwait(false);
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