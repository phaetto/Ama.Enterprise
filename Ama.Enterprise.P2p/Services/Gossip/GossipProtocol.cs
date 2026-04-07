namespace Ama.Enterprise.P2p.Services.Gossip;

using System.Collections.Concurrent;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Orchestrates the Gossip protocol, managing the background sync loop,
/// message deduplication, and delegating to generic transport and dispatcher services.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="GossipProtocol"/> class.
/// </remarks>
public sealed class GossipProtocol(
    IOptions<GossipOptions> options,
    ITransport<GossipMessage> transport,
    ITransportListener<GossipMessage> listener,
    IPeerSelector peerSelector,
    IMessageDispatcher<GossipMessage> dispatcher,
    ILogger<GossipProtocol> logger) : IGossipProtocol, IDisposable
{
    private readonly IOptions<GossipOptions> options = options ?? throw new ArgumentNullException(nameof(options));
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
        logger.LogInformation("Starting Gossip Protocol...");

        loopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await listener.StartListeningAsync(HandleIncomingMessageAsync, loopCts.Token).ConfigureAwait(false);

        backgroundLoopTask = Task.Run(() => GossipLoopAsync(loopCts.Token), loopCts.Token);

        logger.LogInformation("Gossip Protocol started successfully.");
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Stopping Gossip Protocol...");

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
            catch (OperationCanceledException)
            {
                // Expected during graceful shutdown
            }
        }

        logger.LogInformation("Gossip Protocol stopped.");
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

        var message = new GossipMessage(
            Guid.NewGuid(),
            localPeerId,
            options.Value.DefaultTimeToLive,
            payload);

        logger.LogDebug("Broadcasting new message {MessageId} locally.", message.MessageId);

        // Mark as seen so we don't process our own broadcast if it echoes back
        seenMessages.TryAdd(message.MessageId, DateTimeOffset.UtcNow);

        // Queue for gossip
        messageQueue.Enqueue(message);

        // Dispatch locally as well so the local node processes the operation
        return dispatcher.DispatchAsync(message, cancellationToken);
    }

    private async Task GossipLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(options.Value.GossipInterval, cancellationToken).ConfigureAwait(false);

                await PerformGossipTickAsync(cancellationToken).ConfigureAwait(false);
                
                CleanupSeenMessages();
            }
            catch (OperationCanceledException)
            {
                break; // Exit gracefully
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred during the gossip tick.");
            }
        }
    }

    private async Task PerformGossipTickAsync(CancellationToken cancellationToken)
    {
        if (messageQueue.IsEmpty)
        {
            return; // Nothing to gossip
        }

        // Fetching generic peers
        var peers = (await peerSelector.GetPeersAsync(options.Value.Fanout, cancellationToken).ConfigureAwait(false)).ToList();
        
        if (peers.Count == 0)
        {
            return; // No peers available
        }

        // Dequeue current snapshot of messages to forward
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
                // Assign to local variables to prevent closure capture issues inside the Task loop
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
                        logger.LogWarning(ex, "Failed to send message {MessageId} to peer {PeerEndpoint}.", currentMessage.MessageId, currentPeer.Endpoint.Host);
                    }
                }, cancellationToken));
            }
        }

        // Await all parallel transport tasks rather than blocking the gossip queue serially
        await Task.WhenAll(sendTasks).ConfigureAwait(false);
    }

    private async Task HandleIncomingMessageAsync(GossipMessage message)
    {
        if (seenMessages.TryAdd(message.MessageId, DateTimeOffset.UtcNow))
        {
            logger.LogDebug("Received new gossip message {MessageId} from {SenderId}. TTL: {Ttl}", message.MessageId, message.SenderId.Value, message.TimeToLive);

            try
            {
                // Dispatch to local business logic
                if (loopCts is not null)
                {
                    await dispatcher.DispatchAsync(message, loopCts.Token).ConfigureAwait(false);
                }

                // Decrement TTL and enqueue for further gossiping if still valid
                if (message.TimeToLive > 1)
                {
                    var forwardedMessage = message with { TimeToLive = message.TimeToLive - 1 };
                    messageQueue.Enqueue(forwardedMessage);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing incoming message {MessageId}.", message.MessageId);
            }
        }
    }

    private void CleanupSeenMessages()
    {
        var threshold = DateTimeOffset.UtcNow.AddMinutes(-5); // Arbitrary expiry for seen messages
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