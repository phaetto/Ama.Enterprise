using System.Collections.Concurrent;
using Ama.Enterprise.P2p.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// Orchestrates the Gossip protocol, managing the background sync loop,
/// message deduplication, and delegating to transport and dispatcher services.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="GossipProtocol"/> class.
/// </remarks>
public sealed class GossipProtocol(
    IOptions<GossipOptions> options,
    ITransport transport,
    ITransportListener listener,
    IPeerSelector peerSelector,
    IMessageDispatcher dispatcher,
    ILogger<GossipProtocol> logger) : IGossipProtocol, IDisposable
{
    private readonly IOptions<GossipOptions> options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly ITransport transport = transport ?? throw new ArgumentNullException(nameof(transport));
    private readonly ITransportListener listener = listener ?? throw new ArgumentNullException(nameof(listener));
    private readonly IPeerSelector peerSelector = peerSelector ?? throw new ArgumentNullException(nameof(peerSelector));
    private readonly IMessageDispatcher dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
    private readonly ILogger<GossipProtocol> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    private readonly ConcurrentDictionary<Guid, DateTimeOffset> seenMessages = new ConcurrentDictionary<Guid, DateTimeOffset>();
    private readonly ConcurrentQueue<GossipMessage> messageQueue = new ConcurrentQueue<GossipMessage>();
    private readonly PeerId localPeerId = new PeerId(Guid.NewGuid());
    private CancellationTokenSource? loopCts;
    private Task? backgroundLoopTask;

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        this.logger.LogInformation("Starting Gossip Protocol...");

        this.loopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await this.listener.StartListeningAsync(this.HandleIncomingMessageAsync, this.loopCts.Token).ConfigureAwait(false);

        this.backgroundLoopTask = Task.Run(() => this.GossipLoopAsync(this.loopCts.Token), this.loopCts.Token);

        this.logger.LogInformation("Gossip Protocol started successfully.");
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        this.logger.LogInformation("Stopping Gossip Protocol...");

        if (this.loopCts is not null)
        {
            await this.loopCts.CancelAsync().ConfigureAwait(false);
        }

        await this.listener.StopListeningAsync(cancellationToken).ConfigureAwait(false);

        if (this.backgroundLoopTask is not null)
        {
            try
            {
                await this.backgroundLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during graceful shutdown
            }
        }

        this.logger.LogInformation("Gossip Protocol stopped.");
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
            this.localPeerId,
            this.options.Value.DefaultTimeToLive,
            payload);

        this.logger.LogDebug("Broadcasting new message {MessageId} locally.", message.MessageId);

        // Mark as seen so we don't process our own broadcast if it echoes back
        this.seenMessages.TryAdd(message.MessageId, DateTimeOffset.UtcNow);

        // Queue for gossip
        this.messageQueue.Enqueue(message);

        // Dispatch locally as well so the local node processes the operation
        return this.dispatcher.DispatchAsync(message, cancellationToken);
    }

    private async Task GossipLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(this.options.Value.GossipInterval, cancellationToken).ConfigureAwait(false);

                await this.PerformGossipTickAsync(cancellationToken).ConfigureAwait(false);
                
                this.CleanupSeenMessages();
            }
            catch (OperationCanceledException)
            {
                break; // Exit gracefully
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "An error occurred during the gossip tick.");
            }
        }
    }

    private async Task PerformGossipTickAsync(CancellationToken cancellationToken)
    {
        if (this.messageQueue.IsEmpty)
        {
            return; // Nothing to gossip
        }

        var peers = (await this.peerSelector.GetPeersForGossipAsync(this.options.Value.Fanout, cancellationToken).ConfigureAwait(false)).ToList();
        
        if (peers.Count == 0)
        {
            return; // No peers available
        }

        // Dequeue current snapshot of messages to forward
        var messagesToForward = new List<GossipMessage>();
        while (this.messageQueue.TryDequeue(out var msg))
        {
            messagesToForward.Add(msg);
        }

        foreach (var peer in peers)
        {
            foreach (var message in messagesToForward)
            {
                try
                {
                    await this.transport.SendAsync(peer.Endpoint, message, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    this.logger.LogWarning(ex, "Failed to send message {MessageId} to peer {PeerEndpoint}.", message.MessageId, peer.Endpoint.Host);
                }
            }
        }
    }

    private async Task HandleIncomingMessageAsync(GossipMessage message)
    {
        if (this.seenMessages.TryAdd(message.MessageId, DateTimeOffset.UtcNow))
        {
            this.logger.LogDebug("Received new gossip message {MessageId} from {SenderId}. TTL: {Ttl}", message.MessageId, message.SenderId.Value, message.TimeToLive);

            try
            {
                // Dispatch to local business logic
                if (this.loopCts is not null)
                {
                    await this.dispatcher.DispatchAsync(message, this.loopCts.Token).ConfigureAwait(false);
                }

                // Decrement TTL and enqueue for further gossiping if still valid
                if (message.TimeToLive > 1)
                {
                    var forwardedMessage = message with { TimeToLive = message.TimeToLive - 1 };
                    this.messageQueue.Enqueue(forwardedMessage);
                }
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Error processing incoming message {MessageId}.", message.MessageId);
            }
        }
    }

    private void CleanupSeenMessages()
    {
        var threshold = DateTimeOffset.UtcNow.AddMinutes(-5); // Arbitrary expiry for seen messages
        foreach (var kvp in this.seenMessages)
        {
            if (kvp.Value < threshold)
            {
                this.seenMessages.TryRemove(kvp.Key, out _);
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (this.loopCts is not null)
        {
            this.loopCts.Cancel();
            this.loopCts.Dispose();
        }
    }
}