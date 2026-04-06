namespace Ama.Enterprise.P2p.Models;

/// <summary>
/// Represents the fundamental unit of communication in the gossip network.
/// Wraps the actual CRDT state or operation payloads.
/// </summary>
public readonly record struct GossipMessage : IEquatable<GossipMessage>
{
    /// <summary>
    /// Gets the unique identifier of the message for propagation tracking.
    /// </summary>
    public Guid MessageId { get; init; }

    /// <summary>
    /// Gets the identifier of the peer that originally created this message.
    /// </summary>
    public PeerId SenderId { get; init; }

    /// <summary>
    /// Gets the maximum number of times this message should be relayed before being dropped.
    /// </summary>
    public int TimeToLive { get; init; }

    /// <summary>
    /// Gets the underlying business payload (e.g., serialized CRDT updates).
    /// </summary>
    public ReadOnlyMemory<byte> Payload { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="GossipMessage"/> struct.
    /// </summary>
    /// <param name="messageId">The message identifier.</param>
    /// <param name="senderId">The sender identifier.</param>
    /// <param name="timeToLive">The TTL counter.</param>
    /// <param name="payload">The message payload.</param>
    public GossipMessage(Guid messageId, PeerId senderId, int timeToLive, ReadOnlyMemory<byte> payload)
    {
        MessageId = messageId;
        SenderId = senderId;
        TimeToLive = timeToLive;
        Payload = payload;
    }

    /// <inheritdoc />
    public bool Equals(GossipMessage other)
    {
        return MessageId.Equals(other.MessageId) &&
               SenderId.Equals(other.SenderId) &&
               TimeToLive == other.TimeToLive &&
               Payload.Span.SequenceEqual(other.Payload.Span);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(MessageId);
        hash.Add(SenderId);
        hash.Add(TimeToLive);
        
        // Add a sampled hash of the payload to avoid deep scanning on every hash request
        var span = Payload.Span;
        if (!span.IsEmpty)
        {
            hash.Add(span.Length);
            hash.Add(span[0]);
            hash.Add(span[span.Length / 2]);
            hash.Add(span[span.Length - 1]);
        }
        
        return hash.ToHashCode();
    }
}