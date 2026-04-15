namespace Ama.Enterprise.P2p.Models.Gossip;

using System;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Represents the fundamental unit of communication in the gossip network algorithm natively.
/// Wraps the generic application payloads securely for distribution.
/// </summary>
public sealed record GossipMessage : IMeshMessage, IEquatable<GossipMessage>
{
    /// <inheritdoc />
    public string MeshId { get; init; }

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
    /// Initializes a new instance of the <see cref="GossipMessage"/> class.
    /// </summary>
    /// <param name="meshId">The mesh context identifier.</param>
    /// <param name="messageId">The message identifier.</param>
    /// <param name="senderId">The sender identifier.</param>
    /// <param name="timeToLive">The TTL counter.</param>
    /// <param name="payload">The message payload.</param>
    public GossipMessage(string meshId, Guid messageId, PeerId senderId, int timeToLive, ReadOnlyMemory<byte> payload)
    {
        MeshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        MessageId = messageId;
        SenderId = senderId;
        TimeToLive = timeToLive;
        Payload = payload;
    }

    /// <inheritdoc />
    public bool Equals(GossipMessage? other)
    {
        if (other is null) return false;
        
        return string.Equals(MeshId, other.MeshId, StringComparison.Ordinal) &&
               MessageId.Equals(other.MessageId) &&
               SenderId.Equals(other.SenderId) &&
               TimeToLive == other.TimeToLive &&
               Payload.Span.SequenceEqual(other.Payload.Span);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(MeshId);
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