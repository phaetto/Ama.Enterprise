namespace Ama.Enterprise.P2p.Models.Algorithms;

using System;

/// <summary>
/// Configuration options for tuning the behavior of the gossip protocol.
/// </summary>
public sealed class GossipOptions : IEquatable<GossipOptions>
{
    /// <summary>
    /// Gets or sets the interval at which the node will initiate a gossip round.
    /// </summary>
    public TimeSpan GossipInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Gets or sets the number of peers to select for each gossip round (the fanout).
    /// </summary>
    public int Fanout { get; set; } = 3;

    /// <summary>
    /// Gets or sets the default Time-To-Live for newly created gossip messages.
    /// </summary>
    public int DefaultTimeToLive { get; set; } = 10;

    /// <summary>
    /// Gets or sets the duration for which seen message identifiers are retained for deduplication.
    /// </summary>
    public TimeSpan SeenMessageRetention { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the maximum number of seen message identifiers to retain in memory per mesh.
    /// </summary>
    public int MaxSeenMessages { get; set; } = 10_000;

    /// <summary>
    /// Gets or sets the maximum number of outbound messages queued between gossip ticks per mesh.
    /// </summary>
    public int MaxOutboundQueueSize { get; set; } = 2_048;

    /// <inheritdoc />
    public bool Equals(GossipOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        
        return GossipInterval.Equals(other.GossipInterval) && 
               Fanout == other.Fanout && 
               DefaultTimeToLive == other.DefaultTimeToLive &&
               SeenMessageRetention.Equals(other.SeenMessageRetention) &&
               MaxSeenMessages == other.MaxSeenMessages &&
               MaxOutboundQueueSize == other.MaxOutboundQueueSize;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(
            GossipInterval, 
            Fanout, 
            DefaultTimeToLive,
            SeenMessageRetention,
            MaxSeenMessages,
            MaxOutboundQueueSize);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as GossipOptions);
}