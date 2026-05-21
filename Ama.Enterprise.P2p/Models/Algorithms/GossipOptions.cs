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

    /// <inheritdoc />
    public bool Equals(GossipOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        
        return GossipInterval.Equals(other.GossipInterval) && 
               Fanout == other.Fanout && 
               DefaultTimeToLive == other.DefaultTimeToLive;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(
            GossipInterval, 
            Fanout, 
            DefaultTimeToLive);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as GossipOptions);
}