namespace Ama.Enterprise.P2p.Models.Algorithms;

using System;

/// <summary>
/// Configuration options for tuning the behavior of the push-pull gossip protocol.
/// </summary>
public sealed class PushPullGossipOptions : IEquatable<PushPullGossipOptions>
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
    /// Gets or sets a value indicating whether the Push-Pull anti-entropy mechanism is enabled.
    /// </summary>
    public bool EnablePushPull { get; set; } = true;

    /// <summary>
    /// Gets or sets the interval at which the node will initiate a push-pull digest round.
    /// </summary>
    public TimeSpan PushPullInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the maximum number of message identifiers to include in a single push digest.
    /// </summary>
    public int MaxDigestSize { get; set; } = 100;

    /// <summary>
    /// Gets or sets the delay applied within the loop when push-pull operations are disabled.
    /// </summary>
    public TimeSpan PushPullDisabledDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <inheritdoc />
    public bool Equals(PushPullGossipOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        
        return GossipInterval.Equals(other.GossipInterval) && 
               Fanout == other.Fanout && 
               DefaultTimeToLive == other.DefaultTimeToLive &&
               EnablePushPull == other.EnablePushPull &&
               PushPullInterval.Equals(other.PushPullInterval) &&
               MaxDigestSize == other.MaxDigestSize &&
               PushPullDisabledDelay.Equals(other.PushPullDisabledDelay);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(
            GossipInterval, 
            Fanout, 
            DefaultTimeToLive,
            EnablePushPull,
            PushPullInterval,
            MaxDigestSize,
            PushPullDisabledDelay);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as PushPullGossipOptions);
}