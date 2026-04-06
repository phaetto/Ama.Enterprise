namespace Ama.Enterprise.P2p.Models.Core;
/// <summary>
/// Configuration options for tuning the behavior of the failure detector independently of specific protocols.
/// </summary>
public sealed class FailureDetectorOptions : IEquatable<FailureDetectorOptions>
{
    /// <summary>
    /// Gets or sets the expected base interval between heartbeats or communications.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the multiplier applied to the HeartbeatInterval to determine when a node is considered Suspect.
    /// </summary>
    public int SuspectThresholdMultiplier { get; set; } = 3;

    /// <summary>
    /// Gets or sets the multiplier applied to the HeartbeatInterval to determine when a node is considered Dead.
    /// </summary>
    public int DeadThresholdMultiplier { get; set; } = 6;

    /// <inheritdoc />
    public bool Equals(FailureDetectorOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        
        return HeartbeatInterval.Equals(other.HeartbeatInterval) && 
               SuspectThresholdMultiplier == other.SuspectThresholdMultiplier && 
               DeadThresholdMultiplier == other.DeadThresholdMultiplier;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(HeartbeatInterval, SuspectThresholdMultiplier, DeadThresholdMultiplier);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as FailureDetectorOptions);
}