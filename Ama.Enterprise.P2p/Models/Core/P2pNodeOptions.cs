namespace Ama.Enterprise.P2p.Models.Core;

using System;

/// <summary>
/// Configuration options for tuning the core node identity across the P2P mesh.
/// </summary>
public sealed class P2pNodeOptions : IEquatable<P2pNodeOptions>
{
    private static readonly Guid ProcessGlobalPeerId = Guid.NewGuid();

    /// <summary>
    /// Gets or sets the unique identifier of the local peer node. Defaults to a process-wide unique identifier.
    /// </summary>
    public Guid LocalPeerId { get; set; } = ProcessGlobalPeerId;

    /// <summary>
    /// Gets or sets the minimum number of active peers required before the discovery loop begins exponential backoff (Low Watermark).
    /// </summary>
    public int MinActivePeers { get; set; } = 5;

    /// <summary>
    /// Gets or sets the maximum number of active peers before the discovery loop applies aggressive backoff (High Watermark).
    /// </summary>
    public int MaxActivePeers { get; set; } = 15;

    /// <summary>
    /// Gets or sets the maximum time interval between active peer discovery broadcasts when exponential backoff is applied.
    /// </summary>
    public TimeSpan MaxDiscoveryInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets the interval at which the orchestrator runs the background peer health check loop.
    /// </summary>
    public TimeSpan HealthCheckInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the initial delay before the discovery loop starts.
    /// </summary>
    public TimeSpan InitialDiscoveryDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <inheritdoc />
    public bool Equals(P2pNodeOptions? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return LocalPeerId.Equals(other.LocalPeerId) &&
               MinActivePeers == other.MinActivePeers &&
               MaxActivePeers == other.MaxActivePeers &&
               MaxDiscoveryInterval.Equals(other.MaxDiscoveryInterval) &&
               HealthCheckInterval.Equals(other.HealthCheckInterval) &&
               InitialDiscoveryDelay.Equals(other.InitialDiscoveryDelay);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as P2pNodeOptions);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(LocalPeerId, MinActivePeers, MaxActivePeers, MaxDiscoveryInterval, HealthCheckInterval, InitialDiscoveryDelay);
    }
}