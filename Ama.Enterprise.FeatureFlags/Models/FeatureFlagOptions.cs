namespace Ama.Enterprise.FeatureFlags.Models;

using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Configuration options for the feature flags module.
/// </summary>
public sealed class FeatureFlagOptions
{
    /// <summary>
    /// Gets or sets the internal mesh identifier for the feature flags P2P network.
    /// </summary>
    public string InternalMeshId { get; set; } = "feature-flags-internal-mesh";

    /// <summary>
    /// Gets or sets the distributed CRDT options configuration.
    /// </summary>
    public DistributedCrdtOptions Crdt { get; set; } = new();
}