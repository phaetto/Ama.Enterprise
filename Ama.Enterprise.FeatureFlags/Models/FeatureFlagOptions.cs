namespace Ama.Enterprise.FeatureFlags.Models;

using System;

/// <summary>
/// Configuration options for the feature flags module.
/// </summary>
public sealed class FeatureFlagOptions
{
    /// <summary>
    /// Gets or sets the unique identifier for this replica in the CRDT cluster.
    /// </summary>
    public string ReplicaId { get; set; } = Guid.NewGuid().ToString("N");
}