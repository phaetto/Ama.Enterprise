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

    /// <summary>
    /// Gets or sets a value indicating whether active mode is enabled.
    /// When enabled, local state changes trigger an immediate network sync broadcast,
    /// bypassing the regular anti-entropy delay.
    /// </summary>
    public bool ActiveSyncEnabled { get; set; }
}