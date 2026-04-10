namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;

/// <summary>
/// Configuration options for the Distributed CRDT module.
/// </summary>
public sealed class DistributedCrdtOptions
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

    /// <summary>
    /// Gets or sets the interval in seconds at which the background checkpoint service periodically saves the in-memory state to persistent storage and trims the journal bounds explicitly securely.
    /// Defaults to 30 seconds.
    /// </summary>
    public int CheckpointIntervalSeconds { get; set; } = 30;

    /// <summary>
    /// Gets or sets the initial delay in seconds before the anti-entropy background service starts broadcasting.
    /// Defaults to 5 seconds.
    /// </summary>
    public int AntiEntropyInitialDelaySeconds { get; set; } = 5;

    /// <summary>
    /// Gets or sets the interval in seconds between anti-entropy synchronization rounds.
    /// Defaults to 15 seconds.
    /// </summary>
    public int AntiEntropyIntervalSeconds { get; set; } = 15;
}