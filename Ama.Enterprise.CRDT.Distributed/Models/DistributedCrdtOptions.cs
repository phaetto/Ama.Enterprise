namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Configuration options for the Distributed CRDT module.
/// </summary>
public sealed class DistributedCrdtOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether active mode is enabled.
    /// When enabled, local state changes trigger an immediate network sync broadcast,
    /// bypassing the regular anti-entropy delay.
    /// </summary>
    public bool ActiveSyncEnabled { get; set; }

    /// <summary>
    /// Gets or sets the interval in seconds at which the background checkpoint service periodically saves the in-memory state to persistent storage.
    /// Defaults to 30 seconds. Must be greater than 0.
    /// </summary>
    public int CheckpointIntervalSeconds { get; set; } = 30;

    /// <summary>
    /// Gets or sets the interval in seconds at which the background maintenance service periodically trims the journal bounds and evicts dead peers.
    /// Defaults to 60 seconds. Must be greater than 0.
    /// </summary>
    public int MaintenanceIntervalSeconds { get; set; } = 60;

    /// <summary>
    /// Gets or sets the initial delay in seconds before the anti-entropy background service starts broadcasting.
    /// Defaults to 5 seconds. Must be non-negative.
    /// </summary>
    public int AntiEntropyInitialDelaySeconds { get; set; } = 5;

    /// <summary>
    /// Gets or sets the interval in seconds between anti-entropy synchronization rounds.
    /// Defaults to 15 seconds. Must be greater than 0.
    /// </summary>
    public int AntiEntropyIntervalSeconds { get; set; } = 15;

    /// <summary>
    /// Gets or sets the Time-To-Live (TTL) in seconds before a peer is considered dead and its state is evicted from the local cluster map and document metadata.
    /// Defaults to 0 (is disabled). Set to an int (something long like TimeSpan.FromDays(7).TotalSeconds) to enable peer eviction in volatile networks.
    /// If enabled (> 0), must be at least 3x the <see cref="AntiEntropyIntervalSeconds"/> and >= <see cref="MaintenanceIntervalSeconds"/> to prevent structural cluster oscillation.
    /// </summary>
    public int PeerEvictionTtlSeconds { get; set; } = 0;

    /// <summary>
    /// Gets or sets the cooldown duration in seconds before a tombstoned replica is entirely purged from the cluster state tracking maps.
    /// This allows its structural bounds to be safely forgotten avoiding endless scaling journal bloat.
    /// Defaults to 0 (disabled, meaning tombstones remain indefinitely until manually removed or the process restarts without persistence).
    /// </summary>
    public int PeerTombstoneCooldownSeconds { get; set; } = 0;

    /// <summary>
    /// Gets or sets the soft threshold limit for the total number of operations kept in the background journal.
    /// When this limit is exceeded, an aggressive trim is forced utilizing the local version vector in the background,
    /// seamlessly offloading lagging peer synchronization entirely to fallback Snapshot mechanisms.
    /// Defaults to 5000. Set to 0 to disable forced limits (relying strictly on safe GMVV trims).
    /// </summary>
    public int JournalSoftTrimThreshold { get; set; } = 5000;

    /// <summary>
    /// Gets or sets the hard threshold limit for the total number of operations kept in the background journal.
    /// When this limit is exceeded, incoming operations will be actively throttled, yielding the thread 
    /// to apply natural backpressure while the background trim processes, preventing memory death spirals.
    /// Defaults to 10000. Set to 0 to disable hard limits.
    /// </summary>
    public int JournalHardTrimThreshold { get; set; } = 10000;

    /// <summary>
    /// Gets or sets the Time-To-Live (TTL) in seconds for CRDT metadata compaction.
    /// When operations exceed this age, their associated metadata (like tombstones) is eligible for garbage collection.
    /// Defaults to 0 (compaction disabled).
    /// </summary>
    public int CompactionTtlSeconds { get; set; } = 0;

    /// <summary>
    /// Gets or sets a value indicating whether to avoid blind checkpoint writes to storage.
    /// When enabled, the checkpoint service will maintain an in-memory last-known state cache 
    /// and skip storage saves if the payload has not mutated.
    /// Defaults to false.
    /// </summary>
    public bool AvoidBlindCheckpointWrites { get; set; } = false;

    /// <summary>
    /// Gets or sets the maximum capacity of the lock-free document and orchestrator channels.
    /// When set to 0 or negative, the channel is unbounded.
    /// When set to a positive value, the channel is bounded to prevent unbounded memory growth under heavy load.
    /// Defaults to 0 (unbounded).
    /// </summary>
    public int ChannelCapacity { get; set; } = 0;
}