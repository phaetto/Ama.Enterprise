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
    /// Gets or sets the interval in seconds at which the background checkpoint service periodically saves the in-memory state to persistent storage and trims the journal bounds.
    /// Defaults to 30 seconds. Must be greater than 0.
    /// </summary>
    public int CheckpointIntervalSeconds { get; set; } = 30;

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
    /// If enabled (> 0), must be at least 3x the <see cref="AntiEntropyIntervalSeconds"/> and >= <see cref="CheckpointIntervalSeconds"/> to prevent structural cluster oscillation.
    /// </summary>
    public int PeerEvictionTtlSeconds { get; set; } = 0;

    /// <summary>
    /// Gets or sets the threshold limit for the total number of operations kept in the background journal.
    /// When this limit is exceeded, an aggressive trim is forced utilizing the local version vector,
    /// seamlessly offloading lagging peer synchronization entirely to fallback Snapshot mechanisms.
    /// Defaults to 1000. Set to 0 to disable forced limits (relying strictly on safe GMVV trims).
    /// </summary>
    public int JournalTrimThreshold { get; set; } = 1000;
}