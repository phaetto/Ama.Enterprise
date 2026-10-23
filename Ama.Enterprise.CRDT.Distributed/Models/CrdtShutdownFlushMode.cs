namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Specifies the persistence strategy executed when a distributed CRDT node shuts down.
/// </summary>
public enum CrdtShutdownFlushMode
{
    /// <summary>
    /// Drains in-flight pipelines, flushes debounced patches, and flushes the Write-Ahead Log (WAL) buffer to storage without saving full document checkpoints.
    /// Useful for environments with very short shutdown budgets such as AWS Lambda.
    /// </summary>
    JournalOnly = 0,

    /// <summary>
    /// Drains in-flight pipelines, flushes debounced patches and WAL buffers, and persists full checkpoints, shard version vectors, and cluster states to storage.
    /// </summary>
    JournalAndCheckpoints = 1,

    /// <summary>
    /// Drains in-flight pipelines, flushes debounced patches and WAL buffers, dispatches a final anti-entropy state broadcast to peers, and persists full checkpoints, shard version vectors, and cluster states.
    /// </summary>
    FullSyncAndCheckpoint = 2
}