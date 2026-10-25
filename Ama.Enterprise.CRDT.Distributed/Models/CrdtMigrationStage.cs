namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Defines the lifecycle stages of a live CRDT state and schema migration bridge.
/// </summary>
public enum CrdtMigrationStage
{
    /// <summary>
    /// The migration bridge has not started yet.
    /// </summary>
    NotStarted = 0,

    /// <summary>
    /// The migration bridge is waiting for the source and target CRDT orchestrators to initialize.
    /// </summary>
    Initializing = 1,

    /// <summary>
    /// The migration bridge is actively backfilling existing documents from the source while capturing live changes.
    /// </summary>
    Backfilling = 2,

    /// <summary>
    /// The initial backfill is complete, and the bridge is streaming live changes in real time with pending items in the queue.
    /// </summary>
    StreamingLive = 3,

    /// <summary>
    /// The backfill is complete and all queued live changes have been applied to the target. Safe to cut over reads and writes.
    /// </summary>
    Converged = 4,

    /// <summary>
    /// The migration bridge has been drained and stopped.
    /// </summary>
    Drained = 5
}