namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Defines the journaling strategy used when persisting CRDT operations.
/// </summary>
public enum CrdtJournalMode
{
    /// <summary>
    /// Operations are written directly to the underlying storage inline during patch application.
    /// </summary>
    Direct = 0,

    /// <summary>
    /// Operations are staged in memory via a bounded channel and flushed asynchronously in batches to storage.
    /// </summary>
    InMemoryBuffer = 1,

    /// <summary>
    /// Operations are appended sequentially to a local append-only log file before returning, and flushed in batches to storage in the background.
    /// </summary>
    DiskWal = 2
}