namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;

/// <summary>
/// Defines a write-ahead buffer that stages <see cref="JournaledOperation"/> instances
/// prior to batched asynchronous flushing into the backing <see cref="IDistributedCrdtStorage"/>.
/// </summary>
/// <example>
/// <code>
/// await buffer.EnqueueAsync(operations, cancellationToken);
/// var batch = await buffer.ReadNextBatchAsync(100, TimeSpan.FromMilliseconds(50), cancellationToken);
/// await storage.AppendBatchAsync(batch, cancellationToken);
/// await buffer.AcknowledgeBatchAsync(batch, cancellationToken);
/// </code>
/// </example>
public interface ICrdtWriteAheadBuffer : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Gets the approximate number of un-drained operations currently waiting in the buffer queue.
    /// </summary>
    int PendingCount { get; }

    /// <summary>
    /// Synchronously enqueues a list of journaled operations into the write-ahead buffer.
    /// </summary>
    /// <param name="operations">The operations to enqueue.</param>
    void Enqueue(IReadOnlyList<JournaledOperation> operations);

    /// <summary>
    /// Asynchronously enqueues a list of journaled operations into the write-ahead buffer.
    /// </summary>
    /// <param name="operations">The operations to enqueue.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A value task representing the asynchronous enqueue operation.</returns>
    ValueTask EnqueueAsync(IReadOnlyList<JournaledOperation> operations, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the next batch of operations up to <paramref name="maxBatchSize"/>, waiting at most <paramref name="maxWaitTime"/>
    /// once at least one operation is available.
    /// </summary>
    /// <param name="maxBatchSize">The maximum number of operations to read in the batch.</param>
    /// <param name="maxWaitTime">The maximum duration to wait for the batch to fill after the first item arrives.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A read-only list of operations to flush, or an empty list if the buffer is completed and empty.</returns>
    ValueTask<IReadOnlyList<JournaledOperation>> ReadNextBatchAsync(int maxBatchSize, TimeSpan maxWaitTime, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledges that a previously read batch of operations has been durably persisted to the backing storage,
    /// allowing any underlying WAL segment files or buffers to be truncated or deleted.
    /// </summary>
    /// <param name="flushedOperations">The operations that were successfully flushed.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A value task representing the asynchronous acknowledgement operation.</returns>
    ValueTask AcknowledgeBatchAsync(IReadOnlyList<JournaledOperation> flushedOperations, CancellationToken cancellationToken = default);

    /// <summary>
    /// Recovers any unacknowledged operations from persistent WAL storage (if applicable) into the specified target storage on startup.
    /// </summary>
    /// <param name="targetStorage">The underlying storage to receive recovered operations.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A value task representing the asynchronous recovery operation.</returns>
    ValueTask RecoverAsync(IDistributedCrdtStorage targetStorage, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks the write-ahead buffer as complete, preventing new operations from being enqueued.
    /// </summary>
    void Complete();
}