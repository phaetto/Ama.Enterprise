namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;

/// <summary>
/// Defines a staging buffer and write-ahead log mechanism for decoupling operation journaling from slower storage backends.
/// </summary>
/// <example>
/// <code>
/// await buffer.EnqueueAsync(operations, cancellationToken);
/// var batch = await buffer.ReadNextBatchAsync(500, TimeSpan.FromMilliseconds(50), cancellationToken);
/// if (batch.Count > 0)
/// {
///     await storage.AppendBatchAsync(batch, cancellationToken);
///     await buffer.AcknowledgeBatchAsync(batch, cancellationToken);
/// }
/// </code>
/// </example>
public interface ICrdtWriteAheadBuffer : IAsyncDisposable
{
    /// <summary>
    /// Synchronously stages a list of journaled operations into the write-ahead buffer.
    /// </summary>
    /// <param name="operations">The operations to stage.</param>
    void Enqueue(IReadOnlyList<JournaledOperation> operations);

    /// <summary>
    /// Asynchronously stages a list of journaled operations into the write-ahead buffer.
    /// </summary>
    /// <param name="operations">The operations to stage.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A value task representing the asynchronous enqueue operation.</returns>
    ValueTask EnqueueAsync(IReadOnlyList<JournaledOperation> operations, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the next batch of staged operations up to the specified maximum size or until the wait time elapses.
    /// </summary>
    /// <param name="maxBatchSize">The maximum number of operations to include in the batch.</param>
    /// <param name="maxWaitTime">The maximum duration to wait for additional operations after the first item arrives.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A read-only list of staged operations ready to be flushed.</returns>
    ValueTask<IReadOnlyList<JournaledOperation>> ReadNextBatchAsync(int maxBatchSize, TimeSpan maxWaitTime, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledges that a batch of operations has been persisted to the target storage, allowing underlying WAL resources to be truncated.
    /// </summary>
    /// <param name="flushedOperations">The operations that were flushed.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A value task representing the asynchronous acknowledgment.</returns>
    ValueTask AcknowledgeBatchAsync(IReadOnlyList<JournaledOperation> flushedOperations, CancellationToken cancellationToken = default);

    /// <summary>
    /// Recovers any uncommitted operations from persistent write-ahead segments into the target storage upon startup.
    /// </summary>
    /// <param name="targetStorage">The underlying storage to receive recovered operations.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A value task representing the asynchronous recovery process.</returns>
    ValueTask RecoverAsync(IDistributedCrdtStorage targetStorage, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks the write-ahead buffer as complete so no new operations are accepted and remaining items can be drained.
    /// </summary>
    void Complete();
}