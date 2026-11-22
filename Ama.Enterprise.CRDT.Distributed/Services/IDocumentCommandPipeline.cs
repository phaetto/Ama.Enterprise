namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Defines a shared scoped pipeline that processes CRDT document commands through a single channel and object pool.
/// </summary>
/// <example>
/// <code>
/// var cmd = pipeline.RentCommand();
/// cmd.Target = documentTarget;
/// cmd.Type = DocumentCommandType.Initialize;
/// try
/// {
///     await pipeline.EnqueueAndWaitAsync(cmd, cancellationToken);
/// }
/// finally
/// {
///     pipeline.ReturnCommand(cmd);
/// }
/// </code>
/// </example>
public interface IDocumentCommandPipeline
{
    /// <summary>
    /// Gets a value indicating whether the current execution context is already running inside this pipeline's processing loop.
    /// </summary>
    bool IsCurrentThreadPipelineProcessor { get; }

    /// <summary>
    /// Gets the number of commands currently queued or actively executing in the shared document pipeline.
    /// </summary>
    /// <example>
    /// <code>
    /// if (pipeline.PendingCommandCount &gt; 100)
    /// {
    ///     // Backoff background work
    /// }
    /// </code>
    /// </example>
    int PendingCommandCount { get; }

    /// <summary>
    /// Rents a pooled command instance from the shared object pool.
    /// </summary>
    /// <returns>A pooled command ready to be configured.</returns>
    PooledDocumentCommand RentCommand();

    /// <summary>
    /// Resets and returns a command instance back to the shared object pool.
    /// </summary>
    /// <param name="command">The command instance to return.</param>
    void ReturnCommand(PooledDocumentCommand command);

    /// <summary>
    /// Enqueues a command to the shared processing channel and awaits its completion.
    /// </summary>
    /// <param name="command">The configured command to execute.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>A value task that completes when the command has been processed.</returns>
    ValueTask EnqueueAndWaitAsync(PooledDocumentCommand command, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueues a fire-and-forget command to the shared processing channel without awaiting its execution.
    /// The pipeline automatically returns the command to the pool after processing.
    /// </summary>
    /// <param name="command">The configured command to enqueue.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the write operation if the channel is full.</param>
    /// <returns>A value task that completes once the command is written to the channel.</returns>
    ValueTask EnqueueFireAndForgetAsync(PooledDocumentCommand command, CancellationToken cancellationToken = default);

    /// <summary>
    /// Drains all currently queued commands in the shared document pipeline without closing the channel.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to cancel the flush wait.</param>
    /// <returns>A task that completes when all commands queued prior to this call have been processed.</returns>
    Task FlushAsync(CancellationToken cancellationToken = default);
}