namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Defines a centralized state machine channel pipeline that multiplexes and executes commands for all active CRDT documents in a scope.
/// </summary>
/// <example>
/// <code>
/// var cmd = pipeline.RentCommand();
/// cmd.Target = document;
/// cmd.Type = DocumentCommandType.Checkpoint;
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
    /// Rents a pooled document command instance from the shared object pool.
    /// </summary>
    /// <returns>A reset <see cref="PooledDocumentCommand"/> ready for configuration.</returns>
    PooledDocumentCommand RentCommand();

    /// <summary>
    /// Resets and returns a command instance back to the shared object pool.
    /// </summary>
    /// <param name="command">The command instance to return.</param>
    void ReturnCommand(PooledDocumentCommand command);

    /// <summary>
    /// Enqueues a command into the shared channel pipeline and awaits its execution completion.
    /// </summary>
    /// <param name="command">The command to enqueue and execute.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="ValueTask"/> that completes when the command has been processed.</returns>
    ValueTask EnqueueAndWaitAsync(PooledDocumentCommand command, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueues a fire-and-forget command into the shared channel pipeline without waiting for execution completion.
    /// The pipeline automatically returns the command to the pool once processed.
    /// </summary>
    /// <param name="command">The fire-and-forget command to enqueue.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests while enqueuing.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once the command is written to the channel.</returns>
    ValueTask EnqueueFireAndForgetAsync(PooledDocumentCommand command, CancellationToken cancellationToken = default);

    /// <summary>
    /// Completes the command channel writer and waits for all queued document commands to finish executing.
    /// </summary>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns>A task that completes when all queued commands have been processed.</returns>
    Task FlushAsync(CancellationToken cancellationToken = default);
}