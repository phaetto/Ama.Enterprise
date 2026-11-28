namespace Ama.Enterprise.CRDT.Distributed.Services.Commands;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Defines a node-wide striped command processor that routes CRDT document commands across a fixed pool of worker stripes.
/// </summary>
/// <example>
/// <code>
/// var cmd = stripePool.RentCommand();
/// cmd.Target = document;
/// cmd.Type = DocumentCommandType.Initialize;
/// try
/// {
///     await stripePool.EnqueueAndWaitAsync(cmd, cancellationToken);
/// }
/// finally
/// {
///     stripePool.ReturnCommand(cmd);
/// }
/// </code>
/// </example>
public interface IDocumentCommandStripePool
{
    /// <summary>
    /// Gets the number of worker stripes in the pool.
    /// </summary>
    int StripeCount { get; }

    /// <summary>
    /// Gets the total number of commands currently queued or actively executing across all worker stripes.
    /// </summary>
    int PendingCommandCount { get; }

    /// <summary>
    /// Gets a value indicating whether the current execution context is running inside one of the stripe pool's worker loops.
    /// </summary>
    bool IsCurrentThreadStripeProcessor { get; }

    /// <summary>
    /// Rents a pooled command object from the shared pool.
    /// </summary>
    /// <returns>A clean <see cref="PooledDocumentCommand"/> instance.</returns>
    PooledDocumentCommand RentCommand();

    /// <summary>
    /// Resets and returns a command object to the shared pool.
    /// </summary>
    /// <param name="command">The command instance to return.</param>
    void ReturnCommand(PooledDocumentCommand command);

    /// <summary>
    /// Enqueues a command to its target's assigned stripe and awaits its execution completion.
    /// </summary>
    /// <param name="command">The command to execute.</param>
    /// <param name="cancellationToken">A token to cancel waiting or execution.</param>
    /// <returns>A value task representing the asynchronous operation.</returns>
    ValueTask EnqueueAndWaitAsync(PooledDocumentCommand command, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueues a fire-and-forget command to its target's assigned stripe. The pool automatically returns the command upon completion.
    /// </summary>
    /// <param name="command">The command to enqueue.</param>
    /// <param name="cancellationToken">A token to cancel writing to a bounded channel.</param>
    /// <returns>A value task representing the enqueue operation.</returns>
    ValueTask EnqueueFireAndForgetAsync(PooledDocumentCommand command, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueues a non-destructive flush barrier across all worker stripes and waits until all previously enqueued commands have been processed.
    /// </summary>
    /// <param name="scopeReplicaId">The composite replica identifier of the scope requesting the flush.</param>
    /// <param name="cancellationToken">A token to cancel waiting for the flush.</param>
    /// <returns>A task that completes when all commands queued prior to this call have finished executing.</returns>
    Task FlushScopeAsync(string scopeReplicaId, CancellationToken cancellationToken = default);
}