namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Defines a node-wide striped command processor that routes CRDT document orchestrator commands across a fixed pool of worker stripes.
/// </summary>
/// <example>
/// <code>
/// var cmd = stripePool.RentCommand();
/// cmd.Target = orchestrator;
/// cmd.Type = OrchestratorCommandType.Initialize;
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
public interface IOrchestratorCommandStripePool
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
    /// Gets a value indicating whether the current execution context is running inside one of the orchestrator stripe pool's worker loops.
    /// </summary>
    bool IsCurrentThreadStripeProcessor { get; }

    /// <summary>
    /// Rents a pooled orchestrator command object from the shared pool.
    /// </summary>
    /// <returns>A clean <see cref="PooledOrchestratorCommand"/> instance.</returns>
    internal PooledOrchestratorCommand RentCommand();

    /// <summary>
    /// Resets and returns an orchestrator command object to the shared pool.
    /// </summary>
    /// <param name="command">The command instance to return.</param>
    internal void ReturnCommand(PooledOrchestratorCommand command);

    /// <summary>
    /// Determines whether the current execution context is actively executing on the worker stripe assigned to the specified target.
    /// </summary>
    /// <param name="target">The orchestrator command target.</param>
    /// <returns><c>true</c> if the current thread is already running inside the target's assigned stripe; otherwise, <c>false</c>.</returns>
    internal bool IsCurrentThreadProcessingTarget(IOrchestratorCommandTarget target);

    /// <summary>
    /// Enqueues an orchestrator command to its target's assigned stripe and awaits its execution completion.
    /// </summary>
    /// <param name="command">The command to execute.</param>
    /// <param name="cancellationToken">A token to cancel waiting or execution.</param>
    /// <returns>A value task representing the asynchronous operation.</returns>
    internal ValueTask EnqueueAndWaitAsync(PooledOrchestratorCommand command, CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to synchronously write a fire-and-forget orchestrator command to its target's assigned stripe channel without waiting.
    /// </summary>
    /// <param name="command">The command to enqueue.</param>
    /// <returns><c>true</c> if the command was immediately written to the channel; otherwise, <c>false</c>.</returns>
    internal bool TryEnqueueFireAndForget(PooledOrchestratorCommand command);

    /// <summary>
    /// Enqueues a fire-and-forget orchestrator command to its target's assigned stripe. The pool automatically returns the command upon completion.
    /// </summary>
    /// <param name="command">The command to enqueue.</param>
    /// <param name="cancellationToken">A token to cancel writing to a bounded channel.</param>
    /// <returns>A value task representing the enqueue operation.</returns>
    internal ValueTask EnqueueFireAndForgetAsync(PooledOrchestratorCommand command, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueues a non-destructive flush barrier onto the worker stripe assigned to the specified scope and waits until all previously enqueued commands on that stripe have been processed.
    /// </summary>
    /// <param name="scopeReplicaId">The composite replica identifier of the scope requesting the flush.</param>
    /// <param name="cancellationToken">A token to cancel waiting for the flush.</param>
    /// <returns>A task that completes when all commands queued prior to this call for the scope have finished executing.</returns>
    Task FlushScopeAsync(string scopeReplicaId, CancellationToken cancellationToken = default);
}