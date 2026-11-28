namespace Ama.Enterprise.CRDT.Distributed.Services.Commands;

using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Internal contract implemented by <see cref="CrdtDocumentOrchestrator"/> to allow the shared
/// <see cref="IOrchestratorCommandStripePool"/> to route and execute orchestrator lifecycle commands
/// sequentially per scope.
/// </summary>
/// <example>
/// <code>
/// await target.WaitForInitializationGateAsync();
/// await target.ExecuteOrchestratorCommandInternalAsync(command);
/// </code>
/// </example>
internal interface IOrchestratorCommandTarget
{
    /// <summary>
    /// Gets the composite replica and scope identifier used to deterministically route all commands for a scope to a single worker stripe.
    /// </summary>
    string ScopeReplicaId { get; }

    /// <summary>
    /// Gets a value indicating whether the orchestrator has completed initialization and opened its command processing gate.
    /// </summary>
    bool IsInitialized { get; }

    /// <summary>
    /// Waits asynchronously until the orchestrator has finished initializing before processing any enqueued stripe commands.
    /// </summary>
    /// <returns>A value task that completes once initialization has finished.</returns>
    ValueTask WaitForInitializationGateAsync();

    /// <summary>
    /// Executes the specified orchestrator command inline on the assigned stripe worker thread.
    /// </summary>
    /// <param name="command">The pooled orchestrator command to execute.</param>
    /// <returns>A task representing the asynchronous execution.</returns>
    Task ExecuteOrchestratorCommandInternalAsync(PooledOrchestratorCommand command);
}