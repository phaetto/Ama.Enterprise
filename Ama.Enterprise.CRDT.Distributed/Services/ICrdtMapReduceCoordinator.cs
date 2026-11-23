namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Coordinates distributed Map/Reduce jobs and cluster queries across local and remote CRDT shards.
/// </summary>
/// <example>
/// <code>
/// var coordinator = serviceProvider.GetRequiredService&lt;ICrdtMapReduceCoordinator&gt;();
/// var result = await coordinator.ExecuteMapReduceAsync&lt;MyState, MyInput, MyAcc, MyResult&gt;(orchestrator, input, default, cancellationToken);
/// </code>
/// </example>
public interface ICrdtMapReduceCoordinator
{
    /// <summary>
    /// Executes a distributed Map/Reduce computation across the cluster using the specified scope and routing mode.
    /// </summary>
    /// <typeparam name="TState">The CRDT document state type.</typeparam>
    /// <typeparam name="TInput">The serializable input DTO type.</typeparam>
    /// <typeparam name="TAccumulate">The serializable intermediate accumulator DTO type.</typeparam>
    /// <typeparam name="TResult">The final reduced result type.</typeparam>
    /// <param name="orchestrator">The local document orchestrator.</param>
    /// <param name="documentTypeAlias">The resolved document type alias for <typeparamref name="TState"/>.</param>
    /// <param name="input">The job input parameters.</param>
    /// <param name="scope">The routing mode and target shard/document filters.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A <see cref="CrdtMapReduceResultDto{TResult}"/> containing the final result and execution diagnostics.</returns>
    Task<CrdtMapReduceResultDto<TResult>> ExecuteMapReduceAsync<TState, TInput, TAccumulate, TResult>(
        ICrdtDocumentOrchestrator orchestrator,
        string? documentTypeAlias,
        TInput input,
        CrdtMapReduceScopeDto scope,
        CancellationToken cancellationToken = default)
        where TState : class, new();
}