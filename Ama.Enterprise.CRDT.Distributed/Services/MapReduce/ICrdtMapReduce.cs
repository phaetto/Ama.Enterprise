namespace Ama.Enterprise.CRDT.Distributed.Services.MapReduce;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Defines a distributed Map/Reduce computation that executes compiled C# logic against in-memory
/// CRDT documents across local and remote shards, combines intermediate accumulators per node,
/// and reduces the final result on the coordinating node.
/// </summary>
/// <typeparam name="TState">The CRDT document state type.</typeparam>
/// <typeparam name="TInput">The serializable input parameters DTO type.</typeparam>
/// <typeparam name="TAccumulate">The serializable intermediate accumulator DTO type produced by Map and Combine.</typeparam>
/// <typeparam name="TResult">The final result type produced by Reduce on the coordinator.</typeparam>
/// <example>
/// <code>
/// public sealed class DeviceCountMapReduce : ICrdtMapReduce&lt;FleetState, CountInput, int, int&gt;
/// {
///     public ValueTask&lt;int&gt; MapDocumentAsync(CrdtMapContext&lt;FleetState&gt; context, CountInput input, CancellationToken cancellationToken = default)
///         =&gt; new(context.Document.Data?.Devices?.Count ?? 0);
///
///     public int Combine(CountInput input, IReadOnlyList&lt;int&gt; accumulators)
///         =&gt; accumulators.Sum();
///
///     public int Reduce(CountInput input, int finalAccumulator)
///         =&gt; finalAccumulator;
/// }
/// </code>
/// </example>
public interface ICrdtMapReduce<TState, in TInput, TAccumulate, out TResult>
    where TState : class, new()
{
    /// <summary>
    /// Evaluates (and optionally mutates in-place) a single in-memory CRDT document on the node hosting its shard,
    /// producing an intermediate accumulator value.
    /// </summary>
    /// <param name="context">The document execution context including the materialized state, metadata, and live handle.</param>
    /// <param name="input">The job input parameters.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The intermediate accumulator computed for this document.</returns>
    ValueTask<TAccumulate> MapDocumentAsync(
        CrdtMapContext<TState> context,
        TInput input,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Combines multiple intermediate accumulators into a single accumulator.
    /// This runs both on remote peers (to collapse results from multiple local documents into a single wire response)
    /// and on the coordinator node (to merge local and remote peer accumulators).
    /// </summary>
    /// <param name="input">The original input parameters.</param>
    /// <param name="accumulators">The collection of intermediate accumulators to combine.</param>
    /// <returns>A single combined accumulator.</returns>
    TAccumulate Combine(TInput input, IReadOnlyList<TAccumulate> accumulators);

    /// <summary>
    /// Transforms the final combined accumulator into the output result on the coordinator node.
    /// </summary>
    /// <param name="input">The original input parameters.</param>
    /// <param name="finalAccumulator">The final combined accumulator across all evaluated shards.</param>
    /// <returns>The final result.</returns>
    TResult Reduce(TInput input, TAccumulate finalAccumulator);

    /// <summary>
    /// Determines whether the computation has found a definitive answer and can short-circuit
    /// remaining unevaluated documents or in-flight remote peer requests.
    /// </summary>
    /// <param name="input">The original input parameters.</param>
    /// <param name="currentAccumulator">The accumulator produced so far.</param>
    /// <returns><c>true</c> if execution can stop early; otherwise, <c>false</c>.</returns>
    bool CanShortCircuit(TInput input, TAccumulate currentAccumulator) => false;
}