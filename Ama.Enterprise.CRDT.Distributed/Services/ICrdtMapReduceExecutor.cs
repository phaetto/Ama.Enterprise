namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Internal AOT-safe execution adapter that bridges serialized P2P Map/Reduce requests
/// to a strongly-typed <see cref="ICrdtMapReduce{TState, TInput, TAccumulate, TResult}"/>.
/// </summary>
/// <example>
/// <code>
/// var executor = serviceProvider.GetKeyedService&lt;ICrdtMapReduceExecutor&gt;("my-job");
/// var outcome = await executor.ExecuteSerializedLocalBatchAsync(serviceProvider, docIds, inputBytes, cancellationToken);
/// </code>
/// </example>
public interface ICrdtMapReduceExecutor
{
    /// <summary>
    /// Gets the unique wire alias identifying this Map/Reduce job or query handler.
    /// </summary>
    string JobAlias { get; }

    /// <summary>
    /// Gets the document state type targeted by this handler.
    /// </summary>
    Type StateType { get; }

    /// <summary>
    /// Gets the input DTO type.
    /// </summary>
    Type InputType { get; }

    /// <summary>
    /// Gets the intermediate accumulator DTO type sent across the wire.
    /// </summary>
    Type AccumulateType { get; }

    /// <summary>
    /// Gets the final reduced result type.
    /// </summary>
    Type ResultType { get; }

    /// <summary>
    /// Deserializes the incoming input bytes, runs <c>MapDocumentAsync</c> across the specified local documents,
    /// combines the local accumulators into a single <c>TAccumulate</c> via <c>Combine</c>, and serializes the accumulator bytes.
    /// </summary>
    /// <param name="scopedProvider">The replica's scoped service provider.</param>
    /// <param name="targetDocumentIds">The document identifiers to evaluate on this replica.</param>
    /// <param name="serializedInput">The serialized input DTO payload.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The outcome of the local batch evaluation.</returns>
    Task<CrdtMapReduceExecutionOutcomeDto> ExecuteSerializedLocalBatchAsync(
        IServiceProvider scopedProvider,
        IReadOnlyList<string> targetDocumentIds,
        byte[] serializedInput,
        CancellationToken cancellationToken = default);
}