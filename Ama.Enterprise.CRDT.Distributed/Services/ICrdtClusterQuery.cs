namespace Ama.Enterprise.CRDT.Distributed.Services;

/// <summary>
/// Specialization of <see cref="ICrdtMapReduce{TState, TInput, TAccumulate, TResult}"/>
/// for distributed queries and operations where the intermediate accumulator type and the final result type are the same.
/// </summary>
/// <typeparam name="TState">The CRDT document state type.</typeparam>
/// <typeparam name="TQuery">The serializable query request DTO type.</typeparam>
/// <typeparam name="TResult">The serializable query result DTO type.</typeparam>
/// <example>
/// <code>
/// public sealed class LowBatteryQuery : ICrdtClusterQuery&lt;FleetState, int, int&gt;
/// {
///     public ValueTask&lt;int&gt; MapDocumentAsync(CrdtMapContext&lt;FleetState&gt; context, int threshold, CancellationToken ct = default)
///         =&gt; new(context.Document.Data.Devices.Values.Count(d =&gt; d.BatteryLevel &lt;= threshold));
///
///     public int Combine(int threshold, IReadOnlyList&lt;int&gt; accumulators)
///         =&gt; accumulators.Sum();
/// }
/// </code>
/// </example>
public interface ICrdtClusterQuery<TState, in TQuery, TResult>
    : ICrdtMapReduce<TState, TQuery, TResult, TResult>
    where TState : class, new()
{
    /// <inheritdoc />
    TResult ICrdtMapReduce<TState, TQuery, TResult, TResult>.Reduce(TQuery input, TResult finalAccumulator)
        => finalAccumulator;
}