namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Providers;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Strongly-typed AOT execution adapter for <see cref="ICrdtMapReduce{TState, TInput, TAccumulate, TResult}"/>.
/// </summary>
internal sealed class CrdtMapReduceExecutor<TState, TInput, TAccumulate, TResult> : ICrdtMapReduceExecutor
    where TState : class, new()
{
    public CrdtMapReduceExecutor(string jobAlias)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobAlias);
        JobAlias = jobAlias;
    }

    /// <inheritdoc />
    public string JobAlias { get; }

    /// <inheritdoc />
    public Type StateType => typeof(TState);

    /// <inheritdoc />
    public Type InputType => typeof(TInput);

    /// <inheritdoc />
    public Type AccumulateType => typeof(TAccumulate);

    /// <inheritdoc />
    public Type ResultType => typeof(TResult);

    /// <inheritdoc />
    public async Task<CrdtMapReduceExecutionOutcomeDto> ExecuteSerializedLocalBatchAsync(
        IServiceProvider scopedProvider,
        IReadOnlyList<string> targetDocumentIds,
        byte[] serializedInput,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scopedProvider);
        ArgumentNullException.ThrowIfNull(targetDocumentIds);
        ArgumentNullException.ThrowIfNull(serializedInput);

        var serializer = scopedProvider.GetRequiredService<ICrdtSerializer>();
        var orchestrator = scopedProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var handler = scopedProvider.GetRequiredService<ICrdtMapReduce<TState, TInput, TAccumulate, TResult>>();
        var replicaContext = scopedProvider.GetRequiredService<ReplicaContext>();
        var shardIdProvider = scopedProvider.GetRequiredService<IShardIdProvider>();

        var input = serializer.DeserializeFromBytes<TInput>(serializedInput);
        if (input is null)
        {
            return new CrdtMapReduceExecutionOutcomeDto(
                IsSuccess: false,
                HasAccumulator: false,
                EvaluatedDocumentCount: 0,
                SerializedAccumulator: Array.Empty<byte>(),
                MissingDocumentIds: null,
                ErrorMessage: $"Failed to deserialize input of type '{typeof(TInput).Name}' for job '{JobAlias}'.");
        }

        string? resolvedTypeAlias = null;
        foreach (var factory in scopedProvider.GetServices<IDocumentFactory>())
        {
            if (factory.StateType == typeof(TState))
            {
                resolvedTypeAlias = factory.TypeAlias;
                break;
            }
        }

        var localAccumulators = new List<TAccumulate>(targetDocumentIds.Count);
        List<string>? missingDocumentIds = null;

        for (var i = 0; i < targetDocumentIds.Count; i++)
        {
            var docId = targetDocumentIds[i];

            // 1. Check active in-memory documents or promote if this replica is a local owner
            var liveDoc = orchestrator.TryGetLocalDocument<TState>(docId);
            if (liveDoc is null)
            {
                var promotedDoc = await orchestrator.ResolveOrPromoteLocalDocumentAsync(docId, resolvedTypeAlias, cancellationToken).ConfigureAwait(false);
                liveDoc = promotedDoc as IDistributedCrdtDocument<TState>;
            }

            if (liveDoc is not null)
            {
                var crdtDoc = await liveDoc.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
                var context = new CrdtMapContext<TState>(
                    docId,
                    liveDoc.ShardId,
                    replicaContext.ReplicaId,
                    crdtDoc,
                    liveDoc);

                var acc = await handler.MapDocumentAsync(context, input, cancellationToken).ConfigureAwait(false);
                localAccumulators.Add(acc);

                if (handler.CanShortCircuit(input, acc))
                {
                    break;
                }

                continue;
            }

            // 2. Fallback to stored snapshot if the shard was recently demoted during rebalancing
            var storedBytes = await orchestrator
                .TryLoadStoredDocumentSnapshotBytesAsync(docId, resolvedTypeAlias, cancellationToken)
                .ConfigureAwait(false);

            if (storedBytes is { Length: > 0 })
            {
                var storedDoc = serializer.DeserializeFromBytes<CrdtDocument<TState>>(storedBytes);
                if (storedDoc.Data is not null)
                {
                    var shardId = shardIdProvider.GetShardId<TState>(docId) ?? docId;
                    var demotedContext = new CrdtMapContext<TState>(
                        docId,
                        shardId,
                        replicaContext.ReplicaId,
                        storedDoc,
                        LiveDocumentHandle: null);

                    var acc = await handler.MapDocumentAsync(demotedContext, input, cancellationToken).ConfigureAwait(false);
                    localAccumulators.Add(acc);

                    if (handler.CanShortCircuit(input, acc))
                    {
                        break;
                    }

                    continue;
                }
            }

            // 3. Document is not hosted on this replica; report for failover retry
            missingDocumentIds ??= new List<string>();
            missingDocumentIds.Add(docId);
        }

        if (localAccumulators.Count == 0)
        {
            return new CrdtMapReduceExecutionOutcomeDto(
                IsSuccess: true,
                HasAccumulator: false,
                EvaluatedDocumentCount: 0,
                SerializedAccumulator: Array.Empty<byte>(),
                MissingDocumentIds: missingDocumentIds,
                ErrorMessage: null);
        }

        var combinedAccumulator = localAccumulators.Count == 1
            ? localAccumulators[0]
            : handler.Combine(input, localAccumulators);

        var serializedAccumulator = serializer.SerializeToBytes(combinedAccumulator);

        return new CrdtMapReduceExecutionOutcomeDto(
            IsSuccess: true,
            HasAccumulator: true,
            EvaluatedDocumentCount: localAccumulators.Count,
            SerializedAccumulator: serializedAccumulator,
            MissingDocumentIds: missingDocumentIds,
            ErrorMessage: null);
    }
}