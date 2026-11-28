namespace Ama.Enterprise.CRDT.Distributed.Services.MapReduce;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Pooling;
using Ama.CRDT.Services.Providers;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services.Documents;
using Ama.Enterprise.CRDT.Distributed.Services.Lifecycle;
using Ama.Enterprise.CRDT.Distributed.Services.Orchestration;
using Ama.Enterprise.CRDT.Distributed.Services.Sharding;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

/// <summary>
/// Default implementation of <see cref="ICrdtMapReduceCoordinator"/> executing local and batched remote Map/Reduce phases with automatic peer failover.
/// </summary>
public sealed class CrdtMapReduceCoordinator : ICrdtMapReduceCoordinator
{
    private readonly IServiceProvider serviceProvider;
    private readonly IShardDistributionManager distributionManager;
    private readonly IClusterStateTracker clusterStateTracker;
    private readonly ICrdtRemoteProxyClient remoteProxyClient;
    private readonly IDistributedCrdtTelemetry telemetry;
    private readonly ICrdtStringPool stringPool;
    private readonly DistributedCrdtOptions options;

    public CrdtMapReduceCoordinator(
        IServiceProvider serviceProvider,
        IShardDistributionManager distributionManager,
        IClusterStateTracker clusterStateTracker,
        ICrdtRemoteProxyClient remoteProxyClient,
        IDistributedCrdtTelemetry telemetry,
        IOptions<DistributedCrdtOptions> options,
        ICrdtStringPool? stringPool = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.distributionManager = distributionManager ?? throw new ArgumentNullException(nameof(distributionManager));
        this.clusterStateTracker = clusterStateTracker ?? throw new ArgumentNullException(nameof(clusterStateTracker));
        this.remoteProxyClient = remoteProxyClient ?? throw new ArgumentNullException(nameof(remoteProxyClient));
        this.telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        this.stringPool = stringPool ?? CrdtStringPool.Shared;
        this.options = options.Value;
    }

    /// <inheritdoc />
    public async Task<CrdtMapReduceResultDto<TResult>> ExecuteMapReduceAsync<TState, TInput, TAccumulate, TResult>(
        ICrdtDocumentOrchestrator orchestrator,
        string? documentTypeAlias,
        TInput input,
        CrdtMapReduceScopeDto scope,
        CancellationToken cancellationToken = default)
        where TState : class, new()
    {
        ArgumentNullException.ThrowIfNull(orchestrator);
        ArgumentNullException.ThrowIfNull(input);

        var handler = serviceProvider.GetService<ICrdtMapReduce<TState, TInput, TAccumulate, TResult>>()
            ?? throw new InvalidOperationException(
                $"No Map/Reduce or Cluster Query handler is registered for ({typeof(TState).Name}, {typeof(TInput).Name}, {typeof(TAccumulate).Name}, {typeof(TResult).Name}). " +
                "Register it using AddDistributedCrdtMapReduce or AddDistributedCrdtQuery.");

        var jobAlias = ResolveMapReduceJobAlias<TState, TInput, TAccumulate, TResult>();
        var resolvedTypeAlias = string.IsNullOrWhiteSpace(documentTypeAlias) ? typeof(TState).Name : documentTypeAlias;
        var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
        var scopeContext = serviceProvider.GetService<CrdtScopeContext>();
        var wireScopeId = scopeContext != null &&
                          !string.IsNullOrWhiteSpace(scopeContext.ScopeId) &&
                          !string.Equals(scopeContext.ScopeId, scopeContext.HostReplicaId, StringComparison.Ordinal)
            ? scopeContext.ScopeId
            : null;
        var shardIdProvider = serviceProvider.GetRequiredService<IShardIdProvider>();

        var candidateDocuments = await ResolveCandidateMapReduceDocumentsAsync<TState>(
            orchestrator,
            resolvedTypeAlias,
            shardIdProvider,
            scope,
            cancellationToken).ConfigureAwait(false);

        if (candidateDocuments.Count == 0)
        {
            var emptyCombined = handler.Combine(input, Array.Empty<TAccumulate>());
            var emptyResult = handler.Reduce(input, emptyCombined);
            telemetry.RecordOrchestratorMapReduceExecuted(jobAlias, 0);
            return new CrdtMapReduceResultDto<TResult>(
                emptyResult,
                TotalDocumentsTargeted: 0,
                LocalDocumentsEvaluated: 0,
                RemoteDocumentsEvaluated: 0,
                RemotePeersContacted: 0,
                WasShortCircuited: false,
                UnreachableDocumentIds: Array.Empty<string>());
        }

        var concreteOrchestrator = orchestrator as CrdtDocumentOrchestrator;
        var localDocumentBatch = new List<CandidateLocalDocument<TState>>(candidateDocuments.Count);
        var remoteBatchesByReplicaId = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        Dictionary<string, IReadOnlyList<string>>? docCandidateReplicas = null;
        Dictionary<string, IReadOnlyList<string>>? shardTargetNodesCache = null;
        Dictionary<string, List<string>>? shardFallbackRemotesCache = null;
        Dictionary<string, string?>? replicaPeerIdCache = null;
        var unreachableDocumentIds = new List<string>();

        string? GetCachedPeerId(string replicaId)
        {
            replicaPeerIdCache ??= new Dictionary<string, string?>(StringComparer.Ordinal);
            if (!replicaPeerIdCache.TryGetValue(replicaId, out var cachedPeerId))
            {
                cachedPeerId = clusterStateTracker.GetPeerIdByReplicaId(replicaId);
                replicaPeerIdCache[replicaId] = cachedPeerId;
            }

            return cachedPeerId;
        }

        async ValueTask<List<string>> GetCachedFallbackRemotesAsync(string shardId)
        {
            shardFallbackRemotesCache ??= new Dictionary<string, List<string>>(StringComparer.Ordinal);
            if (!shardFallbackRemotesCache.TryGetValue(shardId, out var cachedFallbacks))
            {
                cachedFallbacks = await remoteProxyClient.GetCandidateRemoteReplicasForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
                shardFallbackRemotesCache[shardId] = cachedFallbacks;
            }

            return cachedFallbacks;
        }

        async ValueTask<IReadOnlyList<string>> GetCachedShardReplicasAsync(string shardId)
        {
            shardTargetNodesCache ??= new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
            if (!shardTargetNodesCache.TryGetValue(shardId, out var cachedShardReplicas))
            {
                var targetNodes = await distributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
                cachedShardReplicas = targetNodes ?? Array.Empty<string>();
                shardTargetNodesCache[shardId] = cachedShardReplicas;
            }

            return cachedShardReplicas;
        }

        foreach (var item in candidateDocuments)
        {
            var docId = item.DocumentId;
            var shardId = item.ShardId;
            var liveLocalDoc = item.LiveDocument ?? (concreteOrchestrator is not null
                ? concreteOrchestrator.TryGetLocalDocumentFast<TState>(docId, resolvedTypeAlias)
                : orchestrator.TryGetLocalDocument<TState>(docId));

            // Fast path for MinimalShardCover: if the document is already promoted in local memory,
            // skip consistent hash ring SHA256 calculation and shard target node dictionary allocations entirely.
            if (scope.RoutingMode == CrdtMapReduceRoutingMode.MinimalShardCover && liveLocalDoc is not null)
            {
                localDocumentBatch.Add(new CandidateLocalDocument<TState>(docId, shardId, liveLocalDoc));
                continue;
            }

            var orderedReplicas = await GetCachedShardReplicasAsync(shardId).ConfigureAwait(false);

            switch (scope.RoutingMode)
            {
                case CrdtMapReduceRoutingMode.MinimalShardCover:
                {
                    if (ContainsReplica(orderedReplicas, replicaContext.ReplicaId))
                    {
                        docCandidateReplicas ??= new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
                        docCandidateReplicas[docId] = orderedReplicas;
                        localDocumentBatch.Add(new CandidateLocalDocument<TState>(docId, shardId, liveLocalDoc));
                        break;
                    }

                    docCandidateReplicas ??= new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);

                    string? chosenRemote = null;
                    for (var i = 0; i < orderedReplicas.Count; i++)
                    {
                        var candidate = orderedReplicas[i];
                        if (!string.Equals(candidate, replicaContext.ReplicaId, StringComparison.Ordinal) &&
                            remoteBatchesByReplicaId.ContainsKey(candidate) &&
                            !string.IsNullOrWhiteSpace(GetCachedPeerId(candidate)))
                        {
                            chosenRemote = candidate;
                            break;
                        }
                    }

                    if (chosenRemote is null)
                    {
                        for (var i = 0; i < orderedReplicas.Count; i++)
                        {
                            var candidate = orderedReplicas[i];
                            if (!string.Equals(candidate, replicaContext.ReplicaId, StringComparison.Ordinal) &&
                                !string.IsNullOrWhiteSpace(GetCachedPeerId(candidate)))
                            {
                                chosenRemote = candidate;
                                break;
                            }
                        }
                    }

                    if (chosenRemote is null)
                    {
                        var fallbackRemotes = await GetCachedFallbackRemotesAsync(shardId).ConfigureAwait(false);
                        if (fallbackRemotes.Count > 0)
                        {
                            var expandedReplicas = new List<string>(orderedReplicas.Count + fallbackRemotes.Count);
                            for (var i = 0; i < orderedReplicas.Count; i++)
                            {
                                expandedReplicas.Add(orderedReplicas[i]);
                            }

                            for (var i = 0; i < fallbackRemotes.Count; i++)
                            {
                                var candidate = fallbackRemotes[i];
                                if (!expandedReplicas.Contains(candidate))
                                {
                                    expandedReplicas.Add(candidate);
                                }

                                if (chosenRemote is null && !string.IsNullOrWhiteSpace(GetCachedPeerId(candidate)))
                                {
                                    chosenRemote = candidate;
                                }
                            }

                            orderedReplicas = expandedReplicas;
                            shardTargetNodesCache![shardId] = orderedReplicas;
                        }
                    }

                    docCandidateReplicas[docId] = orderedReplicas;

                    if (chosenRemote is not null)
                    {
                        if (!remoteBatchesByReplicaId.TryGetValue(chosenRemote, out var list))
                        {
                            list = new List<string>();
                            remoteBatchesByReplicaId[chosenRemote] = list;
                        }

                        list.Add(docId);
                    }
                    else
                    {
                        unreachableDocumentIds.Add(docId);
                    }

                    break;
                }

                case CrdtMapReduceRoutingMode.PrimaryReplicaOnly:
                {
                    string? chosenPrimary = null;
                    for (var i = 0; i < orderedReplicas.Count; i++)
                    {
                        var candidate = orderedReplicas[i];
                        if (string.Equals(candidate, replicaContext.ReplicaId, StringComparison.Ordinal))
                        {
                            chosenPrimary = candidate;
                            break;
                        }

                        if (!string.IsNullOrWhiteSpace(GetCachedPeerId(candidate)))
                        {
                            chosenPrimary = candidate;
                            break;
                        }
                    }

                    if (chosenPrimary is null)
                    {
                        var fallbackRemotes = await GetCachedFallbackRemotesAsync(shardId).ConfigureAwait(false);
                        if (fallbackRemotes.Count > 0)
                        {
                            var expandedReplicas = new List<string>(orderedReplicas.Count + fallbackRemotes.Count);
                            for (var i = 0; i < orderedReplicas.Count; i++)
                            {
                                expandedReplicas.Add(orderedReplicas[i]);
                            }

                            for (var i = 0; i < fallbackRemotes.Count; i++)
                            {
                                var candidate = fallbackRemotes[i];
                                if (!expandedReplicas.Contains(candidate))
                                {
                                    expandedReplicas.Add(candidate);
                                }

                                if (chosenPrimary is null && !string.IsNullOrWhiteSpace(GetCachedPeerId(candidate)))
                                {
                                    chosenPrimary = candidate;
                                }
                            }

                            orderedReplicas = expandedReplicas;
                            shardTargetNodesCache![shardId] = orderedReplicas;
                        }
                    }

                    if (chosenPrimary is null)
                    {
                        unreachableDocumentIds.Add(docId);
                    }
                    else if (string.Equals(chosenPrimary, replicaContext.ReplicaId, StringComparison.Ordinal))
                    {
                        if (liveLocalDoc is null)
                        {
                            docCandidateReplicas ??= new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
                            docCandidateReplicas[docId] = orderedReplicas;
                        }

                        localDocumentBatch.Add(new CandidateLocalDocument<TState>(docId, shardId, liveLocalDoc));
                    }
                    else
                    {
                        docCandidateReplicas ??= new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
                        docCandidateReplicas[docId] = orderedReplicas;

                        if (!remoteBatchesByReplicaId.TryGetValue(chosenPrimary, out var list))
                        {
                            list = new List<string>();
                            remoteBatchesByReplicaId[chosenPrimary] = list;
                        }

                        list.Add(docId);
                    }

                    break;
                }

                case CrdtMapReduceRoutingMode.AllReplicas:
                {
                    var assignedAny = false;
                    for (var i = 0; i < orderedReplicas.Count; i++)
                    {
                        var replica = orderedReplicas[i];
                        if (string.Equals(replica, replicaContext.ReplicaId, StringComparison.Ordinal))
                        {
                            localDocumentBatch.Add(new CandidateLocalDocument<TState>(docId, shardId, liveLocalDoc));
                            assignedAny = true;
                        }
                        else if (!string.IsNullOrWhiteSpace(GetCachedPeerId(replica)))
                        {
                            if (!remoteBatchesByReplicaId.TryGetValue(replica, out var list))
                            {
                                list = new List<string>();
                                remoteBatchesByReplicaId[replica] = list;
                            }

                            list.Add(docId);
                            assignedAny = true;
                        }
                    }

                    if (!assignedAny)
                    {
                        unreachableDocumentIds.Add(docId);
                    }

                    break;
                }
            }
        }

        if (unreachableDocumentIds.Count > 0 && !scope.AllowPartialResultsOnPeerFailure)
        {
            throw new InvalidOperationException(
                $"Cannot execute Map/Reduce job '{jobAlias}': no active replicas found for document(s) [{string.Join(", ", unreachableDocumentIds)}].");
        }

        var allNodeAccumulators = new List<TAccumulate>();
        var localDocsEvaluated = 0;
        var remoteDocsEvaluated = 0;
        var remotePeersContacted = 0;
        var wasShortCircuited = false;

        if (localDocumentBatch.Count > 0)
        {
            var localAccumulators = new List<TAccumulate>(localDocumentBatch.Count);
            ICrdtSerializer? serializer = null;
            HashSet<string>? localExcludedReplicaSet = null;

            for (var i = 0; i < localDocumentBatch.Count; i++)
            {
                var localItem = localDocumentBatch[i];
                var docId = localItem.DocumentId;
                var liveDoc = localItem.LiveDocument;
                if (liveDoc is null)
                {
                    var promoted = await orchestrator.ResolveOrPromoteLocalDocumentAsync(docId, resolvedTypeAlias, cancellationToken).ConfigureAwait(false);
                    liveDoc = promoted as IDistributedCrdtDocument<TState>;
                }

                if (liveDoc is not null)
                {
                    var docData = liveDoc.ReadData();
                    var context = new CrdtMapContext<TState>(
                        docId,
                        liveDoc.ShardId,
                        replicaContext.ReplicaId,
                        docData,
                        liveDoc);

                    var mapValueTask = handler.MapDocumentAsync(context, input, cancellationToken);
                    var acc = mapValueTask.IsCompletedSuccessfully
                        ? mapValueTask.Result
                        : await mapValueTask.ConfigureAwait(false);
                    localAccumulators.Add(acc);
                    localDocsEvaluated++;

                    if (handler.CanShortCircuit(input, acc))
                    {
                        wasShortCircuited = true;
                        break;
                    }

                    continue;
                }

                var storedBytes = await orchestrator.TryLoadStoredDocumentSnapshotBytesAsync(docId, resolvedTypeAlias, cancellationToken).ConfigureAwait(false);
                if (storedBytes is { Length: > 0 })
                {
                    serializer ??= serviceProvider.GetRequiredService<ICrdtSerializer>();
                    var storedDoc = serializer.DeserializeFromBytes<CrdtDocument<TState>>(storedBytes);
                    if (storedDoc.Data is not null)
                    {
                        var shardId = localItem.ShardId;
                        var demotedContext = new CrdtMapContext<TState>(
                            docId,
                            shardId,
                            replicaContext.ReplicaId,
                            storedDoc,
                            liveDocumentHandle: null);

                        var mapValueTask = handler.MapDocumentAsync(demotedContext, input, cancellationToken);
                        var acc = mapValueTask.IsCompletedSuccessfully
                            ? mapValueTask.Result
                            : await mapValueTask.ConfigureAwait(false);
                        localAccumulators.Add(acc);
                        localDocsEvaluated++;

                        if (handler.CanShortCircuit(input, acc))
                        {
                            wasShortCircuited = true;
                            break;
                        }

                        continue;
                    }
                }

                if (scope.RoutingMode != CrdtMapReduceRoutingMode.AllReplicas)
                {
                    localExcludedReplicaSet ??= new HashSet<string>(StringComparer.Ordinal) { replicaContext.ReplicaId };
                    if (TryAssignFallbackReplica(docId, replicaContext.ReplicaId, docCandidateReplicas, localExcludedReplicaSet, clusterStateTracker, out var fallbackReplica))
                    {
                        if (!remoteBatchesByReplicaId.TryGetValue(fallbackReplica!, out var list))
                        {
                            list = new List<string>();
                            remoteBatchesByReplicaId[fallbackReplica!] = list;
                        }

                        list.Add(docId);
                        continue;
                    }
                }

                unreachableDocumentIds.Add(docId);
            }

            if (localAccumulators.Count > 0)
            {
                var localCombined = localAccumulators.Count == 1
                    ? localAccumulators[0]
                    : handler.Combine(input, localAccumulators);
                allNodeAccumulators.Add(localCombined);
            }
        }

        if (!wasShortCircuited && remoteBatchesByReplicaId.Count > 0)
        {
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();
            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            var rpcCorrelator = serviceProvider.GetRequiredService<IP2pRpcCorrelator>();

            var serializedInput = serializer.SerializeToBytes(input);
            var maxRetries = Math.Max(1, options.RpcMaxRetries);
            var rpcTimeout = TimeSpan.FromMilliseconds(Math.Max(1, options.RpcTimeoutMilliseconds));
            var retryDelayMs = Math.Max(0, options.RpcRetryDelayMilliseconds);

            var triedReplicasPerDoc = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
            var currentBatches = new Dictionary<string, List<string>>(remoteBatchesByReplicaId, StringComparer.Ordinal);
            var contactedPeersSet = new HashSet<string>(StringComparer.Ordinal);
            Exception? lastRemoteException = null;

            for (var attempt = 0; attempt < maxRetries && currentBatches.Count > 0 && !wasShortCircuited; attempt++)
            {
                if (attempt > 0 && retryDelayMs > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(retryDelayMs * attempt), cancellationToken).ConfigureAwait(false);
                }

                using var remoteCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                var batchEntries = new List<KeyValuePair<string, List<string>>>(currentBatches);
                var batchTasks = new Task<RemoteBatchAttemptOutcome>[batchEntries.Count];

                for (var i = 0; i < batchEntries.Count; i++)
                {
                    var targetReplicaId = batchEntries[i].Key;
                    var docBatch = batchEntries[i].Value;
                    contactedPeersSet.Add(targetReplicaId);

                    for (var d = 0; d < docBatch.Count; d++)
                    {
                        var docId = docBatch[d];
                        if (!triedReplicasPerDoc.TryGetValue(docId, out var triedSet))
                        {
                            triedSet = new HashSet<string>(StringComparer.Ordinal);
                            triedReplicasPerDoc[docId] = triedSet;
                        }

                        triedSet.Add(targetReplicaId);
                    }

                    batchTasks[i] = DispatchRemoteMapReduceBatchAsync(
                        targetReplicaId,
                        replicaContext.ReplicaId,
                        wireScopeId,
                        jobAlias,
                        resolvedTypeAlias,
                        docBatch,
                        serializedInput,
                        rpcTimeout,
                        clusterStateTracker,
                        directSender,
                        rpcCorrelator,
                        serializer,
                        remoteCts.Token);
                }

                var pendingTasks = new List<Task<RemoteBatchAttemptOutcome>>(batchTasks);
                var failedOrMissingDocs = new List<string>();
                var successDocsThisRound = new HashSet<string>(StringComparer.Ordinal);

                while (pendingTasks.Count > 0)
                {
                    var completedTask = await Task.WhenAny(pendingTasks).ConfigureAwait(false);
                    pendingTasks.Remove(completedTask);

                    var outcome = await completedTask.ConfigureAwait(false);
                    if (outcome.IsSuccess)
                    {
                        remoteDocsEvaluated += outcome.EvaluatedDocumentCount;

                        HashSet<string>? missingSet = null;
                        if (outcome.MissingDocumentIds is { Count: > 0 })
                        {
                            missingSet = new HashSet<string>(outcome.MissingDocumentIds, StringComparer.Ordinal);
                            failedOrMissingDocs.AddRange(outcome.MissingDocumentIds);
                        }

                        for (var r = 0; r < outcome.RequestedDocumentIds.Count; r++)
                        {
                            var reqDocId = outcome.RequestedDocumentIds[r];
                            if (missingSet is null || !missingSet.Contains(reqDocId))
                            {
                                successDocsThisRound.Add(reqDocId);
                            }
                        }

                        if (outcome.HasAccumulator)
                        {
                            var peerAcc = serializer.DeserializeFromBytes<TAccumulate>(outcome.SerializedAccumulator);
                            if (peerAcc is not null)
                            {
                                allNodeAccumulators.Add(peerAcc);
                                if (handler.CanShortCircuit(input, peerAcc))
                                {
                                    wasShortCircuited = true;
                                    await remoteCts.CancelAsync().ConfigureAwait(false);
                                    break;
                                }
                            }
                        }
                    }
                    else
                    {
                        if (outcome.Exception is not null)
                        {
                            lastRemoteException = outcome.Exception;
                        }
                        else if (!string.IsNullOrWhiteSpace(outcome.ErrorMessage))
                        {
                            lastRemoteException = new InvalidOperationException(outcome.ErrorMessage);
                        }

                        if (!wasShortCircuited)
                        {
                            failedOrMissingDocs.AddRange(outcome.RequestedDocumentIds);
                        }
                    }
                }

                foreach (var succeededDocId in successDocsThisRound)
                {
                    unreachableDocumentIds.Remove(succeededDocId);
                }

                currentBatches.Clear();

                if (wasShortCircuited || failedOrMissingDocs.Count == 0)
                {
                    break;
                }

                if (scope.RoutingMode == CrdtMapReduceRoutingMode.AllReplicas)
                {
                    for (var i = 0; i < failedOrMissingDocs.Count; i++)
                    {
                        if (!unreachableDocumentIds.Contains(failedOrMissingDocs[i]))
                        {
                            unreachableDocumentIds.Add(failedOrMissingDocs[i]);
                        }
                    }

                    break;
                }

                for (var i = 0; i < failedOrMissingDocs.Count; i++)
                {
                    var failedDocId = failedOrMissingDocs[i];
                    if (successDocsThisRound.Contains(failedDocId))
                    {
                        continue;
                    }

                    if (!triedReplicasPerDoc.TryGetValue(failedDocId, out var triedSet))
                    {
                        triedSet = new HashSet<string>(StringComparer.Ordinal);
                        triedReplicasPerDoc[failedDocId] = triedSet;
                    }

                    if (attempt < maxRetries - 1 &&
                        TryAssignFallbackReplica(failedDocId, replicaContext.ReplicaId, docCandidateReplicas, triedSet, clusterStateTracker, out var nextReplica))
                    {
                        if (!currentBatches.TryGetValue(nextReplica!, out var list))
                        {
                            list = new List<string>();
                            currentBatches[nextReplica!] = list;
                        }

                        if (!list.Contains(failedDocId))
                        {
                            list.Add(failedDocId);
                        }
                    }
                    else if (attempt < maxRetries - 1)
                    {
                        triedSet.Clear();
                        if (TryAssignFallbackReplica(failedDocId, replicaContext.ReplicaId, docCandidateReplicas, triedSet, clusterStateTracker, out var retryReplica))
                        {
                            if (!currentBatches.TryGetValue(retryReplica!, out var list))
                            {
                                list = new List<string>();
                                currentBatches[retryReplica!] = list;
                            }

                            if (!list.Contains(failedDocId))
                            {
                                list.Add(failedDocId);
                            }
                        }
                        else if (!unreachableDocumentIds.Contains(failedDocId))
                        {
                            unreachableDocumentIds.Add(failedDocId);
                        }
                    }
                    else
                    {
                        if (!unreachableDocumentIds.Contains(failedDocId))
                        {
                            unreachableDocumentIds.Add(failedDocId);
                        }
                    }
                }
            }

            remotePeersContacted = contactedPeersSet.Count;

            if (!wasShortCircuited && unreachableDocumentIds.Count > 0 && !scope.AllowPartialResultsOnPeerFailure)
            {
                if (lastRemoteException is not null)
                {
                    throw new InvalidOperationException(
                        $"Map/Reduce job '{jobAlias}' failed for document(s) [{string.Join(", ", unreachableDocumentIds)}]: {lastRemoteException.Message}",
                        lastRemoteException);
                }

                throw new TimeoutException(
                    $"Map/Reduce job '{jobAlias}' could not reach any replica for document(s) [{string.Join(", ", unreachableDocumentIds)}].");
            }
        }

        var finalAccumulator = allNodeAccumulators.Count == 1
            ? allNodeAccumulators[0]
            : handler.Combine(input, allNodeAccumulators);

        var finalResult = handler.Reduce(input, finalAccumulator);
        telemetry.RecordOrchestratorMapReduceExecuted(jobAlias, remotePeersContacted);

        return new CrdtMapReduceResultDto<TResult>(
            finalResult,
            TotalDocumentsTargeted: candidateDocuments.Count,
            LocalDocumentsEvaluated: localDocsEvaluated,
            RemoteDocumentsEvaluated: remoteDocsEvaluated,
            RemotePeersContacted: remotePeersContacted,
            WasShortCircuited: wasShortCircuited,
            UnreachableDocumentIds: unreachableDocumentIds);
    }

    private readonly record struct CandidateMapReduceDocument<TState>(
        string DocumentId,
        string ShardId,
        IDistributedCrdtDocument<TState>? LiveDocument)
        where TState : class, new();

    private readonly record struct CandidateLocalDocument<TState>(
        string DocumentId,
        string ShardId,
        IDistributedCrdtDocument<TState>? LiveDocument)
        where TState : class, new();

    private readonly record struct RemoteBatchAttemptOutcome(
        bool IsSuccess,
        bool HasAccumulator,
        int EvaluatedDocumentCount,
        byte[] SerializedAccumulator,
        IReadOnlyList<string> RequestedDocumentIds,
        IList<string>? MissingDocumentIds,
        string? ErrorMessage,
        Exception? Exception);

    private static bool ContainsReplica(IReadOnlyList<string> replicas, string replicaId)
    {
        for (var i = 0; i < replicas.Count; i++)
        {
            if (string.Equals(replicas[i], replicaId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private string ResolveMapReduceJobAlias<TState, TInput, TAccumulate, TResult>()
    {
        foreach (var executor in serviceProvider.GetServices<ICrdtMapReduceExecutor>())
        {
            if (executor.StateType == typeof(TState) &&
                executor.InputType == typeof(TInput) &&
                executor.AccumulateType == typeof(TAccumulate) &&
                executor.ResultType == typeof(TResult))
            {
                return executor.JobAlias;
            }
        }

        throw new InvalidOperationException(
            $"No ICrdtMapReduceExecutor is registered for ({typeof(TState).Name}, {typeof(TInput).Name}, {typeof(TAccumulate).Name}, {typeof(TResult).Name}).");
    }

    private async Task<List<CandidateMapReduceDocument<TState>>> ResolveCandidateMapReduceDocumentsAsync<TState>(
        ICrdtDocumentOrchestrator orchestrator,
        string documentTypeAlias,
        IShardIdProvider shardIdProvider,
        CrdtMapReduceScopeDto scope,
        CancellationToken cancellationToken)
        where TState : class, new()
    {
        var registry = orchestrator.Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");
        var registryData = registry.ReadData();
        var registryMap = registryData.Documents;
        var concreteOrchestrator = orchestrator as CrdtDocumentOrchestrator;

        var initialCapacity = scope.TargetDocumentIds is { Count: > 0 }
            ? scope.TargetDocumentIds.Count
            : registryMap.Count;
        var candidates = new List<CandidateMapReduceDocument<TState>>(initialCapacity);
        var seenDocIds = new HashSet<string>(initialCapacity, StringComparer.Ordinal);
        HashSet<string>? targetShardSet = null;

        if (scope.TargetShardIds is { Count: > 0 })
        {
            targetShardSet = new HashSet<string>(scope.TargetShardIds, StringComparer.Ordinal);
        }

        if (scope.TargetDocumentIds is { Count: > 0 })
        {
            for (var i = 0; i < scope.TargetDocumentIds.Count; i++)
            {
                var docId = scope.TargetDocumentIds[i];
                if (string.IsNullOrWhiteSpace(docId))
                {
                    continue;
                }

                docId = stringPool.GetOrAdd(docId);
                if (!seenDocIds.Add(docId))
                {
                    continue;
                }

                var compositeKey = CrdtRegistryEntry.GetRegistryKey(documentTypeAlias, docId);
                if ((registryMap.TryGetValue(compositeKey, out var entry) || registryMap.TryGetValue(docId, out entry)) &&
                    entry != null &&
                    entry.IsDeleted)
                {
                    continue;
                }

                var liveDoc = concreteOrchestrator is not null
                    ? concreteOrchestrator.TryGetLocalDocumentFast<TState>(docId, documentTypeAlias)
                    : orchestrator.TryGetLocalDocument<TState>(docId);

                string shardId;
                if (liveDoc is not null)
                {
                    shardId = liveDoc.ShardId;
                }
                else
                {
                    shardId = await orchestrator.TryGetShardIdAsync(docId, documentTypeAlias, cancellationToken).ConfigureAwait(false)
                        ?? stringPool.GetOrAdd(shardIdProvider.GetShardId<TState>(docId) ?? docId);
                }

                if (targetShardSet is null || targetShardSet.Contains(shardId))
                {
                    candidates.Add(new CandidateMapReduceDocument<TState>(docId, shardId, liveDoc));
                }
            }

            return candidates;
        }

        foreach (var kvp in registryMap)
        {
            var entry = kvp.Value;
            if (entry.IsDeleted || !string.Equals(entry.TypeAlias, documentTypeAlias, StringComparison.Ordinal))
            {
                continue;
            }

            if (!seenDocIds.Add(entry.DocumentId))
            {
                continue;
            }

            var liveDoc = concreteOrchestrator is not null
                ? concreteOrchestrator.TryGetLocalDocumentFast<TState>(entry.DocumentId, documentTypeAlias)
                : orchestrator.TryGetLocalDocument<TState>(entry.DocumentId);

            string shardId;
            if (liveDoc is not null)
            {
                shardId = liveDoc.ShardId;
            }
            else
            {
                shardId = await orchestrator.TryGetShardIdAsync(entry.DocumentId, documentTypeAlias, cancellationToken).ConfigureAwait(false)
                    ?? stringPool.GetOrAdd(shardIdProvider.GetShardId<TState>(entry.DocumentId) ?? entry.DocumentId);
            }

            if (targetShardSet is null || targetShardSet.Contains(shardId))
            {
                candidates.Add(new CandidateMapReduceDocument<TState>(entry.DocumentId, shardId, liveDoc));
            }
        }

        if (concreteOrchestrator is not null)
        {
            foreach (var activeDoc in concreteOrchestrator.EnumerateActiveDocuments())
            {
                if (!string.Equals(activeDoc.TypeAlias, documentTypeAlias, StringComparison.Ordinal))
                {
                    continue;
                }

                if (!seenDocIds.Add(activeDoc.DocumentId))
                {
                    continue;
                }

                if (targetShardSet is null || targetShardSet.Contains(activeDoc.ShardId))
                {
                    candidates.Add(new CandidateMapReduceDocument<TState>(
                        activeDoc.DocumentId,
                        activeDoc.ShardId,
                        activeDoc as IDistributedCrdtDocument<TState>));
                }
            }
        }
        else
        {
            var activeDocs = orchestrator.GetActiveDocuments();
            for (int i = 0; i < activeDocs.Count; i++)
            {
                var activeDoc = activeDocs[i];
                if (!string.Equals(activeDoc.TypeAlias, documentTypeAlias, StringComparison.Ordinal))
                {
                    continue;
                }

                if (!seenDocIds.Add(activeDoc.DocumentId))
                {
                    continue;
                }

                if (targetShardSet is null || targetShardSet.Contains(activeDoc.ShardId))
                {
                    candidates.Add(new CandidateMapReduceDocument<TState>(
                        activeDoc.DocumentId,
                        activeDoc.ShardId,
                        activeDoc as IDistributedCrdtDocument<TState>));
                }
            }
        }

        return candidates;
    }

    private static bool TryAssignFallbackReplica(
        string docId,
        string localReplicaId,
        Dictionary<string, IReadOnlyList<string>>? docCandidateReplicas,
        HashSet<string> triedReplicas,
        IClusterStateTracker clusterStateTracker,
        out string? fallbackReplica)
    {
        fallbackReplica = null;

        if (docCandidateReplicas is not null && docCandidateReplicas.TryGetValue(docId, out var candidates))
        {
            for (var i = 0; i < candidates.Count; i++)
            {
                var candidate = candidates[i];
                if (string.Equals(candidate, localReplicaId, StringComparison.Ordinal) || triedReplicas.Contains(candidate))
                {
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(clusterStateTracker.GetPeerIdByReplicaId(candidate)))
                {
                    fallbackReplica = candidate;
                    return true;
                }
            }
        }

        var activeReplicas = clusterStateTracker.GetActiveReplicaIds();
        for (var i = 0; i < activeReplicas.Count; i++)
        {
            var candidate = activeReplicas[i];
            if (string.Equals(candidate, localReplicaId, StringComparison.Ordinal) || triedReplicas.Contains(candidate))
            {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(clusterStateTracker.GetPeerIdByReplicaId(candidate)))
            {
                fallbackReplica = candidate;
                return true;
            }
        }

        return false;
    }

    private static async Task<RemoteBatchAttemptOutcome> DispatchRemoteMapReduceBatchAsync(
        string targetReplicaId,
        string senderReplicaId,
        string? wireScopeId,
        string jobAlias,
        string documentTypeAlias,
        IReadOnlyList<string> documentBatch,
        byte[] serializedInput,
        TimeSpan rpcTimeout,
        IClusterStateTracker clusterStateTracker,
        IDirectMessageSender directSender,
        IP2pRpcCorrelator rpcCorrelator,
        ICrdtSerializer serializer,
        CancellationToken cancellationToken)
    {
        var peerIdStr = clusterStateTracker.GetPeerIdByReplicaId(targetReplicaId);
        if (string.IsNullOrWhiteSpace(peerIdStr) || !Guid.TryParse(peerIdStr, out var peerGuid))
        {
            return new RemoteBatchAttemptOutcome(
                IsSuccess: false,
                HasAccumulator: false,
                EvaluatedDocumentCount: 0,
                SerializedAccumulator: Array.Empty<byte>(),
                RequestedDocumentIds: documentBatch,
                MissingDocumentIds: null,
                ErrorMessage: $"No active network peer ID found for replica '{targetReplicaId}'.",
                Exception: null);
        }

        var correlationId = Guid.NewGuid();
        var req = new CrdtMapReduceRequestDto
        {
            CorrelationId = correlationId,
            JobAlias = jobAlias,
            DocumentTypeAlias = documentTypeAlias,
            DocumentIds = documentBatch as IList<string> ?? new List<string>(documentBatch),
            SerializedInput = serializedInput
        };

        var reqBytes = serializer.SerializeToBytes(req);
        var wrapper = new CrdtMessageWrapper(
            "Cluster",
            "CrdtMapReduceReq",
            reqBytes,
            senderReplicaId,
            documentTypeAlias,
            wireScopeId);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        try
        {
            var waitTask = rpcCorrelator.RegisterRequestAsync<CrdtMapReduceResponseDto>(correlationId, rpcTimeout, cancellationToken);
            await directSender.SendDirectAsync(new PeerId(peerGuid), wrapperBytes, cancellationToken).ConfigureAwait(false);
            var response = await waitTask.ConfigureAwait(false);

            return new RemoteBatchAttemptOutcome(
                IsSuccess: response.IsSuccess,
                HasAccumulator: response.HasAccumulator,
                EvaluatedDocumentCount: response.EvaluatedDocumentCount,
                SerializedAccumulator: response.SerializedAccumulator ?? Array.Empty<byte>(),
                RequestedDocumentIds: documentBatch,
                MissingDocumentIds: response.MissingDocumentIds,
                ErrorMessage: response.ErrorMessage,
                Exception: null);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return new RemoteBatchAttemptOutcome(
                IsSuccess: false,
                HasAccumulator: false,
                EvaluatedDocumentCount: 0,
                SerializedAccumulator: Array.Empty<byte>(),
                RequestedDocumentIds: documentBatch,
                MissingDocumentIds: null,
                ErrorMessage: "Map/Reduce RPC cancelled.",
                Exception: null);
        }
        catch (Exception ex)
        {
            return new RemoteBatchAttemptOutcome(
                IsSuccess: false,
                HasAccumulator: false,
                EvaluatedDocumentCount: 0,
                SerializedAccumulator: Array.Empty<byte>(),
                RequestedDocumentIds: documentBatch,
                MissingDocumentIds: null,
                ErrorMessage: ex.Message,
                Exception: ex);
        }
    }
}