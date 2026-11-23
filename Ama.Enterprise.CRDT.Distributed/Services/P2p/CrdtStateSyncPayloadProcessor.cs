namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Journaling;
using Ama.CRDT.Services.Serialization;
using Ama.CRDT.Services.Versioning;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Default implementation of <see cref="ICrdtStateSyncPayloadProcessor"/> managing causal diff calculation, per-peer dispatch cooldowns, and snapshot streaming.
/// </summary>
public sealed class CrdtStateSyncPayloadProcessor : ICrdtStateSyncPayloadProcessor
{
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<CrdtStateSyncPayloadProcessor> logger;
    private readonly ConcurrentDictionary<(string PeerReplicaId, string ShardId), SnapshotDispatchState> recentSnapshotDispatches = new();

    private readonly record struct SnapshotDispatchState(long DispatchedAtTicks, long CooldownMs, DottedVersionVector DispatchedDvv);

    public CrdtStateSyncPayloadProcessor(
        ICrdtSerializer serializer,
        ILogger<CrdtStateSyncPayloadProcessor> logger)
    {
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task ProcessStateSyncAsync(
        IDistributedCrdtScope scope,
        string meshId,
        CrdtMessageWrapper wrapper,
        PeerId senderId,
        Func<IDistributedCrdtScope, string, string, string, CancellationToken, Task> rejectEvictedReplicaAsync,
        Func<IDistributedCrdtScope, string, PeerId, bool> isLocalLoopbackSender,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(wrapper);
        ArgumentNullException.ThrowIfNull(rejectEvictedReplicaAsync);
        ArgumentNullException.ThrowIfNull(isLocalLoopbackSender);

        if (senderId.Value != Guid.Empty)
        {
            var topologyProvider = scope.ServiceProvider.GetService<IScopeTopologyProvider>();
            if (topologyProvider != null)
            {
                var isExpected = await topologyProvider.IsPeerExpectedAsync(senderId.Value.ToString(), cancellationToken).ConfigureAwait(false);
                if (!isExpected)
                {
                    logger.LogWarning(
                        "[{ReplicaId}] Rejected CrdtSync payload from unexpected peer '{PeerId}'.",
                        scope.ReplicaId,
                        senderId.Value);
                    return;
                }
            }
        }

        var syncMsg = serializer.DeserializeFromBytes<CrdtStateSyncMessage>(wrapper.Payload!);

        if (syncMsg.ReplicaId != null && scope.ClusterTracker.IsReplicaTombstoned(syncMsg.ReplicaId))
        {
            await rejectEvictedReplicaAsync(scope, meshId, scope.Orchestrator.Registry.DocumentId, syncMsg.ReplicaId, cancellationToken).ConfigureAwait(false);
            return;
        }

        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        var versionVectorManager = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        var directSender = scope.ServiceProvider.GetService<IDirectMessageSender>();
        var syncService = scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
        var journalManager = scope.ServiceProvider.GetRequiredService<IJournalManager>();
        var distributionManager = scope.ServiceProvider.GetRequiredService<IShardDistributionManager>();
        var antiEntropyCoordinator = scope.ServiceProvider.GetService<ICrdtAntiEntropyCoordinator>();

        if (string.IsNullOrEmpty(syncMsg.ReplicaId) || syncMsg.State == null)
        {
            return;
        }

        if (string.Equals(syncMsg.ReplicaId, replicaContext.ReplicaId, StringComparison.Ordinal))
        {
            if (isLocalLoopbackSender(scope, meshId, senderId))
            {
                return;
            }

            logger.LogCritical(
                "[{ReplicaId}] Duplicate replica ID collision detected! Received CrdtSync message from remote peer '{PeerId}' claiming the same ReplicaId '{DuplicateReplicaId}'. Ignoring payload to prevent DVV corruption.",
                replicaContext.ReplicaId,
                senderId.Value,
                syncMsg.ReplicaId);
            return;
        }

        scope.ClusterTracker.UpdatePeerState(syncMsg.ReplicaId, senderId.Value.ToString(), syncMsg.State);

        var concreteOrchestrator = scope.Orchestrator as CrdtDocumentOrchestrator;
        var safeLocalState = await versionVectorManager.GetFullSnapshotAsync(cancellationToken).ConfigureAwait(false);

        var allShardsToEvaluate = new HashSet<string>(StringComparer.Ordinal);
        if (syncMsg.IsTopologyAnnouncementOnly)
        {
            var registryShardId = scope.Orchestrator.Registry?.ShardId;
            if (!string.IsNullOrWhiteSpace(registryShardId))
            {
                allShardsToEvaluate.Add(registryShardId);
            }
        }
        else
        {
            foreach (var remoteShardId in syncMsg.State.Keys)
            {
                allShardsToEvaluate.Add(remoteShardId);
            }

            foreach (var localShardId in safeLocalState.Keys)
            {
                var targetNodes = await distributionManager.GetTargetNodesForShardAsync(localShardId, cancellationToken).ConfigureAwait(false);
                if (targetNodes.Contains(syncMsg.ReplicaId, StringComparer.Ordinal))
                {
                    allShardsToEvaluate.Add(localShardId);
                }
            }
        }

        var documents = scope.Orchestrator.GetActiveDocuments();
        var localActiveShards = new HashSet<string>(StringComparer.Ordinal);
        foreach (var doc in documents)
        {
            localActiveShards.Add(doc.ShardId);
        }

        foreach (var knownLocalShard in safeLocalState.Keys)
        {
            localActiveShards.Add(knownLocalShard);
        }

        allShardsToEvaluate.IntersectWith(localActiveShards);

        var acquiredShardScopes = new List<IDisposable>(allShardsToEvaluate.Count);
        try
        {
            if (antiEntropyCoordinator != null)
            {
                var orchestratorPending = concreteOrchestrator?.PendingCommandCount ?? 0;
                var availableShards = new HashSet<string>(StringComparer.Ordinal);
                foreach (var shardId in allShardsToEvaluate)
                {
                    var shardScope = antiEntropyCoordinator.TryAcquireAntiEntropyWorkScope(shardId, orchestratorPending);
                    if (shardScope != null)
                    {
                        acquiredShardScopes.Add(shardScope);
                        availableShards.Add(shardId);
                    }
                }

                if (allShardsToEvaluate.Count > 0 && availableShards.Count == 0)
                {
                    logger.LogDebug(
                        "[{ReplicaId}] Deferring incoming CrdtSync response to peer '{PeerReplicaId}' because all {ShardCount} target shard(s) or local queues are already busy.",
                        replicaContext.ReplicaId,
                        syncMsg.ReplicaId,
                        allShardsToEvaluate.Count);
                    return;
                }

                allShardsToEvaluate = availableShards;
            }

            var requirements = new List<ReplicaSyncRequirement>();
            var missingOpsStreams = new List<IAsyncEnumerable<JournaledOperation>>();
            var shardsWithNoCausalOverlap = new HashSet<string>(StringComparer.Ordinal);

            foreach (var shardId in allShardsToEvaluate)
            {
                var sourceVector = syncMsg.State.TryGetValue(shardId, out var sv) ? sv : new DottedVersionVector();
                var targetVector = safeLocalState.TryGetValue(shardId, out var tv) ? tv : new DottedVersionVector();

                var req = syncService.CalculateRequirement(syncMsg.ReplicaId, sourceVector, replicaContext.ReplicaId, targetVector, shardId);
                if (req.IsBehind)
                {
                    requirements.Add(req);
                    missingOpsStreams.Add(journalManager.GetMissingOperationsAsync(shardId, req, cancellationToken));
                }

                if (!req.IsBehind && IsVersionVectorEmpty(sourceVector))
                {
                    var registryDocInstance = scope.Orchestrator.Registry;
                    if (syncMsg.IsTopologyAnnouncementOnly &&
                        registryDocInstance != null &&
                        string.Equals(shardId, registryDocInstance.ShardId, StringComparison.Ordinal))
                    {
                        var localRegistryDoc = await registryDocInstance.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
                        if (localRegistryDoc.Data?.Documents is { Count: > 0 })
                        {
                            shardsWithNoCausalOverlap.Add(shardId);
                        }
                    }
                    else if (!syncMsg.IsTopologyAnnouncementOnly
                         && !(IsVersionVectorEmpty(sourceVector) && IsVersionVectorEmpty(targetVector)))
                    {
                        var targetNodes = await distributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
                        if (targetNodes.Contains(syncMsg.ReplicaId, StringComparer.Ordinal))
                        {
                            for (int d = 0; d < documents.Count; d++)
                            {
                                if (string.Equals(documents[d].ShardId, shardId, StringComparison.Ordinal) &&
                                    !string.Equals(documents[d].DocumentId, scope.Orchestrator.Registry.DocumentId, StringComparison.Ordinal))
                                {
                                    shardsWithNoCausalOverlap.Add(shardId);
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            if (requirements.Count == 0 && shardsWithNoCausalOverlap.Count == 0)
            {
                return;
            }

            var shardsRequiringSnapshot = new HashSet<string>(shardsWithNoCausalOverlap, StringComparer.Ordinal);
            var allOps = new List<JournaledOperation>();

            for (int i = 0; i < requirements.Count; i++)
            {
                var syncResult = await syncService.EvaluateJournalCompletionAsync(missingOpsStreams[i], requirements[i], cancellationToken).ConfigureAwait(false);
                if (syncResult.SnapshotRequired)
                {
                    shardsRequiringSnapshot.Add(requirements[i].ShardId);
                }
                else
                {
                    allOps.AddRange(syncResult.Operations);
                }
            }

            if (shardsRequiringSnapshot.Count > 0)
            {
                var nowTicks = Environment.TickCount64;
                var options = scope.ServiceProvider.GetService<IOptions<DistributedCrdtOptions>>()?.Value;

                shardsRequiringSnapshot.RemoveWhere(shardId =>
                {
                    var key = (syncMsg.ReplicaId, shardId);
                    if (!recentSnapshotDispatches.TryGetValue(key, out var prevDispatch))
                    {
                        return false;
                    }

                    if (nowTicks - prevDispatch.DispatchedAtTicks >= prevDispatch.CooldownMs)
                    {
                        return false;
                    }

                    if (!safeLocalState.TryGetValue(shardId, out var currentLocalDvv))
                    {
                        return true;
                    }

                    var deltaReq = syncService.CalculateRequirement(
                        syncMsg.ReplicaId,
                        prevDispatch.DispatchedDvv,
                        replicaContext.ReplicaId,
                        currentLocalDvv,
                        shardId);

                    return !deltaReq.IsBehind;
                });

                if (shardsRequiringSnapshot.Count > 0)
                {
                    logger.LogWarning("[{ReplicaId}] Journal missing or trimmed for {ShardCount} shard(s). Sending full snapshots for affected shards.", replicaContext.ReplicaId, shardsRequiringSnapshot.Count);
                    var sentSnapshots = new HashSet<string>(StringComparer.Ordinal);
                    var shardDocsToSend = new Dictionary<string, List<(string DocumentId, string? TypeAlias, string CompositeKey)>>(StringComparer.Ordinal);
                    var shardDocumentKeyLists = new Dictionary<string, List<string>>(StringComparer.Ordinal);
                    var dispatchedShardState = new ShardVersionVectorMap();

                    var registryDocument = scope.Orchestrator.Registry;
                    if (registryDocument != null)
                    {
                        shardsRequiringSnapshot.Add(registryDocument.ShardId);
                    }

                    foreach (var targetDoc in documents)
                    {
                        if (shardsRequiringSnapshot.Contains(targetDoc.ShardId))
                        {
                            var isRegistryDoc = string.Equals(targetDoc.DocumentId, scope.Orchestrator.Registry.DocumentId, StringComparison.Ordinal);
                            var compositeKey = isRegistryDoc
                                ? targetDoc.DocumentId
                                : CrdtRegistryEntry.GetRegistryKey(targetDoc.TypeAlias, targetDoc.DocumentId);

                            if (sentSnapshots.Add(compositeKey))
                            {
                                if (!shardDocsToSend.TryGetValue(targetDoc.ShardId, out var docList))
                                {
                                    docList = new List<(string DocumentId, string? TypeAlias, string CompositeKey)>();
                                    shardDocsToSend[targetDoc.ShardId] = docList;
                                }
                                docList.Add((targetDoc.DocumentId, targetDoc.TypeAlias, compositeKey));

                                if (!shardDocumentKeyLists.TryGetValue(targetDoc.ShardId, out var keyList))
                                {
                                    keyList = new List<string>();
                                    shardDocumentKeyLists[targetDoc.ShardId] = keyList;
                                }
                                keyList.Add(compositeKey);
                            }
                        }
                    }

                    var registryDoc = await scope.Orchestrator.Registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
                    foreach (var entry in registryDoc.Data.Documents.Values)
                    {
                        if (entry.IsDeleted)
                        {
                            continue;
                        }

                        var compositeKey = CrdtRegistryEntry.GetRegistryKey(entry.TypeAlias, entry.DocumentId);
                        if (sentSnapshots.Contains(compositeKey))
                        {
                            continue;
                        }

                        var entryShardId = await scope.Orchestrator.TryGetShardIdAsync(entry.DocumentId, entry.TypeAlias, cancellationToken).ConfigureAwait(false);
                        if (!string.IsNullOrWhiteSpace(entryShardId) && shardsRequiringSnapshot.Contains(entryShardId))
                        {
                            sentSnapshots.Add(compositeKey);

                            if (!shardDocsToSend.TryGetValue(entryShardId, out var docList))
                            {
                                docList = new List<(string DocumentId, string? TypeAlias, string CompositeKey)>();
                                shardDocsToSend[entryShardId] = docList;
                            }
                            docList.Add((entry.DocumentId, entry.TypeAlias, compositeKey));

                            if (!shardDocumentKeyLists.TryGetValue(entryShardId, out var keyList))
                            {
                                keyList = new List<string>();
                                shardDocumentKeyLists[entryShardId] = keyList;
                            }
                            keyList.Add(compositeKey);
                        }
                    }

                    var latestSnapshotState = await versionVectorManager.GetShardsSnapshotAsync(shardsRequiringSnapshot, cancellationToken).ConfigureAwait(false);
                    var orderedShards = shardDocsToSend
                        .OrderByDescending(kvp => registryDocument != null && string.Equals(kvp.Key, registryDocument.ShardId, StringComparison.Ordinal))
                        .ToList();

                    foreach (var shardKvp in orderedShards)
                    {
                        var shardId = shardKvp.Key;
                        var docsInShard = shardKvp.Value;
                        shardDocumentKeyLists.TryGetValue(shardId, out var batchKeys);

                        var startupSec = options?.AntiEntropyStartupIntervalSeconds ?? 2;
                        var baseDynamicCooldownMs = startupSec <= 1 ? 500L : Math.Max(500L, startupSec * 1000L);
                        var dynamicCooldownMs = startupSec <= 1
                            ? baseDynamicCooldownMs
                            : Math.Clamp(baseDynamicCooldownMs + (docsInShard.Count * 25L), baseDynamicCooldownMs, 300_000L);

                        ShardVersionVectorMap? consistentShardState = null;
                        if (latestSnapshotState.TryGetValue(shardId, out var shardDvv) ||
                            safeLocalState.TryGetValue(shardId, out shardDvv))
                        {
                            consistentShardState = new ShardVersionVectorMap { [shardId] = shardDvv };
                            dispatchedShardState[shardId] = shardDvv;
                            recentSnapshotDispatches[(syncMsg.ReplicaId, shardId)] = new SnapshotDispatchState(Environment.TickCount64, dynamicCooldownMs, shardDvv);
                        }

                        for (int i = 0; i < docsInShard.Count; i++)
                        {
                            var item = docsInShard[i];
                            await scope.Orchestrator.ProvideSnapshotAsync(
                                item.DocumentId,
                                item.TypeAlias,
                                syncMsg.ReplicaId,
                                senderId,
                                batchKeys,
                                consistentShardState,
                                cancellationToken).ConfigureAwait(false);

                            if (consistentShardState != null && (i + 1) % 25 == 0 && consistentShardState.TryGetValue(shardId, out var refreshedDvv))
                            {
                                recentSnapshotDispatches[(syncMsg.ReplicaId, shardId)] = new SnapshotDispatchState(Environment.TickCount64, dynamicCooldownMs, refreshedDvv);
                            }
                        }
                    }

                    if (dispatchedShardState.Count > 0)
                    {
                        scope.ClusterTracker.UpdatePeerState(syncMsg.ReplicaId, senderId.Value.ToString(), dispatchedShardState);
                    }
                }
            }

            if (allOps.Count > 0 && directSender != null)
            {
                var nowTicks = Environment.TickCount64;
                var options = scope.ServiceProvider.GetService<IOptions<DistributedCrdtOptions>>()?.Value;
                var startupSec = options?.AntiEntropyStartupIntervalSeconds ?? 2;
                var basePatchCooldownMs = startupSec <= 1 ? 500L : Math.Max(1500L, startupSec * 1000L);

                var opsByShard = allOps
                    .Where(x => !string.IsNullOrEmpty(x.Operation.ShardId) && !shardsRequiringSnapshot.Contains(x.Operation.ShardId!))
                    .GroupBy(x => x.Operation.ShardId!, StringComparer.Ordinal)
                    .ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);

                var throttledPatchShards = new HashSet<string>(StringComparer.Ordinal);
                foreach (var shardOpKvp in opsByShard)
                {
                    var shardId = shardOpKvp.Key;
                    var key = (syncMsg.ReplicaId, shardId);
                    if (recentSnapshotDispatches.TryGetValue(key, out var prevDispatch) &&
                        nowTicks - prevDispatch.DispatchedAtTicks < prevDispatch.CooldownMs &&
                        safeLocalState.TryGetValue(shardId, out var currentLocalDvv))
                    {
                        var deltaReq = syncService.CalculateRequirement(
                            syncMsg.ReplicaId,
                            prevDispatch.DispatchedDvv,
                            replicaContext.ReplicaId,
                            currentLocalDvv,
                            shardId);

                        if (!deltaReq.IsBehind)
                        {
                            throttledPatchShards.Add(shardId);
                            continue;
                        }
                    }

                    if (safeLocalState.TryGetValue(shardId, out var dispatchedDvv))
                    {
                        var dynamicPatchCooldownMs = startupSec <= 1
                            ? basePatchCooldownMs
                            : Math.Clamp(basePatchCooldownMs + (shardOpKvp.Value * 5L), basePatchCooldownMs, 300_000L);
                        recentSnapshotDispatches[key] = new SnapshotDispatchState(nowTicks, dynamicPatchCooldownMs, dispatchedDvv);
                    }
                }

                var opsByDoc = allOps
                    .Where(x => string.IsNullOrEmpty(x.Operation.ShardId) || !throttledPatchShards.Contains(x.Operation.ShardId))
                    .GroupBy(x => (Key: x.DocumentId?.ToString() ?? string.Empty, ShardId: x.Operation.ShardId ?? string.Empty))
                    .ToDictionary(g => g.Key, g => g.Select(x => x.Operation).ToArray());

                var sentBucketKeys = new HashSet<(string Key, string ShardId)>();

                foreach (var targetDoc in documents)
                {
                    if (shardsRequiringSnapshot.Contains(targetDoc.ShardId) || throttledPatchShards.Contains(targetDoc.ShardId))
                    {
                        continue;
                    }

                    var compositeKey = CrdtRegistryEntry.GetRegistryKey(targetDoc.TypeAlias, targetDoc.DocumentId);
                    CrdtOperation[]? docOps = null;
                    if (opsByDoc.TryGetValue((compositeKey, targetDoc.ShardId), out docOps))
                    {
                        sentBucketKeys.Add((compositeKey, targetDoc.ShardId));
                    }
                    else if (opsByDoc.TryGetValue((targetDoc.DocumentId, targetDoc.ShardId), out docOps))
                    {
                        sentBucketKeys.Add((targetDoc.DocumentId, targetDoc.ShardId));
                    }
                    else if (opsByDoc.TryGetValue((compositeKey, string.Empty), out docOps))
                    {
                        sentBucketKeys.Add((compositeKey, string.Empty));
                    }
                    else if (opsByDoc.TryGetValue((targetDoc.DocumentId, string.Empty), out docOps))
                    {
                        sentBucketKeys.Add((targetDoc.DocumentId, string.Empty));
                    }

                    if (docOps != null && docOps.Length > 0)
                    {
                        var patchMsg = new CrdtPatchMessage(replicaContext.ReplicaId, new CrdtPatch(docOps));
                        var patchPayload = serializer.SerializeToBytes(patchMsg);
                        var docTypeAlias = string.Equals(targetDoc.DocumentId, scope.Orchestrator.Registry.DocumentId, StringComparison.Ordinal)
                            ? null
                            : targetDoc.TypeAlias;

                        var replyWrapper = new CrdtMessageWrapper(targetDoc.DocumentId, "CrdtPatch", patchPayload, docTypeAlias);
                        var replyBytes = serializer.SerializeToBytes(replyWrapper);

                        await directSender.SendDirectAsync(senderId, replyBytes, cancellationToken).ConfigureAwait(false);
                    }
                }

                foreach (var kvp in opsByDoc)
                {
                    if (sentBucketKeys.Contains(kvp.Key))
                    {
                        continue;
                    }

                    if (!string.IsNullOrEmpty(kvp.Key.ShardId) &&
                        (shardsRequiringSnapshot.Contains(kvp.Key.ShardId) || throttledPatchShards.Contains(kvp.Key.ShardId)))
                    {
                        continue;
                    }

                    var docOps = kvp.Value;
                    if (docOps == null || docOps.Length == 0 || string.IsNullOrWhiteSpace(kvp.Key.Key))
                    {
                        continue;
                    }

                    string rawDocId = kvp.Key.Key;
                    string? docTypeAlias = null;
                    var colonIdx = rawDocId.IndexOf(':', StringComparison.Ordinal);
                    if (colonIdx > 0 && colonIdx < rawDocId.Length - 1)
                    {
                        docTypeAlias = rawDocId[..colonIdx];
                        rawDocId = rawDocId[(colonIdx + 1)..];
                    }

                    var patchMsg = new CrdtPatchMessage(replicaContext.ReplicaId, new CrdtPatch(docOps));
                    var patchPayload = serializer.SerializeToBytes(patchMsg);
                    var replyWrapper = new CrdtMessageWrapper(rawDocId, "CrdtPatch", patchPayload, docTypeAlias);
                    var replyBytes = serializer.SerializeToBytes(replyWrapper);

                    await directSender.SendDirectAsync(senderId, replyBytes, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            for (int i = 0; i < acquiredShardScopes.Count; i++)
            {
                acquiredShardScopes[i].Dispose();
            }
        }
    }

    private static bool IsVersionVectorEmpty(DottedVersionVector dvv)
    {
        if (dvv.Versions != null)
        {
            foreach (var v in dvv.Versions.Values)
            {
                if (v > 0)
                {
                    return false;
                }
            }
        }

        if (dvv.Dots != null)
        {
            foreach (var dots in dvv.Dots.Values)
            {
                if (dots != null && dots.Count > 0)
                {
                    return false;
                }
            }
        }

        return true;
    }
}