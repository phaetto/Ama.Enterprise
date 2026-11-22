namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Pooling;
using Ama.CRDT.Services.Versioning;

/// <summary>
/// Default implementation of <see cref="IShardVersionVectorManager"/> that synchronizes and isolates all mutations to <see cref="ReplicaContext.ShardVersionVectors"/>.
/// </summary>
public sealed class ShardVersionVectorManager : IShardVersionVectorManager, IDisposable
{
    private readonly ReplicaContext replicaContext;
    private readonly IVersionVectorSyncService syncService;
    private readonly ICrdtStringPool stringPool;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> shardLocks = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, DottedVersionVector>> pendingDocumentSnapshots = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, int> pendingShardExpectedCounts = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> pendingShardLastActivityTicks = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> lastShardSnapshotMergeTicks = new(StringComparer.Ordinal);
    private long lastSnapshotMergeTicks;

    public ShardVersionVectorManager(
        ReplicaContext replicaContext,
        IVersionVectorSyncService syncService,
        ICrdtStringPool? stringPool = null)
    {
        this.replicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));
        this.syncService = syncService ?? throw new ArgumentNullException(nameof(syncService));
        this.stringPool = stringPool ?? CrdtStringPool.Shared;
    }

    /// <inheritdoc />
    public IReadOnlyList<string> GetShardIds()
    {
        var sourceMap = replicaContext.ShardVersionVectors;
        lock (sourceMap)
        {
            var list = new List<string>(sourceMap.Count);
            foreach (var key in sourceMap.Keys)
            {
                list.Add(stringPool.GetOrAdd(key));
            }
            return list;
        }
    }

    /// <inheritdoc />
    public ShardVersionVectorMap GetShardSnapshot(string shardId)
    {
        if (string.IsNullOrWhiteSpace(shardId))
        {
            throw new ArgumentException("Shard ID cannot be null or empty.", nameof(shardId));
        }

        shardId = stringPool.GetOrAdd(shardId);
        var sourceMap = replicaContext.ShardVersionVectors;
        lock (sourceMap)
        {
            var map = new ShardVersionVectorMap();
            if (sourceMap.TryGetValue(shardId, out var dvv))
            {
                map[shardId] = CloneDvv(dvv, stringPool);
            }
            return map;
        }
    }

    /// <inheritdoc />
    public ShardVersionVectorMap GetShardsSnapshot(IEnumerable<string> shardIds)
    {
        ArgumentNullException.ThrowIfNull(shardIds);

        var allowedShards = shardIds as ISet<string> ?? new HashSet<string>(shardIds, StringComparer.Ordinal);
        var sourceMap = replicaContext.ShardVersionVectors;
        lock (sourceMap)
        {
            var map = new ShardVersionVectorMap();
            foreach (var shardKvp in sourceMap)
            {
                if (!allowedShards.Contains(shardKvp.Key))
                {
                    continue;
                }

                map[stringPool.GetOrAdd(shardKvp.Key)] = CloneDvv(shardKvp.Value, stringPool);
            }
            return map;
        }
    }

    /// <inheritdoc />
    public ShardVersionVectorMap GetFullSnapshot()
    {
        var sourceMap = replicaContext.ShardVersionVectors;
        lock (sourceMap)
        {
            var map = new ShardVersionVectorMap();
            foreach (var shardKvp in sourceMap)
            {
                map[stringPool.GetOrAdd(shardKvp.Key)] = CloneDvv(shardKvp.Value, stringPool);
            }
            return map;
        }
    }

    /// <inheritdoc />
    public ValueTask<ShardVersionVectorMap> GetShardsSnapshotAsync(IEnumerable<string> shardIds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(shardIds);
        cancellationToken.ThrowIfCancellationRequested();

        return new ValueTask<ShardVersionVectorMap>(GetShardsSnapshot(shardIds));
    }

    /// <inheritdoc />
    public ValueTask<ShardVersionVectorMap> GetFullSnapshotAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return new ValueTask<ShardVersionVectorMap>(GetFullSnapshot());
    }

    /// <inheritdoc />
    public void EnsureShardRegistered(string shardId)
    {
        if (string.IsNullOrWhiteSpace(shardId))
        {
            throw new ArgumentException("Shard ID cannot be null or empty.", nameof(shardId));
        }

        shardId = stringPool.GetOrAdd(shardId);
        _ = GetShardSemaphore(shardId);

        var sourceMap = replicaContext.ShardVersionVectors;
        lock (sourceMap)
        {
            if (!sourceMap.ContainsKey(shardId))
            {
                sourceMap[shardId] = new DottedVersionVector();
            }
        }
    }

    /// <inheritdoc />
    public IReadOnlySet<string> EnsureShardsRegisteredAndGetAll(IEnumerable<string> shardIds)
    {
        ArgumentNullException.ThrowIfNull(shardIds);

        var result = new HashSet<string>(StringComparer.Ordinal);
        var sourceMap = replicaContext.ShardVersionVectors;
        lock (sourceMap)
        {
            foreach (var rawShardId in shardIds)
            {
                if (!string.IsNullOrWhiteSpace(rawShardId))
                {
                    var shardId = stringPool.GetOrAdd(rawShardId);
                    _ = GetShardSemaphore(shardId);
                    if (!sourceMap.ContainsKey(shardId))
                    {
                        sourceMap[shardId] = new DottedVersionVector();
                    }
                    result.Add(shardId);
                }
            }

            foreach (var key in sourceMap.Keys)
            {
                var pooledKey = stringPool.GetOrAdd(key);
                _ = GetShardSemaphore(pooledKey);
                result.Add(pooledKey);
            }
        }

        return result;
    }

    /// <inheritdoc />
    public void MergeShardState(string shardId, ShardVersionVectorMap incomingState)
    {
        if (string.IsNullOrWhiteSpace(shardId))
        {
            throw new ArgumentException("Shard ID cannot be null or empty.", nameof(shardId));
        }
        ArgumentNullException.ThrowIfNull(incomingState);

        shardId = stringPool.GetOrAdd(shardId);
        if (!incomingState.TryGetValue(shardId, out var incomingDvv))
        {
            return;
        }

        MergeDvvIntoShard(shardId, incomingDvv);
    }

    /// <inheritdoc />
    public void RecordDocumentSnapshotMerge(string shardId, string documentId, ShardVersionVectorMap incomingState, IReadOnlyList<string> activeShardDocumentIds)
    {
        if (string.IsNullOrWhiteSpace(shardId))
        {
            throw new ArgumentException("Shard ID cannot be null or empty.", nameof(shardId));
        }
        if (string.IsNullOrWhiteSpace(documentId))
        {
            throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        }
        ArgumentNullException.ThrowIfNull(incomingState);
        ArgumentNullException.ThrowIfNull(activeShardDocumentIds);

        shardId = stringPool.GetOrAdd(shardId);
        documentId = stringPool.GetOrAdd(documentId);

        if (!incomingState.TryGetValue(shardId, out var incomingDvv))
        {
            return;
        }
        var nowTicks = Environment.TickCount64;
        Volatile.Write(ref lastSnapshotMergeTicks, nowTicks);
        lastShardSnapshotMergeTicks[shardId] = nowTicks;

        if (activeShardDocumentIds.Count <= 1)
        {
            pendingDocumentSnapshots.TryRemove(shardId, out _);
            pendingShardExpectedCounts.TryRemove(shardId, out _);
            pendingShardLastActivityTicks.TryRemove(shardId, out _);
            MergeDvvIntoShard(shardId, incomingDvv);
            return;
        }

        pendingShardExpectedCounts[shardId] = activeShardDocumentIds.Count;
        pendingShardLastActivityTicks[shardId] = nowTicks;

        var docMap = pendingDocumentSnapshots.GetOrAdd(shardId, _ => new ConcurrentDictionary<string, DottedVersionVector>(StringComparer.Ordinal));
        var clonedIncoming = CloneDvv(incomingDvv, stringPool);
        docMap.AddOrUpdate(
            documentId,
            clonedIncoming,
            (_, existing) =>
            {
                lock (existing)
                {
                    existing.Merge(clonedIncoming);
                    return existing;
                }
            });

        foreach (var activeDocId in activeShardDocumentIds)
        {
            if (!docMap.ContainsKey(activeDocId))
            {
                return;
            }
        }

        var dvvList = new List<DottedVersionVector>(activeShardDocumentIds.Count);
        foreach (var activeDocId in activeShardDocumentIds)
        {
            if (docMap.TryGetValue(activeDocId, out var docDvv))
            {
                dvvList.Add(docDvv);
            }
        }

        if (dvvList.Count == activeShardDocumentIds.Count)
        {
            var gmvv = syncService.CalculateGlobalMinimumVersionVector(dvvList);
            var mergedDots = IntersectSnapshotDots(dvvList, gmvv, stringPool);
            var safeFloorDvv = new DottedVersionVector(new Dictionary<string, long>(gmvv), mergedDots);
            MergeDvvIntoShard(shardId, safeFloorDvv);
            docMap.Clear();
            pendingDocumentSnapshots.TryRemove(shardId, out _);
            pendingShardExpectedCounts.TryRemove(shardId, out _);
            pendingShardLastActivityTicks.TryRemove(shardId, out _);
        }
    }

    /// <inheritdoc />
    public bool TryGetActiveSnapshotReceptionState(long activityWindowMilliseconds, out int remainingExpectedDocuments)
    {
        var windowMs = Math.Max(500L, activityWindowMilliseconds);
        var nowTicks = Environment.TickCount64;
        var lastGlobalTicks = Volatile.Read(ref lastSnapshotMergeTicks);
        var isActive = lastGlobalTicks > 0 && (nowTicks - lastGlobalTicks) < windowMs;
        var totalRemaining = 0;

        foreach (var kvp in pendingDocumentSnapshots)
        {
            var shardId = kvp.Key;
            var docMap = kvp.Value;
            if (!pendingShardLastActivityTicks.TryGetValue(shardId, out var shardLastTicks) ||
                (nowTicks - shardLastTicks) >= windowMs)
            {
                // Stale incomplete batch (e.g., sender disconnected mid-stream): clean up tracking so future batches aren't blocked.
                pendingDocumentSnapshots.TryRemove(shardId, out _);
                pendingShardExpectedCounts.TryRemove(shardId, out _);
                pendingShardLastActivityTicks.TryRemove(shardId, out _);
                continue;
            }

            isActive = true;
            var receivedCount = docMap.Count;
            if (pendingShardExpectedCounts.TryGetValue(shardId, out var expectedCount) && expectedCount > receivedCount)
            {
                totalRemaining += expectedCount - receivedCount;
            }
            else
            {
                totalRemaining += 1;
            }
        }

        remainingExpectedDocuments = totalRemaining;
        return isActive;
    }

    /// <inheritdoc />
    public bool TryGetShardSnapshotReceptionState(string shardId, long activityWindowMilliseconds, out int remainingExpectedDocuments)
    {
        if (string.IsNullOrWhiteSpace(shardId))
        {
            throw new ArgumentException("Shard ID cannot be null or empty.", nameof(shardId));
        }

        shardId = stringPool.GetOrAdd(shardId);
        var windowMs = Math.Max(500L, activityWindowMilliseconds);
        var nowTicks = Environment.TickCount64;
        var isActive = lastShardSnapshotMergeTicks.TryGetValue(shardId, out var lastShardTicks) &&
                       lastShardTicks > 0 &&
                       (nowTicks - lastShardTicks) < windowMs;
        var remaining = 0;

        if (pendingDocumentSnapshots.TryGetValue(shardId, out var docMap))
        {
            if (!pendingShardLastActivityTicks.TryGetValue(shardId, out var shardLastTicks) ||
                (nowTicks - shardLastTicks) >= windowMs)
            {
                pendingDocumentSnapshots.TryRemove(shardId, out _);
                pendingShardExpectedCounts.TryRemove(shardId, out _);
                pendingShardLastActivityTicks.TryRemove(shardId, out _);
            }
            else
            {
                isActive = true;
                var receivedCount = docMap.Count;
                if (pendingShardExpectedCounts.TryGetValue(shardId, out var expectedCount) && expectedCount > receivedCount)
                {
                    remaining = expectedCount - receivedCount;
                }
                else
                {
                    remaining = 1;
                }
            }
        }

        remainingExpectedDocuments = remaining;
        return isActive;
    }

    /// <inheritdoc />
    public void RestoreFullState(ShardVersionVectorMap savedMap)
    {
        ArgumentNullException.ThrowIfNull(savedMap);

        var sourceMap = replicaContext.ShardVersionVectors;
        lock (sourceMap)
        {
            sourceMap.Clear();
            foreach (var kvp in savedMap)
            {
                var pooledKey = stringPool.GetOrAdd(kvp.Key);
                _ = GetShardSemaphore(pooledKey);
                sourceMap[pooledKey] = CloneDvv(kvp.Value, stringPool);
            }
        }
    }

    /// <inheritdoc />
    public void RemoveShard(string shardId)
    {
        if (string.IsNullOrWhiteSpace(shardId))
        {
            throw new ArgumentException("Shard ID cannot be null or empty.", nameof(shardId));
        }

        shardId = stringPool.GetOrAdd(shardId);
        pendingDocumentSnapshots.TryRemove(shardId, out _);
        pendingShardExpectedCounts.TryRemove(shardId, out _);
        pendingShardLastActivityTicks.TryRemove(shardId, out _);
        lastShardSnapshotMergeTicks.TryRemove(shardId, out _);

        var sourceMap = replicaContext.ShardVersionVectors;
        lock (sourceMap)
        {
            sourceMap.Remove(shardId);
        }
    }

    /// <inheritdoc />
    public void EvictReplicas(IReadOnlyList<string> replicaIds)
    {
        if (replicaIds == null || replicaIds.Count == 0)
        {
            return;
        }

        pendingDocumentSnapshots.Clear();
        pendingShardExpectedCounts.Clear();
        pendingShardLastActivityTicks.Clear();
        lastShardSnapshotMergeTicks.Clear();

        var sourceMap = replicaContext.ShardVersionVectors;
        lock (sourceMap)
        {
            foreach (var kvp in sourceMap)
            {
                var dvv = kvp.Value;
                lock (dvv)
                {
                    var cleanedDvv = syncService.RemoveEvictedReplicas(dvv, replicaIds);

                    dvv.Versions.Clear();
                    foreach (var versionKvp in cleanedDvv.Versions)
                    {
                        dvv.Versions[stringPool.GetOrAdd(versionKvp.Key)] = versionKvp.Value;
                    }

                    dvv.Dots.Clear();
                    if (cleanedDvv.Dots != null)
                    {
                        foreach (var dotKvp in cleanedDvv.Dots)
                        {
                            dvv.Dots[stringPool.GetOrAdd(dotKvp.Key)] = new HashSet<long>(dotKvp.Value);
                        }
                    }
                }
            }
        }
    }

    /// <inheritdoc />
    public void Clear()
    {
        pendingDocumentSnapshots.Clear();
        pendingShardExpectedCounts.Clear();
        pendingShardLastActivityTicks.Clear();
        lastShardSnapshotMergeTicks.Clear();

        var sourceMap = replicaContext.ShardVersionVectors;
        lock (sourceMap)
        {
            sourceMap.Clear();
        }
    }

    /// <inheritdoc />
    public async Task ExecuteShardLockedAsync(string shardId, Func<Task> action, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(shardId))
        {
            throw new ArgumentException("Shard ID cannot be null or empty.", nameof(shardId));
        }
        ArgumentNullException.ThrowIfNull(action);

        shardId = stringPool.GetOrAdd(shardId);
        var sem = GetShardSemaphore(shardId);
        await sem.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await action().ConfigureAwait(false);
        }
        finally
        {
            sem.Release();
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var sem in shardLocks.Values)
        {
            sem.Dispose();
        }
        shardLocks.Clear();
        pendingDocumentSnapshots.Clear();
        pendingShardExpectedCounts.Clear();
        pendingShardLastActivityTicks.Clear();
    }

    private SemaphoreSlim GetShardSemaphore(string shardId)
    {
        return shardLocks.GetOrAdd(stringPool.GetOrAdd(shardId), _ => new SemaphoreSlim(1, 1));
    }

    private void MergeDvvIntoShard(string shardId, DottedVersionVector incomingDvv)
    {
        shardId = stringPool.GetOrAdd(shardId);
        var sourceMap = replicaContext.ShardVersionVectors;
        lock (sourceMap)
        {
            if (!sourceMap.TryGetValue(shardId, out var dvv))
            {
                dvv = new DottedVersionVector();
                sourceMap[shardId] = dvv;
            }

            lock (dvv)
            {
                var localReplicaId = replicaContext.ReplicaId;
                var hasLocalProgress = !string.IsNullOrEmpty(localReplicaId) &&
                    ((dvv.Versions.TryGetValue(localReplicaId, out var localVer) && localVer > 0) ||
                     (dvv.Dots != null && dvv.Dots.TryGetValue(localReplicaId, out var localDots) && localDots.Count > 0));

                if (hasLocalProgress &&
                    ((incomingDvv.Versions != null && incomingDvv.Versions.ContainsKey(localReplicaId!)) ||
                     (incomingDvv.Dots != null && incomingDvv.Dots.ContainsKey(localReplicaId!))))
                {
                    var sanitized = CloneDvv(incomingDvv, stringPool);
                    sanitized.Versions.Remove(localReplicaId!);
                    sanitized.Dots?.Remove(localReplicaId!);
                    dvv.Merge(sanitized);
                }
                else
                {
                    dvv.Merge(incomingDvv);
                }
            }
        }
    }

    private static DottedVersionVector CloneDvv(DottedVersionVector dvv, ICrdtStringPool stringPool)
    {
        var copiedVersions = new Dictionary<string, long>(StringComparer.Ordinal);
        var copiedDots = new Dictionary<string, ISet<long>>(StringComparer.Ordinal);

        lock (dvv)
        {
            foreach (var kvp in dvv.Versions)
            {
                copiedVersions[stringPool.GetOrAdd(kvp.Key)] = kvp.Value;
            }

            if (dvv.Dots != null)
            {
                foreach (var kvp in dvv.Dots)
                {
                    copiedDots[stringPool.GetOrAdd(kvp.Key)] = new HashSet<long>(kvp.Value);
                }
            }
        }

        return new DottedVersionVector(copiedVersions, copiedDots);
    }

    private static IDictionary<string, ISet<long>> IntersectSnapshotDots(
        IReadOnlyList<DottedVersionVector> dvvList,
        IReadOnlyDictionary<string, long> gmvv,
        ICrdtStringPool stringPool)
    {
        var result = new Dictionary<string, ISet<long>>(StringComparer.Ordinal);
        if (dvvList.Count == 0)
        {
            return result;
        }

        var allReplicasWithDots = new HashSet<string>(StringComparer.Ordinal);
        for (int i = 0; i < dvvList.Count; i++)
        {
            var dots = dvvList[i].Dots;
            if (dots != null)
            {
                foreach (var replicaId in dots.Keys)
                {
                    allReplicasWithDots.Add(replicaId);
                }
            }
        }

        foreach (var replicaId in allReplicasWithDots)
        {
            var floorVersion = gmvv.TryGetValue(replicaId, out var fv) ? fv : 0L;
            HashSet<long>? commonDots = null;

            for (int i = 0; i < dvvList.Count; i++)
            {
                var dvv = dvvList[i];
                var dvvContiguous = dvv.Versions != null && dvv.Versions.TryGetValue(replicaId, out var cv) ? cv : 0L;
                ISet<long>? dvvReplicaDots = null;
                dvv.Dots?.TryGetValue(replicaId, out dvvReplicaDots);

                if (i == 0)
                {
                    commonDots = new HashSet<long>();
                    if (dvvReplicaDots != null)
                    {
                        foreach (var dot in dvvReplicaDots)
                        {
                            if (dot > floorVersion)
                            {
                                commonDots.Add(dot);
                            }
                        }
                    }

                    for (long c = floorVersion + 1; c <= dvvContiguous; c++)
                    {
                        commonDots.Add(c);
                    }
                }
                else if (commonDots != null && commonDots.Count > 0)
                {
                    commonDots.RemoveWhere(dot =>
                        dot <= floorVersion ||
                        (dot > dvvContiguous && (dvvReplicaDots == null || !dvvReplicaDots.Contains(dot))));
                }
            }

            if (commonDots != null && commonDots.Count > 0)
            {
                result[stringPool.GetOrAdd(replicaId)] = commonDots;
            }
        }

        return result;
    }
}