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
    public async ValueTask<ShardVersionVectorMap> GetShardsSnapshotAsync(IEnumerable<string> shardIds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(shardIds);

        var sortedShards = shardIds
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => stringPool.GetOrAdd(s))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

        var acquiredLocks = new List<SemaphoreSlim>(sortedShards.Count);
        try
        {
            foreach (var shardId in sortedShards)
            {
                var sem = GetShardSemaphore(shardId);
                await sem.WaitAsync(cancellationToken).ConfigureAwait(false);
                acquiredLocks.Add(sem);
            }

            return GetShardsSnapshot(sortedShards);
        }
        finally
        {
            for (var i = acquiredLocks.Count - 1; i >= 0; i--)
            {
                acquiredLocks[i].Release();
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<ShardVersionVectorMap> GetFullSnapshotAsync(CancellationToken cancellationToken = default)
    {
        var shardIds = GetShardIds();
        return await GetShardsSnapshotAsync(shardIds, cancellationToken).ConfigureAwait(false);
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

        if (activeShardDocumentIds.Count <= 1)
        {
            pendingDocumentSnapshots.TryRemove(shardId, out _);
            MergeDvvIntoShard(shardId, incomingDvv);
            return;
        }

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
            var safeFloorDvv = new DottedVersionVector(new Dictionary<string, long>(gmvv), new Dictionary<string, ISet<long>>());
            MergeDvvIntoShard(shardId, safeFloorDvv);
            docMap.Clear();
        }
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
}