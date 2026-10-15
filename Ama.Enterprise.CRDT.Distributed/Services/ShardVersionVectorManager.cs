namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Versioning;

/// <summary>
/// Default implementation of <see cref="IShardVersionVectorManager"/> that synchronizes and isolates all mutations to <see cref="ReplicaContext.ShardVersionVectors"/>.
/// </summary>
public sealed class ShardVersionVectorManager : IShardVersionVectorManager, IDisposable
{
    private readonly ReplicaContext replicaContext;
    private readonly IVersionVectorSyncService syncService;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> shardLocks = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, DottedVersionVector>> pendingDocumentSnapshots = new(StringComparer.Ordinal);

    public ShardVersionVectorManager(
        ReplicaContext replicaContext,
        IVersionVectorSyncService syncService)
    {
        this.replicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));
        this.syncService = syncService ?? throw new ArgumentNullException(nameof(syncService));
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
                list.Add(key);
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

        var sourceMap = replicaContext.ShardVersionVectors;
        lock (sourceMap)
        {
            var map = new ShardVersionVectorMap();
            if (sourceMap.TryGetValue(shardId, out var dvv))
            {
                map[shardId] = CloneDvv(dvv);
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

                map[shardKvp.Key] = CloneDvv(shardKvp.Value);
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
                map[shardKvp.Key] = CloneDvv(shardKvp.Value);
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
            foreach (var shardId in shardIds)
            {
                if (!string.IsNullOrWhiteSpace(shardId))
                {
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
                _ = GetShardSemaphore(key);
                result.Add(key);
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
        var clonedIncoming = CloneDvv(incomingDvv);
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
                _ = GetShardSemaphore(kvp.Key);
                sourceMap[kvp.Key] = CloneDvv(kvp.Value);
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
                        dvv.Versions[versionKvp.Key] = versionKvp.Value;
                    }

                    dvv.Dots.Clear();
                    if (cleanedDvv.Dots != null)
                    {
                        foreach (var dotKvp in cleanedDvv.Dots)
                        {
                            dvv.Dots[dotKvp.Key] = new HashSet<long>(dotKvp.Value);
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
        return shardLocks.GetOrAdd(shardId, _ => new SemaphoreSlim(1, 1));
    }

    private void MergeDvvIntoShard(string shardId, DottedVersionVector incomingDvv)
    {
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
                dvv.Merge(incomingDvv);
            }
        }
    }

    private static DottedVersionVector CloneDvv(DottedVersionVector dvv)
    {
        var copiedVersions = new Dictionary<string, long>();
        var copiedDots = new Dictionary<string, ISet<long>>();

        lock (dvv)
        {
            foreach (var kvp in dvv.Versions)
            {
                copiedVersions[kvp.Key] = kvp.Value;
            }

            if (dvv.Dots != null)
            {
                foreach (var kvp in dvv.Dots)
                {
                    copiedDots[kvp.Key] = new HashSet<long>(kvp.Value);
                }
            }
        }

        return new DottedVersionVector(copiedVersions, copiedDots);
    }
}