namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Thread-safe in-memory unified storage and journal for CRDT operations.
/// Provides a default ephemeral implementation for systems not requiring persistent storage.
/// Internally segregates data structures evaluating strictly isolated Shard definitions mapping dictionaries to List bounds dynamically tracking topologies cleanly.
/// </summary>
public sealed class MemoryCrdtStorage : IDistributedCrdtStorage, IDisposable
{
    private readonly Dictionary<string, List<JournaledOperation>> shardedOperations = new();
    private readonly Dictionary<string, HashSet<Guid>> shardedOperationIds = new();
    private readonly Dictionary<string, ClusterStateSnapshotDto> clusterStates = new();
    private readonly Dictionary<string, ShardVersionVectorMap> globalVersionVectors = new();
    private readonly Dictionary<string, object> documents = new();
    private readonly Dictionary<string, byte[]> serializedDocuments = new();
    private readonly object syncRoot = new();

    private readonly Meter meter;
    private readonly Counter<long> appendedOperationsCounter;
    private readonly Counter<long> trimsExecutedCounter;

    public MemoryCrdtStorage(IMeterFactory? meterFactory = null)
    {
        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.MemoryCrdtStorage") ?? new Meter("Ama.Enterprise.CRDT.Distributed.MemoryCrdtStorage");
        this.appendedOperationsCounter = this.meter.CreateCounter<long>("crdt.storage.memory.operations_appended", "operations", "Total operations saved natively into ephemeral memory structures");
        this.trimsExecutedCounter = this.meter.CreateCounter<long>("crdt.storage.memory.trims_executed", "trims", "Total journal trim collections evaluated");
    }

    /// <inheritdoc />
    public void Append(IComparable documentId, IReadOnlyList<CrdtOperation> operationsList)
    {
        if (documentId is null) throw new ArgumentNullException(nameof(documentId));
        ArgumentNullException.ThrowIfNull(operationsList);

        lock (syncRoot)
        {
            foreach (var op in operationsList)
            {
                var shardId = op.ShardId ?? "";
                if (!shardedOperations.TryGetValue(shardId, out var shardList))
                {
                    shardList = new List<JournaledOperation>();
                    shardedOperations[shardId] = shardList;
                }

                if (!shardedOperationIds.TryGetValue(shardId, out var shardOpIds))
                {
                    shardOpIds = new HashSet<Guid>();
                    shardedOperationIds[shardId] = shardOpIds;
                }

                if (shardOpIds.Add(op.Id))
                {
                    shardList.Add(new JournaledOperation(documentId, op));
                }
            }
        }
        
        appendedOperationsCounter.Add(operationsList.Count, new KeyValuePair<string, object?>("document_id", documentId.ToString()));
    }

    /// <inheritdoc />
    public Task AppendAsync(IComparable documentId, IReadOnlyList<CrdtOperation> operationsList, CancellationToken cancellationToken = default)
    {
        Append(documentId, operationsList);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string shardId, string originReplicaId, long minShardClock, long maxShardClock, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(shardId)) throw new ArgumentException("Shard ID cannot be null or empty.", nameof(shardId));
        if (string.IsNullOrWhiteSpace(originReplicaId)) throw new ArgumentException("Origin Replica ID cannot be null or empty.", nameof(originReplicaId));

        List<JournaledOperation> snapshot;
        lock (syncRoot) 
        { 
            if (shardedOperations.TryGetValue(shardId, out var shardList))
            {
                snapshot = shardList.ToList();
            }
            else
            {
                snapshot = new List<JournaledOperation>();
            }
        }

        foreach (var op in snapshot.Where(o => o.Operation.ReplicaId == originReplicaId && o.Operation.ShardClock > minShardClock && o.Operation.ShardClock <= maxShardClock))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return op;
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string shardId, string originReplicaId, IEnumerable<long> shardClocks, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(shardId)) throw new ArgumentException("Shard ID cannot be null or empty.", nameof(shardId));
        if (string.IsNullOrWhiteSpace(originReplicaId)) throw new ArgumentException("Origin Replica ID cannot be null or empty.", nameof(originReplicaId));
        ArgumentNullException.ThrowIfNull(shardClocks);

        var clockSet = shardClocks.ToHashSet();
        List<JournaledOperation> snapshot;
        lock (syncRoot) 
        { 
            if (shardedOperations.TryGetValue(shardId, out var shardList))
            {
                snapshot = shardList.ToList();
            }
            else
            {
                snapshot = new List<JournaledOperation>();
            }
        }

        foreach (var op in snapshot.Where(o => o.Operation.ReplicaId == originReplicaId && clockSet.Contains(o.Operation.ShardClock)))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return op;
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<long> GetJournalCountAsync(string shardId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(shardId);
        
        lock (syncRoot)
        {
            return Task.FromResult(shardedOperations.TryGetValue(shardId, out var shardList) ? (long)shardList.Count : 0L);
        }
    }

    /// <inheritdoc />
    public Task TrimAsync(string shardId, IReadOnlyDictionary<string, long> globalMinimumVersionVector, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(shardId);
        ArgumentNullException.ThrowIfNull(globalMinimumVersionVector);

        if (globalMinimumVersionVector.Count == 0)
        {
            trimsExecutedCounter.Add(1);
            return Task.CompletedTask;
        }

        lock (syncRoot)
        {
            if (shardedOperations.TryGetValue(shardId, out var shardList))
            {
                shardedOperationIds.TryGetValue(shardId, out var shardOpIds);

                shardList.RemoveAll(op =>
                {
                    if (globalMinimumVersionVector.TryGetValue(op.Operation.ReplicaId, out var maxClock) &&
                        op.Operation.ShardClock <= maxClock)
                    {
                        shardOpIds?.Remove(op.Operation.Id);
                        return true;
                    }

                    return false;
                });
                
                if (shardList.Count == 0)
                {
                    shardedOperations.Remove(shardId);
                    shardedOperationIds.Remove(shardId);
                }
            }
        }
        
        trimsExecutedCounter.Add(1);
        
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<JournaledOperation> GetAllJournaledOperationsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<JournaledOperation> snapshot;
        lock (syncRoot) 
        { 
            snapshot = shardedOperations.Values.SelectMany(x => x).ToList(); 
        }

        foreach (var op in snapshot)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return op;
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<ShardVersionVectorMap?> LoadShardVersionVectorMapAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        lock (syncRoot)
        {
            return Task.FromResult(globalVersionVectors.TryGetValue(replicaId, out var dvv) ? dvv : null);
        }
    }

    /// <inheritdoc />
    public Task SaveShardVersionVectorMapAsync(string replicaId, ShardVersionVectorMap shardVersionVectorMap, CancellationToken cancellationToken = default)
    {
        lock (syncRoot)
        {
            globalVersionVectors[replicaId] = shardVersionVectorMap;
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<ClusterStateSnapshotDto?> LoadClusterStateAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        lock (syncRoot)
        {
            return Task.FromResult(clusterStates.TryGetValue(replicaId, out var state) ? state : null);
        }
    }

    /// <inheritdoc />
    public Task SaveClusterStateAsync(string replicaId, ClusterStateSnapshotDto state, CancellationToken cancellationToken = default)
    {
        lock (syncRoot)
        {
            clusterStates[replicaId] = state;
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<CrdtDocument<TState>?> LoadDocumentAsync<TState>(string shardId, string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        var key = $"{shardId}_{documentId}";
        lock (syncRoot)
        {
            if (documents.TryGetValue(key, out var doc) && doc is CrdtDocument<TState> typedDoc)
            {
                return Task.FromResult<CrdtDocument<TState>?>(typedDoc);
            }
            return Task.FromResult<CrdtDocument<TState>?>(null);
        }
    }

    /// <inheritdoc />
    public Task<CrdtDocument<TState>?> LoadOrphanedDocumentAsync<TState>(string shardId, string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        // In-memory storage is ephemeral and not shared across processes boundaries, 
        // thus partitioned orphaned states logic is not applicable in this context.
        return Task.FromResult<CrdtDocument<TState>?>(null);
    }

    /// <inheritdoc />
    public Task SaveDocumentAsync<TState>(string shardId, string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default) where TState : class, new()
    {
        var key = $"{shardId}_{documentId}";
        lock (syncRoot)
        {
            documents[key] = document;
            serializedDocuments.Remove(key);
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task SaveSerializedDocumentAsync<TState>(string shardId, string documentId, byte[] serializedDocument, CancellationToken cancellationToken = default) where TState : class, new()
    {
        ArgumentNullException.ThrowIfNull(serializedDocument);

        var key = $"{shardId}_{documentId}";
        lock (syncRoot)
        {
            serializedDocuments[key] = serializedDocument;
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task DeleteDocumentAsync(string shardId, string documentId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return Task.CompletedTask;

        var key = $"{shardId}_{documentId}";
        var rawDocId = ExtractRawDocumentId(documentId);
        lock (syncRoot)
        {
            if (!string.IsNullOrWhiteSpace(shardId))
            {
                if (shardedOperations.TryGetValue(shardId, out var shardList))
                {
                    shardedOperationIds.TryGetValue(shardId, out var shardOpIds);
                    shardList.RemoveAll(op =>
                    {
                        var opDocId = op.DocumentId?.ToString();
                        var match = string.Equals(opDocId, documentId, StringComparison.Ordinal) ||
                                    string.Equals(opDocId, rawDocId, StringComparison.Ordinal);
                        if (match)
                        {
                            shardOpIds?.Remove(op.Operation.Id);
                        }

                        return match;
                    });
                }
            }
            else
            {
                foreach (var kvp in shardedOperations)
                {
                    shardedOperationIds.TryGetValue(kvp.Key, out var shardOpIds);
                    kvp.Value.RemoveAll(op =>
                    {
                        var opDocId = op.DocumentId?.ToString();
                        var match = string.Equals(opDocId, documentId, StringComparison.Ordinal) ||
                                    string.Equals(opDocId, rawDocId, StringComparison.Ordinal);
                        if (match)
                        {
                            shardOpIds?.Remove(op.Operation.Id);
                        }

                        return match;
                    });
                }
            }

            documents.Remove(key);
            serializedDocuments.Remove(key);
        }

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        meter.Dispose();
    }

    private static string ExtractRawDocumentId(string documentId)
    {
        var colonIndex = documentId.IndexOf(':', StringComparison.Ordinal);
        return colonIndex >= 0 && colonIndex < documentId.Length - 1
            ? documentId[(colonIndex + 1)..]
            : documentId;
    }
}