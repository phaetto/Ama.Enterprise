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
/// </summary>
public sealed class MemoryCrdtStorage : IDistributedCrdtStorage, IDisposable
{
    private readonly List<JournaledOperation> operations = new();
    private readonly Dictionary<string, ClusterStateSnapshotDto> clusterStates = new();
    private readonly Dictionary<string, DottedVersionVector> globalVersionVectors = new();
    private readonly Dictionary<string, object> documents = new();
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
    public void Append(string documentId, IReadOnlyList<CrdtOperation> operationsList)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        ArgumentNullException.ThrowIfNull(operationsList);

        lock (syncRoot)
        {
            foreach (var op in operationsList)
            {
                if (!operations.Any(o => o.Operation.Id == op.Id))
                {
                    operations.Add(new JournaledOperation(documentId, op));
                }
            }
        }
        
        appendedOperationsCounter.Add(operationsList.Count, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public Task AppendAsync(string documentId, IReadOnlyList<CrdtOperation> operationsList, CancellationToken cancellationToken = default)
    {
        Append(documentId, operationsList);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string originReplicaId, long minGlobalClock, long maxGlobalClock, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(originReplicaId)) throw new ArgumentException("Origin Replica ID cannot be null or empty.", nameof(originReplicaId));

        List<JournaledOperation> snapshot;
        lock (syncRoot) 
        { 
            snapshot = operations.ToList(); 
        }

        foreach (var op in snapshot.Where(o => o.Operation.ReplicaId == originReplicaId && o.Operation.GlobalClock > minGlobalClock && o.Operation.GlobalClock <= maxGlobalClock))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return op;
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string originReplicaId, IEnumerable<long> globalClocks, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(originReplicaId)) throw new ArgumentException("Origin Replica ID cannot be null or empty.", nameof(originReplicaId));
        ArgumentNullException.ThrowIfNull(globalClocks);

        var clocks = globalClocks.ToHashSet();
        List<JournaledOperation> snapshot;
        lock (syncRoot) 
        { 
            snapshot = operations.ToList(); 
        }

        foreach (var op in snapshot.Where(o => o.Operation.ReplicaId == originReplicaId && clocks.Contains(o.Operation.GlobalClock)))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return op;
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<long> GetJournalCountAsync(CancellationToken cancellationToken = default)
    {
        lock (syncRoot)
        {
            return Task.FromResult((long)operations.Count);
        }
    }

    /// <inheritdoc />
    public void Trim(IReadOnlyDictionary<string, long> gmvv)
    {
        ArgumentNullException.ThrowIfNull(gmvv);

        lock (syncRoot)
        {
            operations.RemoveAll(op => 
                !gmvv.TryGetValue(op.Operation.ReplicaId, out var minKnown) || 
                op.Operation.GlobalClock <= minKnown);
        }
        
        trimsExecutedCounter.Add(1);
    }

    /// <inheritdoc />
    public Task TrimAsync(IReadOnlyDictionary<string, long> globalMinimumVersionVector, CancellationToken cancellationToken = default)
    {
        Trim(globalMinimumVersionVector);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<JournaledOperation> GetAllJournaledOperationsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<JournaledOperation> snapshot;
        lock (syncRoot) 
        { 
            snapshot = operations.ToList(); 
        }

        foreach (var op in snapshot)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return op;
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<DottedVersionVector?> LoadGlobalVersionVectorAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        lock (syncRoot)
        {
            return Task.FromResult(globalVersionVectors.TryGetValue(replicaId, out var dvv) ? dvv : null);
        }
    }

    /// <inheritdoc />
    public Task SaveGlobalVersionVectorAsync(string replicaId, DottedVersionVector globalVersionVector, CancellationToken cancellationToken = default)
    {
        lock (syncRoot)
        {
            globalVersionVectors[replicaId] = globalVersionVector;
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
    public Task<CrdtDocument<TState>?> LoadDocumentAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        lock (syncRoot)
        {
            if (documents.TryGetValue(documentId, out var doc) && doc is CrdtDocument<TState> typedDoc)
            {
                return Task.FromResult<CrdtDocument<TState>?>(typedDoc);
            }
            return Task.FromResult<CrdtDocument<TState>?>(null);
        }
    }

    /// <inheritdoc />
    public Task SaveDocumentAsync<TState>(string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default) where TState : class, new()
    {
        lock (syncRoot)
        {
            documents[documentId] = document;
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task DeleteDocumentAsync(string documentId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return Task.CompletedTask;

        lock (syncRoot)
        {
            operations.RemoveAll(op => op.DocumentId == documentId);
            documents.Remove(documentId);
        }

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        meter.Dispose();
    }
}