namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Journaling;

/// <summary>
/// Thread-safe in-memory journal for CRDT operations.
/// </summary>
public sealed class MemoryJournal : ICrdtOperationJournal
{
    private readonly List<JournaledOperation> operations = new();
    private readonly object syncRoot = new();

    /// <inheritdoc />
    public void Append(string documentId, IReadOnlyList<CrdtOperation> operationsList)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        if (operationsList == null) throw new ArgumentNullException(nameof(operationsList));

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
        if (globalClocks == null) throw new ArgumentNullException(nameof(globalClocks));

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

    /// <summary>
    /// Trims operations that are strictly older than the global minimum version vector.
    /// </summary>
    public void Trim(IReadOnlyDictionary<string, long> gmvv)
    {
        if (gmvv == null) throw new ArgumentNullException(nameof(gmvv));

        lock (syncRoot)
        {
            operations.RemoveAll(op => 
                gmvv.TryGetValue(op.Operation.ReplicaId, out var minKnown) && 
                op.Operation.GlobalClock <= minKnown);
        }
    }
}