namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Journaling;

/// <summary>
/// Forwards journaling operations from the core CRDT pipeline to the unified distributed storage.
/// Ensures the active IJournalManager utilizes the shared registered storage backend implicitly.
/// </summary>
internal sealed class StorageJournalForwarder : ICrdtOperationJournal, IDisposable
{
    private readonly IDistributedCrdtStorage storage;
    private readonly Meter meter;
    private readonly Counter<long> appendedOperationsCounter;

    public StorageJournalForwarder(IDistributedCrdtStorage storage, IMeterFactory? meterFactory = null)
    {
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        
        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.StorageJournalForwarder") ?? new Meter("Ama.Enterprise.CRDT.Distributed.StorageJournalForwarder");
        this.appendedOperationsCounter = this.meter.CreateCounter<long>("crdt.journal.forwarded_operations", "operations", "Total operations effectively forwarded towards explicit backend configurations naturally");
    }

    public void Append(string documentId, IReadOnlyList<CrdtOperation> operationsList)
    {
        storage.Append(documentId, operationsList);
        appendedOperationsCounter.Add(operationsList.Count, new KeyValuePair<string, object?>("document_id", documentId));
    }

    public async Task AppendAsync(string documentId, IReadOnlyList<CrdtOperation> operationsList, CancellationToken cancellationToken = default)
    {
        await storage.AppendAsync(documentId, operationsList, cancellationToken).ConfigureAwait(false);
        appendedOperationsCounter.Add(operationsList.Count, new KeyValuePair<string, object?>("document_id", documentId));
    }

    public IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string originReplicaId, long minGlobalClock, long maxGlobalClock, CancellationToken cancellationToken = default) 
        => storage.GetOperationsByRangeAsync(originReplicaId, minGlobalClock, maxGlobalClock, cancellationToken);

    public IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string originReplicaId, IEnumerable<long> globalClocks, CancellationToken cancellationToken = default) 
        => storage.GetOperationsByDotsAsync(originReplicaId, globalClocks, cancellationToken);

    public void Dispose()
    {
        meter.Dispose();
    }
}