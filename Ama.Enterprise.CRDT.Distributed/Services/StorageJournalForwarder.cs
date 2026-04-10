namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Journaling;

/// <summary>
/// Forwards journaling operations from the core CRDT pipeline to the unified distributed storage.
/// Ensures the active IJournalManager utilizes the shared registered storage backend implicitly.
/// </summary>
internal sealed class StorageJournalForwarder : ICrdtOperationJournal
{
    private readonly IDistributedCrdtStorage storage;

    public StorageJournalForwarder(IDistributedCrdtStorage storage)
    {
        this.storage = storage;
    }

    public void Append(string documentId, IReadOnlyList<CrdtOperation> operationsList) => storage.Append(documentId, operationsList);

    public Task AppendAsync(string documentId, IReadOnlyList<CrdtOperation> operationsList, CancellationToken cancellationToken = default) => storage.AppendAsync(documentId, operationsList, cancellationToken);

    public IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string originReplicaId, long minGlobalClock, long maxGlobalClock, CancellationToken cancellationToken = default) => storage.GetOperationsByRangeAsync(originReplicaId, minGlobalClock, maxGlobalClock, cancellationToken);

    public IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string originReplicaId, IEnumerable<long> globalClocks, CancellationToken cancellationToken = default) => storage.GetOperationsByDotsAsync(originReplicaId, globalClocks, cancellationToken);
}