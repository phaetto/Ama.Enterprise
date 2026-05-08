namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Composite router securely mapping multi-document P2P persistence architectures explicitly isolating distinct underlying database backends dynamically.
/// </summary>
public sealed class CompositeCrdtStorage : IDistributedCrdtStorage
{
    private readonly IServiceProvider serviceProvider;
    private readonly IDistributedCrdtStorage primaryStorage;
    private readonly List<IDistributedCrdtStorage> allStorages;
    private readonly Dictionary<string, IDistributedCrdtStorage> storagesByDocumentType = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IDistributedCrdtStorage> storagesByDocumentId = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IDistributedCrdtStorage> documentRoutes = new(StringComparer.Ordinal);
    private readonly object syncRoot = new();

    public CompositeCrdtStorage(IServiceProvider serviceProvider, IEnumerable<CrdtStorageRegistration> registrations)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        
        primaryStorage = serviceProvider.GetKeyedService<IDistributedCrdtStorage>("primary") 
            ?? throw new InvalidOperationException("No primary keyed CRDT storage is registered. A fallback primary architecture must structurally exist.");
            
        var uniqueStorages = new HashSet<IDistributedCrdtStorage> { primaryStorage };
        
        if (registrations != null)
        {
            foreach (var reg in registrations)
            {
                var storage = serviceProvider.GetKeyedService<IDistributedCrdtStorage>(reg.Key);
                if (storage != null)
                {
                    if (reg.RoutingType == CrdtStorageRoutingType.DocumentType)
                    {
                        storagesByDocumentType[reg.TargetValue] = storage;
                    }
                    else if (reg.RoutingType == CrdtStorageRoutingType.DocumentId)
                    {
                        storagesByDocumentId[reg.TargetValue] = storage;
                    }
                    uniqueStorages.Add(storage);
                }
            }
        }
        
        allStorages = uniqueStorages.ToList();
    }

    private IDistributedCrdtStorage GetStorage(string documentId)
    {
        lock (syncRoot)
        {
            if (documentRoutes.TryGetValue(documentId, out var storage))
            {
                return storage;
            }
        }

        var selectedStorage = primaryStorage;
        
        if (storagesByDocumentId.TryGetValue(documentId, out var documentStorage))
        {
            selectedStorage = documentStorage;
        }
        else
        {
            var orchestrator = serviceProvider.GetService<ICrdtDocumentOrchestrator>();
            if (orchestrator?.Registry?.Document.Data?.Documents != null && 
                orchestrator.Registry.Document.Data.Documents.TryGetValue(documentId, out var entry))
            {
                if (storagesByDocumentType.TryGetValue(entry.TypeAlias, out var typeStorage))
                {
                    selectedStorage = typeStorage;
                }
            }
        }

        lock (syncRoot)
        {
            documentRoutes[documentId] = selectedStorage;
        }

        return selectedStorage;
    }

    /// <inheritdoc />
    public Task<DottedVersionVector?> LoadGlobalVersionVectorAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        return primaryStorage.LoadGlobalVersionVectorAsync(replicaId, cancellationToken);
    }

    /// <inheritdoc />
    public Task SaveGlobalVersionVectorAsync(string replicaId, DottedVersionVector globalVersionVector, CancellationToken cancellationToken = default)
    {
        return primaryStorage.SaveGlobalVersionVectorAsync(replicaId, globalVersionVector, cancellationToken);
    }

    /// <inheritdoc />
    public Task<CrdtDocument<TState>?> LoadDocumentAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        return GetStorage(documentId).LoadDocumentAsync<TState>(documentId, cancellationToken);
    }

    /// <inheritdoc />
    public Task SaveDocumentAsync<TState>(string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default) where TState : class, new()
    {
        return GetStorage(documentId).SaveDocumentAsync(documentId, document, cancellationToken);
    }

    /// <inheritdoc />
    public Task DeleteDocumentAsync(string documentId, CancellationToken cancellationToken = default)
    {
        return GetStorage(documentId).DeleteDocumentAsync(documentId, cancellationToken);
    }

    /// <inheritdoc />
    public async Task TrimAsync(IReadOnlyDictionary<string, long> globalMinimumVersionVector, CancellationToken cancellationToken = default)
    {
        foreach (var storage in allStorages)
        {
            await storage.TrimAsync(globalMinimumVersionVector, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<JournaledOperation> GetAllJournaledOperationsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var storage in allStorages)
        {
            await foreach (var op in storage.GetAllJournaledOperationsAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return op;
            }
        }
    }

    /// <inheritdoc />
    public void Append(string documentId, IReadOnlyList<CrdtOperation> operationsList)
    {
        GetStorage(documentId).Append(documentId, operationsList);
    }

    /// <inheritdoc />
    public Task AppendAsync(string documentId, IReadOnlyList<CrdtOperation> operationsList, CancellationToken cancellationToken = default)
    {
        return GetStorage(documentId).AppendAsync(documentId, operationsList, cancellationToken);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string originReplicaId, long minGlobalClock, long maxGlobalClock, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var storage in allStorages)
        {
            await foreach (var op in storage.GetOperationsByRangeAsync(originReplicaId, minGlobalClock, maxGlobalClock, cancellationToken).ConfigureAwait(false))
            {
                yield return op;
            }
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string originReplicaId, IEnumerable<long> globalClocks, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var storage in allStorages)
        {
            await foreach (var op in storage.GetOperationsByDotsAsync(originReplicaId, globalClocks, cancellationToken).ConfigureAwait(false))
            {
                yield return op;
            }
        }
    }
}