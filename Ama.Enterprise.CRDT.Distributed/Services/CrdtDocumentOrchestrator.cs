namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

/// <summary>
/// Centralized generic orchestrator managing global active P2P CRDT document bindings.
/// </summary>
public sealed class CrdtDocumentOrchestrator(
    IServiceProvider serviceProvider,
    IDistributedCrdtStorage storage,
    ICrdtPatcher patcher,
    ILogger<CrdtDocumentOrchestrator> logger) : ICrdtDocumentOrchestrator, IDisposable
{
    private readonly IServiceProvider serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    private readonly IDistributedCrdtStorage storage = storage ?? throw new ArgumentNullException(nameof(storage));
    private readonly ICrdtPatcher patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));
    private readonly ILogger<CrdtDocumentOrchestrator> logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private readonly ConcurrentDictionary<string, IDistributedCrdtDocument> activeDocuments = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim syncLock = new(1, 1);
    
    public IDistributedCrdtDocument<CrdtRegistryState> Registry { get; private set; } = null!;

    public event EventHandler? DocumentsChanged;

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var registryState = new CrdtRegistryState();
        Registry = ActivatorUtilities.CreateInstance<DistributedCrdtDocument<CrdtRegistryState>>(serviceProvider, registryState);
        
        await Registry.InitializeAsync(cancellationToken).ConfigureAwait(false);
        
        Registry.StateChanged += OnRegistryStateChanged;
        
        await SyncDocumentsAsync(cancellationToken).ConfigureAwait(false);
    }

    private void OnRegistryStateChanged(object? sender, EventArgs e)
    {
        _ = SyncDocumentsAsync(CancellationToken.None);
    }

    public async Task SyncDocumentsAsync(CancellationToken cancellationToken = default)
    {
        await syncLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var registryMap = Registry.Document.Data.Documents;
            bool changed = false;

            // Handle additions accurately
            foreach (var kvp in registryMap)
            {
                if (!kvp.Value.IsDeleted && !activeDocuments.ContainsKey(kvp.Key))
                {
                    var factory = serviceProvider.GetKeyedService<IDocumentFactory>(kvp.Value.TypeAlias);
                    if (factory != null)
                    {
                        var doc = factory.CreateDocument(serviceProvider, kvp.Key);
                        await doc.InitializeAsync(cancellationToken).ConfigureAwait(false);
                        
                        if (activeDocuments.TryAdd(kvp.Key, doc))
                        {
                            changed = true;
                            logger.LogInformation("Orchestrator dynamically mapped new CRDT document: {DocumentId}", kvp.Key);
                        }
                    }
                    else
                    {
                        logger.LogWarning("Missing AOT explicit type factory for CRDT alias: {TypeAlias}", kvp.Value.TypeAlias);
                    }
                }
            }

            // Handle mathematically secure deletions tracking cluster tombstones smoothly
            var toRemove = new List<string>();
            foreach (var active in activeDocuments)
            {
                if (!registryMap.TryGetValue(active.Key, out var entry) || entry.IsDeleted)
                {
                    toRemove.Add(active.Key);
                }
            }

            foreach (var docId in toRemove)
            {
                if (activeDocuments.TryRemove(docId, out var doc))
                {
                    if (doc is IDisposable d) d.Dispose();
                    await storage.DeleteDocumentAsync(docId, cancellationToken).ConfigureAwait(false);
                    changed = true;
                    logger.LogInformation("Orchestrator tombstoned CRDT document: {DocumentId}", docId);
                }
            }

            if (changed)
            {
                DocumentsChanged?.Invoke(this, EventArgs.Empty);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An error occurred synchronizing dynamic P2P orchestrator matrices.");
        }
        finally
        {
            syncLock.Release();
        }
    }

    public IReadOnlyList<IDistributedCrdtDocument> GetActiveDocuments()
    {
        var docs = new List<IDistributedCrdtDocument>(activeDocuments.Count + 1) { Registry };
        docs.AddRange(activeDocuments.Values);
        return docs;
    }

    public IDistributedCrdtDocument<TState>? GetDocument<TState>(string documentId) where TState : class, new()
    {
        if (activeDocuments.TryGetValue(documentId, out var doc) && doc is IDistributedCrdtDocument<TState> typedDoc)
        {
            return typedDoc;
        }
        return null;
    }

    public async Task CreateDocumentAsync(string documentId, string typeAlias, CancellationToken cancellationToken = default)
    {
        var intent = new MapSetIntent(documentId, new CrdtRegistryEntry(documentId, typeAlias, false));
        var operation = patcher.GenerateOperation(Registry.Document, x => x.Documents, intent);
        var patch = new CrdtPatch(new[] { operation });
        
        await Registry.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteDocumentAsync(string documentId, CancellationToken cancellationToken = default)
    {
        if (Registry.Document.Data.Documents.TryGetValue(documentId, out var existing))
        {
            var intent = new MapSetIntent(documentId, existing with { IsDeleted = true });
            var operation = patcher.GenerateOperation(Registry.Document, x => x.Documents, intent);
            var patch = new CrdtPatch(new[] { operation });
            
            await Registry.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task DispatchAntiEntropyStateAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();

            DottedVersionVector globalState;
            lock (replicaContext.GlobalVersionVector)
            {
                globalState = replicaContext.GlobalVersionVector.DeepClone();
            }

            var syncMsg = new CrdtStateSyncMessage(replicaContext.ReplicaId, globalState);
            var payload = serializer.SerializeToBytes(syncMsg);
            
            var wrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", payload);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            await directSender.SendToRandomPeerAsync(finalBytes, cancellationToken).ConfigureAwait(false);
            
            logger.LogTrace("Dispatched targeted global DVV cluster state sync.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to dispatch point-to-point cluster state sync.");
        }
    }

    public void Dispose()
    {
        if (Registry != null)
        {
            Registry.StateChanged -= OnRegistryStateChanged;
            if (Registry is IDisposable rd) rd.Dispose();
        }
        
        foreach (var doc in activeDocuments.Values)
        {
            if (doc is IDisposable d) d.Dispose();
        }
        
        activeDocuments.Clear();
        syncLock.Dispose();
    }
}