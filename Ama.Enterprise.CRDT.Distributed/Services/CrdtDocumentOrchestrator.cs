namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

/// <summary>
/// Centralized generic orchestrator managing global active P2P CRDT document bindings.
/// </summary>
public sealed class CrdtDocumentOrchestrator : ICrdtDocumentOrchestrator, IDisposable
{
    private readonly IServiceProvider serviceProvider;
    private readonly IDistributedCrdtStorage storage;
    private readonly IAsyncCrdtPatcher patcher;
    private readonly ILogger<CrdtDocumentOrchestrator> logger;
    private readonly ConcurrentDictionary<string, IDistributedCrdtDocument> activeDocuments = new(StringComparer.Ordinal);
    
    // Completely replaces lock blockages allowing multi-threaded external awaiting bounds
    private readonly Channel<PooledOrchestratorCommand> commandChannel;
    private readonly ConcurrentQueue<PooledOrchestratorCommand> commandPool = new();
    private readonly CancellationTokenSource disposeCts = new();
    private readonly Task processingTask;

    private readonly Meter meter;
    private readonly Counter<long> documentCreatedCounter;
    private readonly Counter<long> documentDeletedCounter;
    private readonly Counter<long> antiEntropySyncCounter;
    private readonly Counter<long> snapshotsDispatchedCounter;
    private readonly Counter<long> snapshotBytesDispatchedCounter;
    private readonly Counter<long> broadcastBytesCounter;
    
    // Channel metrics
    private readonly Counter<long> commandsEnqueuedCounter;
    private readonly Counter<long> commandsProcessedCounter;
    private readonly Counter<long> commandFailuresCounter;
    private readonly ObservableGauge<int> channelQueueLengthGauge;
    private readonly ObservableGauge<int> commandPoolSizeGauge;
    
    public IDistributedCrdtDocument<CrdtRegistryState> Registry { get; private set; } = null!;

    public event EventHandler? DocumentsChanged;

    public CrdtDocumentOrchestrator(
        IServiceProvider serviceProvider,
        IDistributedCrdtStorage storage,
        IAsyncCrdtPatcher patcher,
        ILogger<CrdtDocumentOrchestrator> logger,
        IMeterFactory? meterFactory = null)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.commandChannel = Channel.CreateUnbounded<PooledOrchestratorCommand>(new UnboundedChannelOptions 
        { 
            SingleReader = true, 
            SingleWriter = false 
        });

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtDocumentOrchestrator") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtDocumentOrchestrator");
        this.documentCreatedCounter = this.meter.CreateCounter<long>("crdt.orchestrator.documents_created", "documents", "Total CRDT documents mapped dynamically");
        this.documentDeletedCounter = this.meter.CreateCounter<long>("crdt.orchestrator.documents_deleted", "documents", "Total CRDT documents permanently tombstoned");
        this.antiEntropySyncCounter = this.meter.CreateCounter<long>("crdt.orchestrator.anti_entropy_syncs", "syncs", "Total anti-entropy point-to-point synchronizations dispatched");
        this.snapshotsDispatchedCounter = this.meter.CreateCounter<long>("crdt.orchestrator.snapshots_dispatched", "snapshots", "Total full state snapshots explicitly provided responding to log gaps");
        this.snapshotBytesDispatchedCounter = this.meter.CreateCounter<long>("crdt.orchestrator.snapshot_bytes_dispatched", "bytes", "Total bytes dispatched for full state snapshot fallbacks");
        this.broadcastBytesCounter = this.meter.CreateCounter<long>("crdt.orchestrator.broadcast_bytes", "bytes", "Total bytes broadcasted across real-time patch syncs");

        this.commandsEnqueuedCounter = this.meter.CreateCounter<long>("crdt.orchestrator.channel.commands_enqueued", "commands", "Total orchestrator commands enqueued to the lock-free channel");
        this.commandsProcessedCounter = this.meter.CreateCounter<long>("crdt.orchestrator.channel.commands_processed", "commands", "Total orchestrator commands processed by the channel");
        this.commandFailuresCounter = this.meter.CreateCounter<long>("crdt.orchestrator.channel.command_failures", "errors", "Total orchestrator command processing failures");
        this.channelQueueLengthGauge = this.meter.CreateObservableGauge("crdt.orchestrator.channel.queue_length", () => this.commandChannel.Reader.CanCount ? this.commandChannel.Reader.Count : 0, "commands", "Current number of pending orchestrator commands");
        this.commandPoolSizeGauge = this.meter.CreateObservableGauge("crdt.orchestrator.channel.pool_size", () => this.commandPool.Count, "commands", "Current size of the orchestrator command object pool");

        this.processingTask = Task.Run(ProcessChannelAsync);
    }

    private PooledOrchestratorCommand GetCommand() => commandPool.TryDequeue(out var cmd) ? cmd : new PooledOrchestratorCommand();

    private void ReturnCommand(PooledOrchestratorCommand cmd)
    {
        cmd.Reset();
        commandPool.Enqueue(cmd);
    }

    private async Task ProcessChannelAsync()
    {
        try
        {
            await foreach (var cmd in commandChannel.Reader.ReadAllAsync(disposeCts.Token).ConfigureAwait(false))
            {
                try
                {
                    cmd.CancellationToken.ThrowIfCancellationRequested();

                    switch (cmd.Type)
                    {
                        case OrchestratorCommandType.Initialize:
                            await ProcessInitializeInternalAsync(cmd.CancellationToken).ConfigureAwait(false);
                            break;
                        case OrchestratorCommandType.SyncDocuments:
                            await ProcessSyncDocumentsInternalAsync(cmd.CancellationToken).ConfigureAwait(false);
                            break;
                        case OrchestratorCommandType.CreateDocument:
                            await ProcessCreateDocumentInternalAsync(cmd).ConfigureAwait(false);
                            break;
                        case OrchestratorCommandType.DeleteDocument:
                            await ProcessDeleteDocumentInternalAsync(cmd).ConfigureAwait(false);
                            break;
                    }

                    cmd.SetResult();
                    commandsProcessedCounter.Add(1);
                }
                catch (OperationCanceledException)
                {
                    cmd.SetException(new OperationCanceledException("Orchestrator operation cancelled."));
                }
                catch (Exception ex)
                {
                    commandFailuresCounter.Add(1);
                    cmd.SetException(ex);
                }
            }
        }
        catch (OperationCanceledException) { /* Clean graceful termination */ }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Distributed CRDT Orchestrator lock-free processing loop critically faulted.");
        }
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var cmd = GetCommand();
        cmd.Type = OrchestratorCommandType.Initialize;
        cmd.CancellationToken = cancellationToken;

        commandChannel.Writer.TryWrite(cmd);
        commandsEnqueuedCounter.Add(1);
        try
        {
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    private async Task ProcessInitializeInternalAsync(CancellationToken cancellationToken)
    {
        var registryState = new CrdtRegistryState();
        Registry = ActivatorUtilities.CreateInstance<DistributedCrdtDocument<CrdtRegistryState>>(serviceProvider, registryState);
        
        await Registry.InitializeAsync(cancellationToken).ConfigureAwait(false);
        
        Registry.StateChanged += OnRegistryStateChanged;
        Registry.PatchGenerated += OnDocumentPatchGenerated;
        
        await ProcessSyncDocumentsInternalAsync(cancellationToken).ConfigureAwait(false);
    }

    private void OnRegistryStateChanged(object? sender, EventArgs e)
    {
        _ = SyncDocumentsAsync(CancellationToken.None);
    }

    private void OnDocumentPatchGenerated(object? sender, CrdtPatch patch)
    {
        if (sender is IDistributedCrdtDocument doc)
        {
            // Bypasses the channel using Task.Run ensuring instant execution and preventing blocking the single-reader document loop.
            _ = Task.Run(() => BroadcastPatchAsync(doc.DocumentId, patch, CancellationToken.None));
        }
    }

    public async Task SyncDocumentsAsync(CancellationToken cancellationToken = default)
    {
        var cmd = GetCommand();
        cmd.Type = OrchestratorCommandType.SyncDocuments;
        cmd.CancellationToken = cancellationToken;

        commandChannel.Writer.TryWrite(cmd);
        commandsEnqueuedCounter.Add(1);
        try
        {
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    private async Task ProcessSyncDocumentsInternalAsync(CancellationToken cancellationToken)
    {
        try
        {
            var registryMap = Registry.Document.Data.Documents;
            bool changed = false;

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
                            doc.PatchGenerated += OnDocumentPatchGenerated;
                            changed = true;
                            documentCreatedCounter.Add(1, new KeyValuePair<string, object?>("document_id", kvp.Key));
                            logger.LogInformation("Orchestrator dynamically mapped new CRDT document: {DocumentId}", kvp.Key);
                        }
                    }
                    else
                    {
                        logger.LogWarning("Missing AOT explicit type factory for CRDT alias: {TypeAlias}", kvp.Value.TypeAlias);
                    }
                }
            }

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
                    doc.PatchGenerated -= OnDocumentPatchGenerated;
                    if (doc is IDisposable d) d.Dispose();
                    await storage.DeleteDocumentAsync(docId, cancellationToken).ConfigureAwait(false);
                    changed = true;
                    documentDeletedCounter.Add(1, new KeyValuePair<string, object?>("document_id", docId));
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
            throw;
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
        var cmd = GetCommand();
        cmd.Type = OrchestratorCommandType.CreateDocument;
        cmd.DocumentId = documentId;
        cmd.TypeAlias = typeAlias;
        cmd.CancellationToken = cancellationToken;

        commandChannel.Writer.TryWrite(cmd);
        commandsEnqueuedCounter.Add(1);
        try
        {
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    private async Task ProcessCreateDocumentInternalAsync(PooledOrchestratorCommand cmd)
    {
        var intent = new MapSetIntent(cmd.DocumentId!, new CrdtRegistryEntry(cmd.DocumentId!, cmd.TypeAlias!, false));
        var operation = await patcher.GenerateOperationAsync(Registry.Document, x => x.Documents, intent, cmd.CancellationToken).ConfigureAwait(false);
        var patch = new CrdtPatch(new[] { operation });
        
        await Registry.ApplyPatchAsync(patch, cmd.CancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteDocumentAsync(string documentId, CancellationToken cancellationToken = default)
    {
        var cmd = GetCommand();
        cmd.Type = OrchestratorCommandType.DeleteDocument;
        cmd.DocumentId = documentId;
        cmd.CancellationToken = cancellationToken;

        commandChannel.Writer.TryWrite(cmd);
        commandsEnqueuedCounter.Add(1);
        try
        {
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    private async Task ProcessDeleteDocumentInternalAsync(PooledOrchestratorCommand cmd)
    {
        if (Registry.Document.Data.Documents.TryGetValue(cmd.DocumentId!, out var existing))
        {
            var intent = new MapSetIntent(cmd.DocumentId!, existing with { IsDeleted = true });
            var operation = await patcher.GenerateOperationAsync(Registry.Document, x => x.Documents, intent, cmd.CancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });
            
            await Registry.ApplyPatchAsync(patch, cmd.CancellationToken).ConfigureAwait(false);
        }
    }

    public async Task ProvideSnapshotAsync(string documentId, string targetReplicaId, PeerId targetPeerId, CancellationToken cancellationToken = default)
    {
        var topologyProvider = serviceProvider.GetRequiredService<IScopeTopologyProvider>();
        if (!topologyProvider.IsPeerExpected(targetPeerId.Value.ToString()))
        {
            logger.LogWarning("Rejected snapshot request for document {DocumentId} from explicitly excluded peer {PeerId}.", documentId, targetPeerId.Value);
            return;
        }

        if (!activeDocuments.TryGetValue(documentId, out var doc) && Registry.DocumentId != documentId)
        {
            logger.LogWarning("Requested snapshot for unknown document {DocumentId}.", documentId);
            return;
        }

        var targetDoc = documentId == Registry.DocumentId ? Registry : doc;

        try
        {
            var snapshotResult = await targetDoc!.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
            
            var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();

            var resMsg = new CrdtSnapshotMessage(replicaContext.ReplicaId, snapshotResult.SnapshotData, snapshotResult.GlobalState);
            var payload = serializer.SerializeToBytes(resMsg);

            var wrapper = new CrdtMessageWrapper(documentId, "CrdtSnapshot", payload);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            await directSender.SendDirectAsync(targetPeerId, finalBytes, cancellationToken).ConfigureAwait(false); 
            
            snapshotsDispatchedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
            snapshotBytesDispatchedCounter.Add(finalBytes.Length, new KeyValuePair<string, object?>("document_id", documentId));
            logger.LogInformation("Dispatched targeted complete document snapshot fallback payload for document {DocumentId} to expected peer {PeerId}.", documentId, targetPeerId.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to dispatch snapshot fallback payload for document {DocumentId}.", documentId);
        }
    }

    public async Task BroadcastPatchAsync(string documentId, CrdtPatch patch, CancellationToken cancellationToken = default)
    {
        try
        {
            var p2pProtocol = serviceProvider.GetRequiredService<IP2pAlgorithm>();
            var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();

            var patchMsg = new CrdtPatchMessage(replicaContext.ReplicaId, patch);
            var payload = serializer.SerializeToBytes(patchMsg);
            
            var wrapper = new CrdtMessageWrapper(documentId, "CrdtPatch", payload);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            broadcastBytesCounter.Add(finalBytes.Length, new KeyValuePair<string, object?>("document_id", documentId));

            await p2pProtocol.BroadcastAsync(finalBytes, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to broadcast active sync patch for document {DocumentId}.", documentId);
        }
    }

    public async Task DispatchAntiEntropyStateAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var topologyProvider = serviceProvider.GetRequiredService<IScopeTopologyProvider>();
            var peerRegistry = serviceProvider.GetRequiredService<IPeerRegistry>();
            var meshes = serviceProvider.GetServices<P2pMeshMetadata>();
            
            var validPeers = new List<PeerId>();
            foreach (var mesh in meshes)
            {
                var activePeers = await peerRegistry.GetPeersByStatusAsync(mesh.MeshId, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
                foreach (var peer in activePeers)
                {
                    var networkIdStr = peer.Id.Value.ToString();
                    if (topologyProvider.IsPeerExpected(networkIdStr))
                    {
                        validPeers.Add(peer.Id);
                    }
                }
            }

            if (validPeers.Count == 0)
            {
                return;
            }

            var targetPeerId = validPeers[Random.Shared.Next(validPeers.Count)];

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

            await directSender.SendDirectAsync(targetPeerId, finalBytes, cancellationToken).ConfigureAwait(false);
            
            antiEntropySyncCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaContext.ReplicaId));
            logger.LogTrace("Dispatched targeted global DVV cluster state sync to expected peer {PeerId}.", targetPeerId.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to dispatch point-to-point cluster state sync.");
        }
    }

    public void Dispose()
    {
        disposeCts.Cancel();
        commandChannel.Writer.TryComplete();
        disposeCts.Dispose();

        if (Registry != null)
        {
            Registry.StateChanged -= OnRegistryStateChanged;
            Registry.PatchGenerated -= OnDocumentPatchGenerated;
            if (Registry is IDisposable rd) rd.Dispose();
        }
        
        foreach (var doc in activeDocuments.Values)
        {
            doc.PatchGenerated -= OnDocumentPatchGenerated;
            if (doc is IDisposable d) d.Dispose();
        }
        
        activeDocuments.Clear();
        meter.Dispose();
    }
}