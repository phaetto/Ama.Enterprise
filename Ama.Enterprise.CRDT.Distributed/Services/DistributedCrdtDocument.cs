namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Journaling;
using Ama.CRDT.Services.Providers;
using Ama.CRDT.Services.Serialization;
using Ama.CRDT.Services.Versioning;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Generic document manager responsible for maintaining consistency and routing P2P actions for a specific CRDT tree.
/// </summary>
public sealed class DistributedCrdtDocument<TState> : IDistributedCrdtDocument<TState>, IDisposable where TState : class, new()
{
    private readonly ReplicaContext replicaContext;
    private readonly IAsyncCrdtApplicator applicator;
    private readonly ICrdtMetadataManager metadataManager;
    private readonly IServiceProvider serviceProvider;
    private readonly ICrdtSerializer serializer;
    private readonly IDistributedCrdtStorage storage;
    private readonly ILogger<DistributedCrdtDocument<TState>> logger;
    private readonly bool activeSyncEnabled;
    private readonly TState initialState;
    
    // Fast synchronous lock for atomic reference/flag swapping
    private readonly object syncRoot = new();
    
    // Asynchronous lock guaranteeing strictly serialized patch/operation pipelines to prevent Lost Update anomalies
    private readonly SemaphoreSlim modificationLock = new(1, 1);
    
    private volatile bool isDirty;

    /// <inheritdoc />
    public string DocumentId { get; }

    /// <inheritdoc />
    public CrdtDocument<TState> Document { get; private set; }

    /// <inheritdoc />
    public event EventHandler? StateChanged;

    public DistributedCrdtDocument(
        TState initialState,
        ReplicaContext replicaContext,
        IAsyncCrdtApplicator applicator,
        ICrdtMetadataManager metadataManager,
        IOptions<DistributedCrdtOptions> options,
        IServiceProvider serviceProvider,
        ICrdtSerializer serializer,
        IDistributedCrdtStorage storage,
        ILogger<DistributedCrdtDocument<TState>> logger,
        IDocumentIdProvider documentIdProvider)
    {
        if (options == null) throw new ArgumentNullException(nameof(options));
        if (initialState == null) throw new ArgumentNullException(nameof(initialState));
        if (documentIdProvider == null) throw new ArgumentNullException(nameof(documentIdProvider));
        
        this.replicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));
        this.applicator = applicator ?? throw new ArgumentNullException(nameof(applicator));
        this.metadataManager = metadataManager ?? throw new ArgumentNullException(nameof(metadataManager));
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.activeSyncEnabled = options.Value.ActiveSyncEnabled;
        this.initialState = initialState;

        DocumentId = documentIdProvider.GetDocumentId(initialState);
        
        var metadata = metadataManager.Initialize(initialState);
        Document = new CrdtDocument<TState>(initialState, metadata);
    }

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var storedDoc = await storage.LoadDocumentAsync<TState>(DocumentId, cancellationToken).ConfigureAwait(false);
            if (storedDoc != null)
            {
                await modificationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    lock (syncRoot)
                    {
                        Document = storedDoc.Value;
                    }
                }
                finally
                {
                    modificationLock.Release();
                }

                StateChanged?.Invoke(this, EventArgs.Empty);
                logger.LogInformation("Successfully loaded initial state for document {DocumentId} from persistent storage.", DocumentId);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load initial state for document {DocumentId} from persistent storage.", DocumentId);
        }
    }

    /// <inheritdoc />
    public async Task ApplyPatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default)
    {
        await modificationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            CrdtDocument<TState> currentDoc;
            lock (syncRoot)
            {
                currentDoc = Document;
            }

            var result = await applicator.ApplyPatchAsync(currentDoc, patch).ConfigureAwait(false);

            lock (syncRoot)
            {
                Document = result.Document;
                isDirty = true;
            }
        }
        finally
        {
            modificationLock.Release();
        }

        StateChanged?.Invoke(this, EventArgs.Empty);

        if (activeSyncEnabled && patch.Operations != null)
        {
            foreach (var operation in patch.Operations)
            {
                await BroadcastOperationAsync(operation, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc />
    public DottedVersionVector GetLocalState()
    {
        var sourceDvv = replicaContext.GlobalVersionVector;

        // Lock to prevent cross-thread collection modification errors during serialization mappings
        lock (sourceDvv)
        {
            return sourceDvv.DeepClone();
        }
    }

    /// <inheritdoc />
    public async Task ApplyOperationsAsync(IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default)
    {
        if (operations == null) throw new ArgumentNullException(nameof(operations));
        if (operations.Count == 0) return;

        await modificationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            CrdtDocument<TState> currentDoc;
            lock (syncRoot)
            {
                currentDoc = Document;
            }

            async IAsyncEnumerable<JournaledOperation> GetStreamAsync()
            {
                foreach (var op in operations)
                {
                    yield return new JournaledOperation(DocumentId, op);
                }
                await Task.CompletedTask;
            }

            var result = await applicator.ApplyOperationsAsync(currentDoc, GetStreamAsync()).ConfigureAwait(false);

            lock (syncRoot)
            {
                Document = result.Document;
                isDirty = true;
            }
        }
        finally
        {
            modificationLock.Release();
        }

        StateChanged?.Invoke(this, EventArgs.Empty);
    }

    /// <inheritdoc />
    public async Task ProvideSnapshotAsync(string targetReplicaId, PeerId targetPeerId, CancellationToken cancellationToken = default)
    {
        try
        {
            CrdtDocument<TState> currentDoc;
            DottedVersionVector globalState;

            // Strict Pipeline lock ensures extraction of Document and DVV cannot be horizontally torn by concurrent active patches
            await modificationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                lock (syncRoot)
                {
                    currentDoc = Document;
                }
                
                globalState = GetLocalState(); 
            }
            finally
            {
                modificationLock.Release();
            }

            var snapshotData = serializer.SerializeToBytes(currentDoc);
            var resMsg = new CrdtSnapshotMessage(replicaContext.ReplicaId, snapshotData, globalState);
            var payload = serializer.SerializeToBytes(resMsg);

            var wrapper = new CrdtMessageWrapper(DocumentId, "CrdtSnapshot", payload);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            await directSender.SendDirectAsync(targetPeerId, finalBytes, cancellationToken).ConfigureAwait(false); 
            
            logger.LogInformation("Dispatched targeted complete document snapshot fallback payload for document {DocumentId} to peer {PeerId}.", DocumentId, targetPeerId.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to dispatch snapshot fallback payload for document {DocumentId}.", DocumentId);
        }
    }

    /// <inheritdoc />
    public async Task MergeSnapshotAsync(byte[] snapshotData, DottedVersionVector globalState, CancellationToken cancellationToken = default)
    {
        if (snapshotData == null || snapshotData.Length == 0) return;

        try
        {
            var snapshotDoc = serializer.DeserializeFromBytes<CrdtDocument<TState>>(snapshotData);

            await modificationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                lock (syncRoot)
                {
                    Document = snapshotDoc;
                    isDirty = true;
                }

                // Crucial alignment: Overwrite tracked encompassing P2P tracking vectors matching the provider.
                lock (replicaContext.GlobalVersionVector)
                {
                    replicaContext.GlobalVersionVector.Merge(globalState);
                }
            }
            finally
            {
                modificationLock.Release();
            }

            StateChanged?.Invoke(this, EventArgs.Empty);
            logger.LogInformation("Successfully merged global state snapshot for document {DocumentId}.", DocumentId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process snapshot merger for document {DocumentId}.", DocumentId);
        }
    }

    /// <inheritdoc />
    public async Task CheckpointAsync(CancellationToken cancellationToken = default)
    {
        if (!isDirty)
        {
            return;
        }

        CrdtDocument<TState> currentDoc;

        lock (syncRoot)
        {
            currentDoc = Document;
            // Acknowledge the dirty state prior to asynchronous I/O to prevent 
            // concurrent writes during saving from being ignored.
            isDirty = false; 
        }

        try
        {
            // Intentionally bubble exceptions so orchestrator aborts overarching global log modifications avoiding write ahead gaps
            await storage.SaveDocumentAsync(DocumentId, currentDoc, cancellationToken).ConfigureAwait(false);
            logger.LogDebug("Successfully saved checkpoint to persistent storage for document {DocumentId}.", DocumentId);
        }
        catch (Exception)
        {
            lock (syncRoot)
            {
                // Revert flag on failure so the orchestrator attempts mapping it again on the next loop
                isDirty = true;
            }
            throw;
        }
    }

    /// <inheritdoc />
    public async Task EvictReplicaAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));

        await modificationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            lock (syncRoot)
            {
                metadataManager.EvictReplica(Document, replicaId);
                isDirty = true;
            }
        }
        finally
        {
            modificationLock.Release();
        }

        StateChanged?.Invoke(this, EventArgs.Empty);
    }

    /// <inheritdoc />
    public async Task ResetLocalStateAsync(string oldReplicaId, CancellationToken cancellationToken = default)
    {
        await modificationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            lock (syncRoot)
            {
                var metadata = metadataManager.Initialize(initialState);
                Document = new CrdtDocument<TState>(initialState, metadata);
                isDirty = true;
            }
        }
        finally
        {
            modificationLock.Release();
        }

        StateChanged?.Invoke(this, EventArgs.Empty);
        logger.LogInformation("Successfully performed a hard reset following identity re-bootstrap for document {DocumentId}.", DocumentId);
    }

    private async Task BroadcastOperationAsync(CrdtOperation operation, CancellationToken cancellationToken)
    {
        try
        {
            var p2pProtocol = serviceProvider.GetRequiredService<IP2pProtocol>();
            var opsMsg = new CrdtOperationsMessage(replicaContext.ReplicaId, new[] { operation });
            var payload = serializer.SerializeToBytes(opsMsg);
            
            var wrapper = new CrdtMessageWrapper(DocumentId, "CrdtOps", payload);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            await p2pProtocol.BroadcastAsync(finalBytes, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to broadcast active sync operation for document {DocumentId}.", DocumentId);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        modificationLock.Dispose();
    }
}