namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Journaling;
using Ama.CRDT.Services.Serialization;
using Ama.CRDT.Services.Versioning;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Generic document manager responsible for maintaining consistency and routing P2P actions for a specific CRDT tree.
/// </summary>
public sealed class DistributedCrdtDocument<TState> : IDistributedCrdtDocument<TState>, IDisposable where TState : class, IDistributedCrdtState, new()
{
    private readonly ReplicaContext replicaContext;
    private readonly IAsyncCrdtApplicator applicator;
    private readonly IJournalManager journalManager;
    private readonly IVersionVectorSyncService syncService;
    private readonly IServiceProvider serviceProvider;
    private readonly ICrdtSerializer serializer;
    private readonly IDistributedCrdtStorage storage;
    private readonly ILogger<DistributedCrdtDocument<TState>> logger;
    private readonly bool activeSyncEnabled;
    
    // Fast synchronous lock for atomic reference/flag swapping
    private readonly object syncRoot = new();
    
    // Asynchronous lock guaranteeing strictly serialized patch/operation pipelines to completely prevent Lost Update anomalies
    private readonly SemaphoreSlim modificationLock = new(1, 1);
    
    private volatile bool isDirty;

    /// <inheritdoc />
    public string DocumentId { get; }

    /// <inheritdoc />
    public CrdtDocument<TState> Document { get; private set; }

    /// <inheritdoc />
    public event EventHandler? StateChanged;

    public DistributedCrdtDocument(
        ReplicaContext replicaContext,
        IAsyncCrdtApplicator applicator,
        IJournalManager journalManager,
        IVersionVectorSyncService syncService,
        ICrdtMetadataManager metadataManager,
        IOptions<DistributedCrdtOptions> options,
        IServiceProvider serviceProvider,
        ICrdtSerializer serializer,
        IDistributedCrdtStorage storage,
        ILogger<DistributedCrdtDocument<TState>> logger)
    {
        if (metadataManager == null) throw new ArgumentNullException(nameof(metadataManager));
        if (options == null) throw new ArgumentNullException(nameof(options));
        
        this.replicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));
        this.applicator = applicator ?? throw new ArgumentNullException(nameof(applicator));
        this.journalManager = journalManager ?? throw new ArgumentNullException(nameof(journalManager));
        this.syncService = syncService ?? throw new ArgumentNullException(nameof(syncService));
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.activeSyncEnabled = options.Value.ActiveSyncEnabled;

        var initialState = new TState();
        DocumentId = initialState.Id;
        
        if (string.IsNullOrWhiteSpace(DocumentId))
        {
            throw new InvalidOperationException($"The state model '{typeof(TState).Name}' must provide a valid DocumentId.");
        }
        
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
        var copiedVersions = new Dictionary<string, long>();
        var copiedDots = new Dictionary<string, ISet<long>>();

        // Lock to natively prevent cross-thread collection modification errors during serialization mappings
        lock (sourceDvv)
        {
            foreach (var kvp in sourceDvv.Versions)
            {
                copiedVersions[kvp.Key] = kvp.Value;
            }
            foreach (var kvp in sourceDvv.Dots)
            {
                copiedDots[kvp.Key] = new HashSet<long>(kvp.Value);
            }
        }

        return new DottedVersionVector(copiedVersions, copiedDots);
    }

    /// <inheritdoc />
    public async Task<(IReadOnlyList<CrdtOperation> Operations, bool SnapshotRequired)> GetMissingOperationsAsync(string remoteReplicaId, DottedVersionVector remoteState, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(remoteReplicaId)) throw new ArgumentException("Remote replica ID cannot be null or empty.", nameof(remoteReplicaId));
        if (remoteState == null) throw new ArgumentNullException(nameof(remoteState));

        var localState = GetLocalState();
        var requirement = syncService.CalculateRequirement(remoteReplicaId, remoteState, replicaContext.ReplicaId, localState);

        if (!requirement.IsBehind)
        {
            return (Array.Empty<CrdtOperation>(), false);
        }

        var allJournaledOps = new List<JournaledOperation>();
        var missingOpsStream = journalManager.GetMissingOperationsAsync(requirement, cancellationToken);

        await foreach (var jOp in missingOpsStream.ConfigureAwait(false))
        {
            allJournaledOps.Add(jOp);
        }

        bool journalTruncated = false;

        if (requirement.RequirementsByOrigin != null)
        {
            foreach (var kvp in requirement.RequirementsByOrigin)
            {
                var origin = kvp.Key;
                var req = kvp.Value;

                if (req.TargetContiguousVersion < req.SourceContiguousVersion)
                {
                    long firstRequiredClock = req.TargetContiguousVersion + 1;
                    
                    while (req.TargetKnownDots != null && req.TargetKnownDots.Contains(firstRequiredClock) && firstRequiredClock <= req.SourceContiguousVersion)
                    {
                        firstRequiredClock++;
                    }

                    if (firstRequiredClock <= req.SourceContiguousVersion)
                    {
                        bool hasRequired = allJournaledOps.Any(o => o.Operation.ReplicaId == origin && o.Operation.GlobalClock == firstRequiredClock);
                        if (!hasRequired)
                        {
                            journalTruncated = true;
                            break;
                        }
                    }
                }
                
                if (!journalTruncated && req.SourceMissingDots != null && req.SourceMissingDots.Count > 0)
                {
                    foreach (var dot in req.SourceMissingDots)
                    {
                        bool hasRequiredDot = allJournaledOps.Any(o => o.Operation.ReplicaId == origin && o.Operation.GlobalClock == dot);
                        if (!hasRequiredDot)
                        {
                            journalTruncated = true;
                            break;
                        }
                    }
                }

                if (journalTruncated) break;
            }
        }

        if (journalTruncated)
        {
            return (Array.Empty<CrdtOperation>(), true);
        }

        var documentOperations = allJournaledOps
            .Where(jOp => jOp.DocumentId == DocumentId)
            .Select(jOp => jOp.Operation)
            .ToList();

        return (documentOperations, false);
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
    public async Task BroadcastStateAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var p2pProtocol = serviceProvider.GetRequiredService<IP2pProtocol>();
            var state = GetLocalState();
            var syncMsg = new CrdtStateSyncMessage(replicaContext.ReplicaId, state);
            
            var payload = serializer.SerializeToBytes(syncMsg);
            var wrapper = new CrdtMessageWrapper(DocumentId, "CrdtSync", payload);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            await p2pProtocol.BroadcastAsync(finalBytes, cancellationToken).ConfigureAwait(false);
            
            logger.LogTrace("Broadcasted DVV state sync for document {DocumentId}.", DocumentId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to broadcast state for document {DocumentId}.", DocumentId);
        }
    }

    /// <inheritdoc />
    public async Task ProvideSnapshotAsync(string targetReplicaId, CancellationToken cancellationToken = default)
    {
        try
        {
            CrdtDocument<TState> currentDoc;
            DottedVersionVector globalState;

            // Strict Pipeline lock ensures extraction of Document and DVV cannot be horizontally torn by concurrent active patches organically
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

            var p2pProtocol = serviceProvider.GetRequiredService<IP2pProtocol>();
            await p2pProtocol.BroadcastAsync(finalBytes, cancellationToken).ConfigureAwait(false); 
            
            logger.LogInformation("Broadcasted complete structurally safe document snapshot fallback payload correctly for document {DocumentId}.", DocumentId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to broadcast snapshot payload mapping for document {DocumentId}.", DocumentId);
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
                // 1. Defend against "Destructive Overwrite" by isolating locally generated offline operations not known by the incoming snapshot natively.
                var localState = GetLocalState();
                var requirement = syncService.CalculateRequirement("snapshot", globalState, replicaContext.ReplicaId, localState);
                
                var missingLocalOps = new List<CrdtOperation>();
                if (requirement.IsBehind)
                {
                    var missingOpsStream = journalManager.GetMissingOperationsAsync(requirement, cancellationToken);
                    await foreach (var jOp in missingOpsStream.ConfigureAwait(false))
                    {
                        if (jOp.DocumentId == DocumentId)
                        {
                            missingLocalOps.Add(jOp.Operation);
                        }
                    }
                }

                // 2. Base mapping starts cleanly matching incoming fallback exactly
                CrdtDocument<TState> mergedDoc = snapshotDoc;
                
                // 3. Re-apply any structurally isolated operations directly over the snapshot restoring local offline continuity smoothly
                if (missingLocalOps.Count > 0)
                {
                    async IAsyncEnumerable<JournaledOperation> GetStreamAsync()
                    {
                        foreach (var op in missingLocalOps)
                        {
                            yield return new JournaledOperation(DocumentId, op);
                        }
                        await Task.CompletedTask;
                    }
                    
                    var result = await applicator.ApplyOperationsAsync(mergedDoc, GetStreamAsync()).ConfigureAwait(false);
                    mergedDoc = result.Document;
                    
                    logger.LogInformation("Safely re-integrated {Count} local isolated offline operations perfectly mapping over the inbound snapshot base protecting offline continuity for document {DocumentId}.", missingLocalOps.Count, DocumentId);
                }

                // 4. Commit fully protected mathematically merged architecture organically
                lock (syncRoot)
                {
                    Document = mergedDoc;
                    isDirty = true;
                }

                // Crucial alignment: Overwrite explicitly tracked encompassing P2P tracking vectors effectively matching the provider natively.
                lock (replicaContext.GlobalVersionVector)
                {
                    foreach (var kvp in globalState.Versions)
                    {
                        if (!replicaContext.GlobalVersionVector.Versions.TryGetValue(kvp.Key, out var currentVersion) || kvp.Value > currentVersion)
                        {
                            replicaContext.GlobalVersionVector.Versions[kvp.Key] = kvp.Value;
                        }
                    }
                    
                    foreach (var kvp in globalState.Dots)
                    {
                        if (!replicaContext.GlobalVersionVector.Dots.TryGetValue(kvp.Key, out var currentDots))
                        {
                            currentDots = new HashSet<long>();
                            replicaContext.GlobalVersionVector.Dots[kvp.Key] = currentDots;
                        }
                        foreach (var dot in kvp.Value)
                        {
                            currentDots.Add(dot);
                        }
                    }
                }
            }
            finally
            {
                modificationLock.Release();
            }

            StateChanged?.Invoke(this, EventArgs.Empty);
            logger.LogInformation("Successfully merged global state snapshot securely mapping initialization log gaps explicitly ensuring continuity for document {DocumentId}.", DocumentId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to safely process snapshot merger logic bridging states natively for document {DocumentId}.", DocumentId);
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
            // concurrent writes during saving from being permanently ignored.
            isDirty = false; 
        }

        try
        {
            // Intentionally bubble exceptions so orchestrator safely aborts overarching global log modifications natively avoiding write ahead gaps securely
            await storage.SaveDocumentAsync(DocumentId, currentDoc, cancellationToken).ConfigureAwait(false);
            logger.LogDebug("Successfully saved checkpoint to persistent storage for document {DocumentId}.", DocumentId);
        }
        catch (Exception)
        {
            lock (syncRoot)
            {
                // Revert flag on failure so the orchestrator attempts mapping it again logically on the next loop cleanly
                isDirty = true;
            }
            throw;
        }
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