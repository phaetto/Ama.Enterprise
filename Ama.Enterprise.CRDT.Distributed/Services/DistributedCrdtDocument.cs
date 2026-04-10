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
public sealed class DistributedCrdtDocument<TState> : IDistributedCrdtDocument<TState> where TState : class, new()
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
    private readonly object syncRoot = new();

    /// <inheritdoc />
    public string DocumentId { get; }

    /// <inheritdoc />
    public CrdtDocument<TState> Document { get; private set; }

    /// <inheritdoc />
    public event EventHandler? StateChanged;

    public DistributedCrdtDocument(
        string documentId,
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
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        if (metadataManager == null) throw new ArgumentNullException(nameof(metadataManager));
        if (options == null) throw new ArgumentNullException(nameof(options));
        
        DocumentId = documentId;
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
                lock (syncRoot)
                {
                    Document = storedDoc.Value;
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
        CrdtDocument<TState> currentDoc;
        lock (syncRoot)
        {
            currentDoc = Document;
        }

        var result = await applicator.ApplyPatchAsync(currentDoc, patch).ConfigureAwait(false);

        lock (syncRoot)
        {
            Document = result.Document;
        }

        try
        {
            await storage.SaveGlobalVersionVectorAsync(replicaContext.ReplicaId, replicaContext.GlobalVersionVector, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to persist global version vector to storage after local patch on document {DocumentId}.", DocumentId);
        }

        try
        {
            await storage.SaveDocumentAsync(DocumentId, Document, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to persist document {DocumentId} state to storage after local patch.", DocumentId);
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

        // Clone to prevent cross-thread modification errors during serialization mappings
        var copiedVersions = new Dictionary<string, long>(sourceDvv.Versions);
        var copiedDots = new Dictionary<string, ISet<long>>();
        
        foreach (var kvp in sourceDvv.Dots)
        {
            copiedDots[kvp.Key] = new HashSet<long>(kvp.Value);
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
        }

        try
        {
            await storage.SaveGlobalVersionVectorAsync(replicaContext.ReplicaId, replicaContext.GlobalVersionVector, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to persist global version vector to storage after remote operations synchronization on document {DocumentId}.", DocumentId);
        }

        try
        {
            await storage.SaveDocumentAsync(DocumentId, Document, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to persist document {DocumentId} state to storage after remote operations synchronization.", DocumentId);
        }

        StateChanged?.Invoke(this, EventArgs.Empty);

        // Intentionally completely removed the aggressive local Trimming mechanism here.
        // A single peer syncing operations must NOT actively delete those operations utilizing its own local bounds.
        // If it trims locally, other peers requiring those specific historic operations across different timings will encounter hard truncation gaps, natively spiraling network snapshots continuously.
        // Trimming effectively requires an independent process evaluating the actual multi-node GMVV mapping bounds actively.
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

            lock (syncRoot)
            {
                currentDoc = Document;
                globalState = GetLocalState(); // Extract explicit overarching Global Bounds
            }

            var snapshotData = serializer.SerializeToBytes(currentDoc);
            var resMsg = new CrdtSnapshotMessage(replicaContext.ReplicaId, snapshotData, globalState); // Bind explicitly separate matrices safely
            var payload = serializer.SerializeToBytes(resMsg);

            var wrapper = new CrdtMessageWrapper(DocumentId, "CrdtSnapshot", payload);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            var p2pProtocol = serviceProvider.GetRequiredService<IP2pProtocol>();
            await p2pProtocol.BroadcastAsync(finalBytes, cancellationToken).ConfigureAwait(false); 
            
            logger.LogInformation("Broadcasted complete document snapshot fallback payload correctly for document {DocumentId} directly addressing DVV log truncation.", DocumentId);
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

            lock (syncRoot)
            {
                Document = snapshotDoc;
                
                // Crucial alignment: Overwrite explicitly tracked encompassing P2P tracking vectors effectively matching the provider natively.
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

            try
            {
                await storage.SaveGlobalVersionVectorAsync(replicaContext.ReplicaId, replicaContext.GlobalVersionVector, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to persist global version vector to storage after snapshot alignment for document {DocumentId}.", DocumentId);
            }

            try
            {
                await storage.SaveDocumentAsync(DocumentId, Document, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to persist document {DocumentId} state to storage after snapshot application.", DocumentId);
            }

            StateChanged?.Invoke(this, EventArgs.Empty);
            logger.LogInformation("Successfully merged global state snapshot completely fulfilling initialization log gaps mapping document {DocumentId}.", DocumentId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process snapshot merger logic bridging states natively for document {DocumentId}.", DocumentId);
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
}