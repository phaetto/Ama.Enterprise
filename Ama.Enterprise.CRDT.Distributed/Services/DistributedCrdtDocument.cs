namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
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
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.activeSyncEnabled = options.Value.ActiveSyncEnabled;

        var initialState = new TState();
        var metadata = metadataManager.Initialize(initialState);
        Document = new CrdtDocument<TState>(initialState, metadata);
    }

    /// <inheritdoc />
    public async Task ApplyPatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default)
    {
        if (patch == null) throw new ArgumentNullException(nameof(patch));

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
    public async Task<IReadOnlyList<CrdtOperation>> GetMissingOperationsAsync(string remoteReplicaId, DottedVersionVector remoteState, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(remoteReplicaId)) throw new ArgumentException("Remote replica ID cannot be null or empty.", nameof(remoteReplicaId));
        if (remoteState == null) throw new ArgumentNullException(nameof(remoteState));

        var localState = GetLocalState();
        var requirement = syncService.CalculateRequirement(remoteReplicaId, remoteState, replicaContext.ReplicaId, localState);

        if (!requirement.IsBehind)
        {
            return Array.Empty<CrdtOperation>();
        }

        var operations = new List<CrdtOperation>();
        var missingOpsStream = journalManager.GetMissingOperationsAsync(requirement, cancellationToken);

        await foreach (var jOp in missingOpsStream.ConfigureAwait(false))
        {
            if (jOp.DocumentId == DocumentId)
            {
                operations.Add(jOp.Operation);
            }
        }

        return operations;
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