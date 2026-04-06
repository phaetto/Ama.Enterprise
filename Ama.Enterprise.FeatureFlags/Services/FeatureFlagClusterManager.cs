namespace Ama.Enterprise.FeatureFlags.Services;

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Journaling;
using Ama.CRDT.Services.Versioning;
using Ama.Enterprise.FeatureFlags.Models;

/// <summary>
/// Implementation of the cluster manager for feature flags.
/// </summary>
public sealed class FeatureFlagClusterManager : IFeatureFlagClusterManager
{
    private readonly ReplicaContext replicaContext;
    private readonly IAsyncCrdtApplicator applicator;
    private readonly ICrdtPatcher patcher;
    private readonly IJournalManager journalManager;
    private readonly IVersionVectorSyncService syncService;
    private readonly ICrdtMetadataManager metadataManager;

    private CrdtDocument<FeatureFlagState> document;
    private readonly object syncRoot = new();

    public FeatureFlagClusterManager(
        ReplicaContext replicaContext,
        IAsyncCrdtApplicator applicator,
        ICrdtPatcher patcher,
        IJournalManager journalManager,
        IVersionVectorSyncService syncService,
        ICrdtMetadataManager metadataManager)
    {
        this.replicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));
        this.applicator = applicator ?? throw new ArgumentNullException(nameof(applicator));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));
        this.journalManager = journalManager ?? throw new ArgumentNullException(nameof(journalManager));
        this.syncService = syncService ?? throw new ArgumentNullException(nameof(syncService));
        this.metadataManager = metadataManager ?? throw new ArgumentNullException(nameof(metadataManager));

        var initialState = new FeatureFlagState();
        var metadata = this.metadataManager.Initialize(initialState);
        document = new CrdtDocument<FeatureFlagState>(initialState, metadata);
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, FeatureFlag> GetFlags()
    {
        lock (syncRoot)
        {
            return new ReadOnlyDictionary<string, FeatureFlag>(document.Data.Flags);
        }
    }

    /// <inheritdoc />
    public async Task SetFlagAsync(string name, bool isEnabled, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Flag name cannot be null or empty.", nameof(name));
        }

        var flag = new FeatureFlag(name, isEnabled);

        CrdtDocument<FeatureFlagState> currentDoc;

        lock (syncRoot)
        {
            currentDoc = document;
        }

        var operation = patcher.GenerateOperation(currentDoc, x => x.Flags, new MapSetIntent(name, flag));
        var patch = new CrdtPatch(new[] { operation });

        var result = await applicator.ApplyPatchAsync(currentDoc, patch).ConfigureAwait(false);

        lock (syncRoot)
        {
            document = result.Document;
        }
    }

    /// <inheritdoc />
    public async Task RemoveFlagAsync(string name, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Flag name cannot be null or empty.", nameof(name));
        }

        CrdtDocument<FeatureFlagState> currentDoc;

        lock (syncRoot)
        {
            currentDoc = document;
        }

        var operation = patcher.GenerateOperation(currentDoc, x => x.Flags, new MapRemoveIntent(name));
        var patch = new CrdtPatch(new[] { operation });

        var result = await applicator.ApplyPatchAsync(currentDoc, patch).ConfigureAwait(false);

        lock (syncRoot)
        {
            document = result.Document;
        }
    }

    /// <inheritdoc />
    public DottedVersionVector GetLocalState()
    {
        // TODO: Add deep cloning to DVV
        var sourceDvv = replicaContext.GlobalVersionVector;

        // Creating a clone of the DottedVersionVector to avoid concurrent modification issues
        // during sync requirement calculations. Since the collections might be mutated internally.
        var copiedVersions = new Dictionary<string, long>(sourceDvv.Versions);
        var copiedDots = sourceDvv.Dots.ToDictionary(k => k.Key, v => (ISet<long>)new HashSet<long>(v.Value));

        return new DottedVersionVector(copiedVersions, copiedDots);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<CrdtOperation>> GetMissingOperationsAsync(string remoteReplicaId, DottedVersionVector remoteState, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(remoteReplicaId))
        {
            throw new ArgumentException("Remote replica ID cannot be null or empty.", nameof(remoteReplicaId));
        }

        if (remoteState == null)
        {
            throw new ArgumentNullException(nameof(remoteState));
        }

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
            operations.Add(jOp.Operation);
        }

        return operations;
    }

    /// <inheritdoc />
    public async Task ApplyOperationsAsync(IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default)
    {
        if (operations == null)
        {
            throw new ArgumentNullException(nameof(operations));
        }

        if (operations.Count == 0)
        {
            return;
        }

        CrdtDocument<FeatureFlagState> currentDoc;

        lock (syncRoot)
        {
            currentDoc = document;
        }

        // Use stream based ApplyOperationsAsync to leverage the framework's strict dependency ordering
        async IAsyncEnumerable<JournaledOperation> GetStreamAsync()
        {
            var docId = currentDoc.Data.Id;
            foreach (var op in operations)
            {
                yield return new JournaledOperation(docId, op);
            }
            await Task.CompletedTask;
        }

        var result = await applicator.ApplyOperationsAsync(currentDoc, GetStreamAsync()).ConfigureAwait(false);

        lock (syncRoot)
        {
            document = result.Document;
        }
    }
}