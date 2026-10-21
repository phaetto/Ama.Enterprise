namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Pooling;
using Ama.CRDT.Services.Providers;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Generic document manager responsible for maintaining the state of a specific CRDT tree processing active shards.
/// </summary>
public sealed class DistributedCrdtDocument<TState> : IDistributedCrdtDocument<TState>, IDocumentCommandTarget, IDisposable where TState : class, new()
{
    private static readonly ConcurrentDictionary<string, Expression<Func<TState, object>>> PropertySelectorCache = new(StringComparer.Ordinal);

    // Shared static estimation resources to prevent per-document HashSet retention and LOH bloat
    private static readonly object EstimationSyncRoot = new();
    private static readonly HashSet<object> SharedEstimationVisitedSet = new(ReferenceEqualityComparer.Instance);

    private readonly IServiceProvider serviceProvider;
    private readonly IShardVersionVectorManager versionVectorManager;
    private readonly IAsyncCrdtApplicator applicator;
    private readonly ICrdtApplicator syncApplicator;
    private readonly IAsyncCrdtMerger merger;
    private readonly ICrdtMetadataManager metadataManager;
    private readonly ICrdtSerializer serializer;
    private readonly IDistributedCrdtStorage storage;
    private readonly IShardDistributionManager distributionManager;
    private readonly IDocumentCommandPipeline commandPipeline;
    private readonly IDistributedCrdtTelemetry telemetry;
    private readonly ICrdtStringPool stringPool;
    private readonly ILogger<DistributedCrdtDocument<TState>> logger;
    
    private readonly IEnumerable<IDocumentStateObserver<TState>> stateObservers;
    private readonly IEnumerable<IDocumentPatchObserver> patchObservers;

    private readonly bool activeSyncEnabled;
    private readonly int activeSyncDebounceMs;
    private readonly bool avoidBlindWrites;
    private readonly TState initialState;
    
    private readonly object syncRoot = new();
    private CrdtDocument<TState> currentDocument;
    
    private readonly object debounceSyncRoot = new();
    private readonly List<CrdtOperation>? debounceBuffer;
    private readonly Timer? debounceTimer;

    private volatile bool isDirty;
    private volatile bool isDisposed;

    private CrdtAotContext[]? cachedAotContexts;

    /// <inheritdoc />
    public string ShardId { get; }

    /// <inheritdoc />
    public string DocumentId { get; }

    /// <inheritdoc />
    public string TypeAlias { get; }

    public DistributedCrdtDocument(
        TState initialState,
        IServiceProvider serviceProvider,
        IShardVersionVectorManager versionVectorManager,
        IAsyncCrdtApplicator applicator,
        ICrdtApplicator syncApplicator,
        IAsyncCrdtMerger merger,
        ICrdtMetadataManager metadataManager,
        IOptions<DistributedCrdtOptions> options,
        ICrdtSerializer serializer,
        IDistributedCrdtStorage storage,
        ILogger<DistributedCrdtDocument<TState>> logger,
        IDocumentIdProvider documentIdProvider,
        IShardIdProvider shardIdProvider,
        IShardDistributionManager distributionManager,
        IDocumentCommandPipeline commandPipeline,
        IDistributedCrdtTelemetry telemetry,
        IEnumerable<IDocumentStateObserver<TState>> stateObservers,
        IEnumerable<IDocumentPatchObserver> patchObservers,
        string? typeAlias = null,
        ICrdtStringPool? stringPool = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(initialState);
        ArgumentNullException.ThrowIfNull(documentIdProvider);
        ArgumentNullException.ThrowIfNull(shardIdProvider);
        ArgumentNullException.ThrowIfNull(distributionManager);

        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.versionVectorManager = versionVectorManager ?? throw new ArgumentNullException(nameof(versionVectorManager));
        this.applicator = applicator ?? throw new ArgumentNullException(nameof(applicator));
        this.syncApplicator = syncApplicator ?? throw new ArgumentNullException(nameof(syncApplicator));
        this.merger = merger ?? throw new ArgumentNullException(nameof(merger));
        this.metadataManager = metadataManager ?? throw new ArgumentNullException(nameof(metadataManager));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        this.distributionManager = distributionManager;
        this.commandPipeline = commandPipeline ?? throw new ArgumentNullException(nameof(commandPipeline));
        this.telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        this.stringPool = stringPool ?? CrdtStringPool.Shared;
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        
        this.stateObservers = stateObservers ?? throw new ArgumentNullException(nameof(stateObservers));
        this.patchObservers = patchObservers ?? throw new ArgumentNullException(nameof(patchObservers));

        this.activeSyncEnabled = options.Value.ActiveSyncEnabled;
        this.activeSyncDebounceMs = options.Value.ActiveSyncDebounceMilliseconds;
        this.avoidBlindWrites = options.Value.AvoidBlindCheckpointWrites;
        this.initialState = initialState;

        var metadata = metadataManager.Initialize(initialState);
        currentDocument = new CrdtDocument<TState>(initialState, metadata);

        DocumentId = this.stringPool.GetOrAdd(documentIdProvider.GetDocumentId(initialState).ToString() ?? string.Empty);
        TypeAlias = this.stringPool.GetOrAdd(ResolveTypeAlias(serviceProvider, typeAlias));
        ShardId = this.stringPool.GetOrAdd(initialState is CrdtRegistryState
            ? DocumentId
            : shardIdProvider.GetShardId(currentDocument) ?? string.Empty);

        if (this.activeSyncEnabled && this.activeSyncDebounceMs > 0)
        {
            this.debounceBuffer = new List<CrdtOperation>();
            this.debounceTimer = new Timer(OnDebounceTimerFired, null, Timeout.Infinite, Timeout.Infinite);
        }
    }
    
    /// <inheritdoc />
    public ValueTask<CrdtDocument<TState>> GetDocumentAsync(CancellationToken cancellationToken = default)
    {
        lock (syncRoot)
        {
            return new ValueTask<CrdtDocument<TState>>(currentDocument);
        }
    }

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (isDisposed) return;

        var cmd = commandPipeline.RentCommand();
        cmd.Target = this;
        cmd.Type = DocumentCommandType.Initialize;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandPipeline.EnqueueAndWaitAsync(cmd, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            commandPipeline.ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public async Task<bool> ApplyRemoteIntentAsync(IOperationIntent intent, string targetProperty, IAsyncCrdtPatcher patcher, IntentDeliveryMode deliveryMode = IntentDeliveryMode.Acknowledged, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(intent);
        ArgumentNullException.ThrowIfNull(patcher);

        if (isDisposed) return false;

        if (!string.IsNullOrEmpty(targetProperty))
        {
            targetProperty = stringPool.GetOrAdd(targetProperty);
        }

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            var ffCmd = commandPipeline.RentCommand();
            ffCmd.Target = this;
            ffCmd.Type = DocumentCommandType.ApplyRemoteIntent;
            ffCmd.Intent = intent;
            ffCmd.TargetProperty = targetProperty;
            ffCmd.Patcher = patcher;

            await commandPipeline.EnqueueFireAndForgetAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            return true;
        }

        var cmd = commandPipeline.RentCommand();
        cmd.Target = this;
        cmd.Type = DocumentCommandType.ApplyRemoteIntent;
        cmd.Intent = intent;
        cmd.TargetProperty = targetProperty;
        cmd.Patcher = patcher;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandPipeline.EnqueueAndWaitAsync(cmd, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            commandPipeline.ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public Task ApplyPatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default)
    {
        return ApplyPatchAsync(patch, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task ApplyPatchAsync(CrdtPatch patch, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (isDisposed) return;

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            var ffCmd = commandPipeline.RentCommand();
            ffCmd.Target = this;
            ffCmd.Type = DocumentCommandType.ApplyPatch;
            ffCmd.Patch = patch;

            await commandPipeline.EnqueueFireAndForgetAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            return;
        }

        var cmd = commandPipeline.RentCommand();
        cmd.Target = this;
        cmd.Type = DocumentCommandType.ApplyPatch;
        cmd.Patch = patch;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandPipeline.EnqueueAndWaitAsync(cmd, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            commandPipeline.ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public ShardVersionVectorMap GetLocalState()
    {
        return versionVectorManager.GetShardSnapshot(ShardId);
    }

    /// <inheritdoc />
    public Task ApplyRemotePatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default)
    {
        return ApplyRemotePatchAsync(patch, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task ApplyRemotePatchAsync(CrdtPatch patch, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (isDisposed) return;

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            var ffCmd = commandPipeline.RentCommand();
            ffCmd.Target = this;
            ffCmd.Type = DocumentCommandType.ApplyRemotePatch;
            ffCmd.Patch = patch;

            await commandPipeline.EnqueueFireAndForgetAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            return;
        }

        var cmd = commandPipeline.RentCommand();
        cmd.Target = this;
        cmd.Type = DocumentCommandType.ApplyRemotePatch;
        cmd.Patch = patch;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandPipeline.EnqueueAndWaitAsync(cmd, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            commandPipeline.ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public async Task ApplyJournaledPatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default)
    {
        if (isDisposed) return;

        var cmd = commandPipeline.RentCommand();
        cmd.Target = this;
        cmd.Type = DocumentCommandType.ApplyJournaledPatch;
        cmd.Patch = patch;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandPipeline.EnqueueAndWaitAsync(cmd, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            commandPipeline.ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public async Task<CrdtSnapshotDataDto> GetSnapshotDataAsync(CancellationToken cancellationToken = default)
    {
        if (isDisposed)
        {
            CrdtDocument<TState> currentDoc;
            lock (syncRoot) { currentDoc = currentDocument; }
            var snapshotData = serializer.SerializeToBytes(currentDoc);
            return new CrdtSnapshotDataDto(snapshotData, GetLocalState());
        }

        var cmd = commandPipeline.RentCommand();
        cmd.Target = this;
        cmd.Type = DocumentCommandType.GetSnapshotData;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandPipeline.EnqueueAndWaitAsync(cmd, cancellationToken).ConfigureAwait(false);
            return new CrdtSnapshotDataDto(cmd.ResultSnapshotData!, cmd.ResultGlobalState!);
        }
        finally
        {
            commandPipeline.ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public Task MergeSnapshotAsync(byte[] snapshotData, ShardVersionVectorMap globalState, CancellationToken cancellationToken = default)
    {
        return MergeSnapshotAsync(snapshotData, globalState, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task MergeSnapshotAsync(byte[] snapshotData, ShardVersionVectorMap globalState, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (snapshotData == null || snapshotData.Length == 0 || isDisposed) return;
        ArgumentNullException.ThrowIfNull(globalState);

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            var ffCmd = commandPipeline.RentCommand();
            ffCmd.Target = this;
            ffCmd.Type = DocumentCommandType.MergeSnapshot;
            ffCmd.SnapshotData = snapshotData;
            ffCmd.GlobalState = globalState;

            await commandPipeline.EnqueueFireAndForgetAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            return;
        }

        var cmd = commandPipeline.RentCommand();
        cmd.Target = this;
        cmd.Type = DocumentCommandType.MergeSnapshot;
        cmd.SnapshotData = snapshotData;
        cmd.GlobalState = globalState;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandPipeline.EnqueueAndWaitAsync(cmd, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            commandPipeline.ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public Task CheckpointAsync(CancellationToken cancellationToken = default)
    {
        if (isDisposed) return Task.CompletedTask;
        if (avoidBlindWrites && !isDirty) return Task.CompletedTask;

        return ExecuteCheckpointDirectAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task EvictReplicaAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        return EvictReplicaAsync(replicaId, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task EvictReplicaAsync(string replicaId, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));

        if (isDisposed) return;

        replicaId = stringPool.GetOrAdd(replicaId);

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            var ffCmd = commandPipeline.RentCommand();
            ffCmd.Target = this;
            ffCmd.Type = DocumentCommandType.EvictReplica;
            ffCmd.ReplicaIdToEvict = replicaId;

            await commandPipeline.EnqueueFireAndForgetAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            return;
        }

        var cmd = commandPipeline.RentCommand();
        cmd.Target = this;
        cmd.Type = DocumentCommandType.EvictReplica;
        cmd.ReplicaIdToEvict = replicaId;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandPipeline.EnqueueAndWaitAsync(cmd, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            commandPipeline.ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public async Task ResetLocalStateAsync(string oldReplicaId, CancellationToken cancellationToken = default)
    {
        if (isDisposed) return;

        if (!string.IsNullOrEmpty(oldReplicaId))
        {
            oldReplicaId = stringPool.GetOrAdd(oldReplicaId);
        }

        var cmd = commandPipeline.RentCommand();
        cmd.Target = this;
        cmd.Type = DocumentCommandType.ResetLocalState;
        cmd.OldReplicaId = oldReplicaId;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandPipeline.EnqueueAndWaitAsync(cmd, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            commandPipeline.ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public async ValueTask<long> EstimateObjectSizeAsync(CancellationToken cancellationToken = default)
    {
        var footprint = await EstimateMemoryFootprintAsync(cancellationToken).ConfigureAwait(false);
        return footprint.WholeDocumentBytes;
    }

    /// <inheritdoc />
    public ValueTask<DocumentMemoryFootprintDto> EstimateMemoryFootprintAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        CrdtDocument<TState> currentDoc;

        lock (syncRoot)
        {
            currentDoc = currentDocument;
        }

        var aotContexts = GetAotContexts();

        long dataBytes;
        long metadataBytes;
        long dataAndMetaBytes;
        long wholeObjectBytes;

        lock (EstimationSyncRoot)
        {
            SharedEstimationVisitedSet.Clear();

            dataBytes = EstimateGraphSize(currentDoc.Data, SharedEstimationVisitedSet, aotContexts);
            metadataBytes = EstimateGraphSize(currentDoc.Metadata, SharedEstimationVisitedSet, aotContexts);
            dataAndMetaBytes = 32L + dataBytes + metadataBytes;

            // Base overhead for DistributedCrdtDocument instance, references, and locks
            long managerOverheadBytes = 256;

            // Strings (Object header + length + UTF-16 chars)
            managerOverheadBytes += EstimateStringSize(DocumentId);
            managerOverheadBytes += EstimateStringSize(ShardId);

            // Debounce buffer and timer overhead
            if (debounceBuffer != null)
            {
                lock (debounceSyncRoot)
                {
                    managerOverheadBytes += 160 + (debounceBuffer.Capacity * 64L);
                }
            }

            wholeObjectBytes = managerOverheadBytes + dataAndMetaBytes;

            SharedEstimationVisitedSet.Clear();
            SharedEstimationVisitedSet.TrimExcess(64);
        }

        var globalState = GetLocalState();
        var shardDvvBytes = EstimateShardVersionVectorSize(globalState);

        return new ValueTask<DocumentMemoryFootprintDto>(
            new DocumentMemoryFootprintDto(dataBytes, metadataBytes, dataAndMetaBytes, wholeObjectBytes, shardDvvBytes));
    }

    /// <inheritdoc />
    public async Task ExecuteCommandInternalAsync(PooledDocumentCommand cmd)
    {
        ArgumentNullException.ThrowIfNull(cmd);

        switch (cmd.Type)
        {
            case DocumentCommandType.Initialize:
                await ProcessInitializeInternalAsync(cmd.CancellationToken).ConfigureAwait(false);
                break;
            case DocumentCommandType.ApplyRemoteIntent:
                await ProcessApplyRemoteIntentInternalAsync(cmd).ConfigureAwait(false);
                break;
            case DocumentCommandType.ApplyPatch:
                await ProcessApplyPatchInternalAsync(cmd).ConfigureAwait(false);
                break;
            case DocumentCommandType.ApplyRemotePatch:
                await ProcessApplyRemotePatchInternalAsync(cmd).ConfigureAwait(false);
                break;
            case DocumentCommandType.ApplyJournaledPatch:
                await ProcessApplyJournaledPatchInternalAsync(cmd).ConfigureAwait(false);
                break;
            case DocumentCommandType.GetSnapshotData:
                await ProcessGetSnapshotDataInternalAsync(cmd).ConfigureAwait(false);
                break;
            case DocumentCommandType.MergeSnapshot:
                await ProcessMergeSnapshotInternalAsync(cmd).ConfigureAwait(false);
                break;
            case DocumentCommandType.Checkpoint:
                await ProcessCheckpointInternalAsync(cmd).ConfigureAwait(false);
                break;
            case DocumentCommandType.EvictReplica:
                await ProcessEvictReplicaInternalAsync(cmd).ConfigureAwait(false);
                break;
            case DocumentCommandType.ResetLocalState:
                await ProcessResetLocalStateInternalAsync(cmd.CancellationToken).ConfigureAwait(false);
                break;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        isDisposed = true;
        debounceTimer?.Dispose();
    }

    private static Expression<Func<TState, object>> GetOrAddPropertySelector(string? targetProperty)
    {
        var key = targetProperty is not null ? CrdtStringPool.Shared.GetOrAdd(targetProperty) : string.Empty;
        return PropertySelectorCache.GetOrAdd(key, prop =>
        {
            var parameter = Expression.Parameter(typeof(TState), "x");
            Expression propertyExpression = string.IsNullOrWhiteSpace(prop)
                ? parameter
                : Expression.PropertyOrField(parameter, prop);
            var converted = Expression.Convert(propertyExpression, typeof(object));
            return Expression.Lambda<Func<TState, object>>(converted, parameter);
        });
    }

    private async Task NotifyStateChangedAsync(CancellationToken cancellationToken)
    {
        foreach (var observer in stateObservers)
        {
            try
            {
                await observer.OnStateChangedAsync(DocumentId, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error notifying state observer for document {DocumentId}.", DocumentId);
            }
        }
    }

    private async Task NotifyPatchGeneratedAsync(CrdtPatch patch, CancellationToken cancellationToken)
    {
        foreach (var observer in patchObservers)
        {
            try
            {
                await observer.OnPatchGeneratedAsync(DocumentId, TypeAlias, patch, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error notifying patch observer for document {DocumentId}.", DocumentId);
            }
        }
    }

    private async Task ProcessInitializeInternalAsync(CancellationToken cancellationToken)
    {
        try
        {
            var isLocalShard = await distributionManager.IsShardLocalAsync(ShardId, cancellationToken).ConfigureAwait(false);
            if (!isLocalShard)
            {
                logger.LogWarning("Document initialized for a remote shard {ShardId}. The Orchestrator should handle remote requests.", this.ShardId);
            }

            var storageDocKey = GetStorageDocumentKey();
            var storedDoc = await storage.LoadDocumentAsync<TState>(ShardId, storageDocKey, cancellationToken).ConfigureAwait(false);
            if (storedDoc == null && !string.Equals(storageDocKey, DocumentId, StringComparison.Ordinal))
            {
                storedDoc = await storage.LoadDocumentAsync<TState>(ShardId, DocumentId, cancellationToken).ConfigureAwait(false);
            }
            
            if (storedDoc == null)
            {
                storedDoc = await storage.LoadOrphanedDocumentAsync<TState>(ShardId, storageDocKey, cancellationToken).ConfigureAwait(false);
                if (storedDoc == null && !string.Equals(storageDocKey, DocumentId, StringComparison.Ordinal))
                {
                    storedDoc = await storage.LoadOrphanedDocumentAsync<TState>(ShardId, DocumentId, cancellationToken).ConfigureAwait(false);
                }

                if (storedDoc != null)
                {
                    logger.LogInformation("Loaded orphaned state for document {DocumentId} ({TypeAlias}) from storage.", DocumentId, TypeAlias);
                }
            }

            if (storedDoc != null)
            {
                lock (syncRoot)
                {
                    currentDocument = storedDoc.Value;
                }

                await NotifyStateChangedAsync(cancellationToken).ConfigureAwait(false);
                logger.LogInformation("Loaded initial state for document {DocumentId} ({TypeAlias}) from storage.", DocumentId, TypeAlias);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load initial state for document {DocumentId} ({TypeAlias}) from storage.", DocumentId, TypeAlias);
            throw;
        }
    }

    private async Task ProcessApplyRemoteIntentInternalAsync(PooledDocumentCommand cmd)
    {
        try
        {
            CrdtPatch generatedPatch = default;

            await versionVectorManager.ExecuteShardLockedAsync(ShardId, async () =>
            {
                CrdtDocument<TState> docState;
                lock (syncRoot)
                {
                    docState = currentDocument;
                }

                var propertySelector = GetOrAddPropertySelector(cmd.TargetProperty);
                var operation = await cmd.Patcher!.GenerateOperationAsync(docState, propertySelector, cmd.Intent!, cmd.CancellationToken).ConfigureAwait(false);
                generatedPatch = new CrdtPatch([operation]);
                cmd.Patch = generatedPatch;

                var result = await applicator.ApplyPatchAsync(docState, generatedPatch).ConfigureAwait(false);

                lock (syncRoot)
                {
                    currentDocument = result.Document;
                    isDirty = true;
                }
            }, cmd.CancellationToken).ConfigureAwait(false);

            telemetry.RecordDocumentPatchApplied(DocumentId);
            telemetry.RecordDocumentIntentApplied(DocumentId);

            await NotifyStateChangedAsync(cmd.CancellationToken).ConfigureAwait(false);
            await TriggerActiveSyncBroadcastAsync(generatedPatch, cmd.CancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            telemetry.RecordDocumentIntentFailed(DocumentId);
            logger.LogError(ex, "Failed to apply forwarded intent to local document {DocumentId}.", DocumentId);
            throw;
        }
    }

    private async Task ProcessApplyPatchInternalAsync(PooledDocumentCommand cmd)
    {
        try
        {
            await versionVectorManager.ExecuteShardLockedAsync(ShardId, async () =>
            {
                CrdtDocument<TState> currentDoc;
                lock (syncRoot) { currentDoc = currentDocument; }

                var result = await applicator.ApplyPatchAsync(currentDoc, cmd.Patch!.Value).ConfigureAwait(false);

                lock (syncRoot)
                {
                    currentDocument = result.Document;
                    isDirty = true;
                }
            }, cmd.CancellationToken).ConfigureAwait(false);

            telemetry.RecordDocumentPatchApplied(DocumentId);
            await NotifyStateChangedAsync(cmd.CancellationToken).ConfigureAwait(false);
            await TriggerActiveSyncBroadcastAsync(cmd.Patch!.Value, cmd.CancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to apply local patch to document {DocumentId}.", DocumentId);
            throw;
        }
    }

    private async Task TriggerActiveSyncBroadcastAsync(CrdtPatch patch, CancellationToken cancellationToken)
    {
        if (activeSyncEnabled && patch.Operations != null && patch.Operations.Count > 0)
        {
            if (activeSyncDebounceMs > 0)
            {
                lock (debounceSyncRoot)
                {
                    debounceBuffer!.AddRange(patch.Operations);
                    debounceTimer!.Change(activeSyncDebounceMs, Timeout.Infinite);
                }
            }
            else
            {
                await NotifyPatchGeneratedAsync(patch, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private void OnDebounceTimerFired(object? state)
    {
        List<CrdtOperation> opsToBroadcast;

        lock (debounceSyncRoot)
        {
            if (debounceBuffer == null || debounceBuffer.Count == 0) return;
            
            opsToBroadcast = new List<CrdtOperation>(debounceBuffer);
            debounceBuffer.Clear();
        }

        if (opsToBroadcast.Count > 0)
        {
            var batchedPatch = new CrdtPatch(opsToBroadcast);
            _ = Task.Run(async () =>
            {
                try
                {
                    await NotifyPatchGeneratedAsync(batchedPatch, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Unhandled exception while broadcasting debounced patch for document {DocumentId}.", DocumentId);
                }
            });
        }
    }

    private async Task ProcessApplyRemotePatchInternalAsync(PooledDocumentCommand cmd)
    {
        try
        {
            await versionVectorManager.ExecuteShardLockedAsync(ShardId, async () =>
            {
                CrdtDocument<TState> currentDoc;
                lock (syncRoot) { currentDoc = currentDocument; }

                var result = await applicator.ApplyPatchAsync(currentDoc, cmd.Patch!.Value).ConfigureAwait(false);

                lock (syncRoot)
                {
                    currentDocument = result.Document;
                    isDirty = true;
                }
            }, cmd.CancellationToken).ConfigureAwait(false);

            telemetry.RecordDocumentPatchApplied(DocumentId);
            await NotifyStateChangedAsync(cmd.CancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to apply remote patch to document {DocumentId}.", DocumentId);
            throw;
        }
    }

    private async Task ProcessApplyJournaledPatchInternalAsync(PooledDocumentCommand cmd)
    {
        try
        {
            await versionVectorManager.ExecuteShardLockedAsync(ShardId, () =>
            {
                CrdtDocument<TState> currentDoc;
                lock (syncRoot) { currentDoc = currentDocument; }

                var result = syncApplicator.ApplyPatch(currentDoc, cmd.Patch!.Value);

                lock (syncRoot)
                {
                    currentDocument = result.Document;
                    isDirty = true;
                }

                return Task.CompletedTask;
            }, cmd.CancellationToken).ConfigureAwait(false);

            telemetry.RecordDocumentPatchApplied(DocumentId);
            await NotifyStateChangedAsync(cmd.CancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to apply journaled patch to document {DocumentId}.", DocumentId);
            throw;
        }
    }

    private async Task ProcessGetSnapshotDataInternalAsync(PooledDocumentCommand cmd)
    {
        try
        {
            await versionVectorManager.ExecuteShardLockedAsync(ShardId, () =>
            {
                CrdtDocument<TState> currentDoc;
                lock (syncRoot) { currentDoc = currentDocument; }
                
                var globalState = GetLocalState(); 

                cmd.ResultSnapshotData = serializer.SerializeToBytes(currentDoc);
                cmd.ResultGlobalState = globalState;
                return Task.CompletedTask;
            }, cmd.CancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get snapshot data for document {DocumentId}.", DocumentId);
            throw;
        }
    }

    private async Task ProcessMergeSnapshotInternalAsync(PooledDocumentCommand cmd)
    {
        try
        {
            var snapshotDoc = serializer.DeserializeFromBytes<CrdtDocument<TState>>(cmd.SnapshotData!);

            await versionVectorManager.ExecuteShardLockedAsync(ShardId, async () =>
            {
                CrdtDocument<TState> currentDoc;
                lock (syncRoot) { currentDoc = currentDocument; }

                await merger.MergeStateAsync(currentDoc, snapshotDoc, cmd.CancellationToken).ConfigureAwait(false);

                lock (syncRoot)
                {
                    currentDocument = currentDoc;
                    isDirty = true;
                }

            if ( cmd.GlobalState != null )
            {
                var activeShardDocIds = GetActiveDocumentIdsForShard();
                var compositeDocId = GetStorageDocumentKey();
                versionVectorManager.RecordDocumentSnapshotMerge(ShardId, compositeDocId, cmd.GlobalState, activeShardDocIds);
            }
            }, cmd.CancellationToken).ConfigureAwait(false);

            telemetry.RecordDocumentSnapshotMerged(DocumentId);
            await NotifyStateChangedAsync(cmd.CancellationToken).ConfigureAwait(false);

            logger.LogInformation("Merged full state snapshot for document {DocumentId}.", DocumentId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to merge snapshot for document {DocumentId}.", DocumentId);
            throw;
        }
    }

    private IReadOnlyList<string> GetActiveDocumentIdsForShard()
    {
        var compositeSelf = GetStorageDocumentKey();
        var orchestrator = serviceProvider.GetService<ICrdtDocumentOrchestrator>();
        if (orchestrator == null)
        {
            return [compositeSelf];
        }

        try
        {
            var docs = orchestrator.GetActiveDocuments();
            var list = new List<string>();
            foreach (var doc in docs)
            {
                if (string.Equals(doc.ShardId, ShardId, StringComparison.Ordinal)
                    && !string.Equals(doc.DocumentId, Constants.SystemRegistryDocumentId, StringComparison.Ordinal))
                {
                    list.Add(CrdtRegistryEntry.GetRegistryKey(doc.TypeAlias, doc.DocumentId));
                }
            }

            if (!list.Contains(compositeSelf))
            {
                list.Add(compositeSelf);
            }

            return list;
        }
        catch (InvalidOperationException)
        {
            return [compositeSelf];
        }
    }

    private Task ProcessCheckpointInternalAsync(PooledDocumentCommand cmd)
    {
        return ExecuteCheckpointDirectAsync(cmd.CancellationToken);
    }

    private async Task ExecuteCheckpointDirectAsync(CancellationToken cancellationToken)
    {
        CrdtDocument<TState> currentDoc = default;
        var shouldSave = false;

        try
        {
            await versionVectorManager.ExecuteShardLockedAsync(ShardId, () =>
            {
                lock (syncRoot)
                {
                    if (avoidBlindWrites && !isDirty)
                    {
                        return Task.CompletedTask;
                    }

                    // Materialize an isolated snapshot copy inside the shard and document locks
                    // so storage backends can serialize or persist it without concurrent collection mutation.
                    var snapshotBytes = serializer.SerializeToBytes(currentDocument);
                    currentDoc = serializer.DeserializeFromBytes<CrdtDocument<TState>>(snapshotBytes);

                    isDirty = false;
                    shouldSave = true;
                }

                return Task.CompletedTask;
            }, cancellationToken).ConfigureAwait(false);

            if (!shouldSave)
            {
                return;
            }

            var storageDocKey = GetStorageDocumentKey();
            await storage.SaveDocumentAsync(ShardId, storageDocKey, currentDoc, cancellationToken).ConfigureAwait(false);

            telemetry.RecordDocumentCheckpointSaved(DocumentId);
            logger.LogDebug("Saved checkpoint to storage for document {DocumentId}.", DocumentId);
        }
        catch (TaskCanceledException ex)
        {
            if (shouldSave)
            {
                lock (syncRoot) { isDirty = true; }
            }
        }
        catch (Exception ex)
        {
            if (shouldSave)
            {
                lock (syncRoot) { isDirty = true; }
            }

            logger.LogError(ex, "Failed to save checkpoint to storage for document {DocumentId}.", DocumentId);
            throw;
        }
    }

    private async Task ProcessEvictReplicaInternalAsync(PooledDocumentCommand cmd)
    {
        try
        {
            await versionVectorManager.ExecuteShardLockedAsync(ShardId, () =>
            {
                lock (syncRoot)
                {
                    metadataManager.EvictReplica(currentDocument, cmd.ReplicaIdToEvict!);
                    isDirty = true;
                }

                return Task.CompletedTask;
            }, cmd.CancellationToken).ConfigureAwait(false);
            
            telemetry.RecordDocumentReplicaEviction(DocumentId);
            await NotifyStateChangedAsync(cmd.CancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to evict replica {ReplicaId} from document {DocumentId}.", cmd.ReplicaIdToEvict, DocumentId);
            throw;
        }
    }

    private async Task ProcessResetLocalStateInternalAsync(CancellationToken cancellationToken)
    {
        try
        {
            await versionVectorManager.ExecuteShardLockedAsync(ShardId, () =>
            {
                lock (syncRoot)
                {
                    var metadata = metadataManager.Initialize(initialState);
                    currentDocument = new CrdtDocument<TState>(initialState, metadata);
                    isDirty = true;
                }

                return Task.CompletedTask;
            }, cancellationToken).ConfigureAwait(false);
            
            telemetry.RecordDocumentStateReset(DocumentId);
            await NotifyStateChangedAsync(cancellationToken).ConfigureAwait(false);
            logger.LogInformation("Performed a hard reset for document {DocumentId}.", DocumentId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to reset local state for document {DocumentId}.", DocumentId);
            throw;
        }
    }

    private static long EstimateShardVersionVectorSize(ShardVersionVectorMap globalState)
    {
        if (globalState == null)
        {
            return 0;
        }

        // ConcurrentDictionary / Dictionary wrapper + bucket arrays
        long size = 128;
        foreach (var kvp in globalState)
        {
            // Entry overhead + key string + DottedVersionVector object header
            size += 64 + EstimateStringSize(kvp.Key);
            if (kvp.Value.Dots != null)
            {
                // Dots dictionary overhead + 24 bytes per dot entry (long + hash bucket)
                size += 80 + (kvp.Value.Dots.Count * 24L);
            }
        }

        return size;
    }

    private static long EstimateStringSize(string? str)
    {
        if (str is null)
        {
            return 0;
        }

        // 64-bit CLR string: 16B object header + 4B length + 2B null terminator + UTF-16 chars, aligned to 8 bytes
        long raw = 22L + ((long)str.Length * sizeof(char));
        return (raw + 7L) & ~7L;
    }

    private CrdtAotContext[] GetAotContexts()
    {
        if (cachedAotContexts != null)
        {
            return cachedAotContexts;
        }

        var resolved = serviceProvider.GetServices<CrdtAotContext>();
        var list = new List<CrdtAotContext>();
        foreach (var ctx in resolved)
        {
            list.Add(ctx);
        }

        cachedAotContexts = list.ToArray();
        return cachedAotContexts;
    }

    private static long EstimateGraphSize(object? obj, HashSet<object> visited, CrdtAotContext[] aotContexts)
    {
        if (obj is null)
        {
            return 0;
        }

        if (obj is string str)
        {
            return EstimateStringSize(str);
        }

        var type = obj.GetType();
        if (IsSimpleValueType(type))
        {
            return GetPrimitiveOrSimpleValueSize(type);
        }

        if (!type.IsValueType && !visited.Add(obj))
        {
            return 0;
        }

        if (obj is byte[] byteArray)
        {
            return 24L + byteArray.LongLength;
        }

        if (obj is Array array)
        {
            var elemType = type.GetElementType();
            if (elemType != null && IsSimpleValueType(elemType))
            {
                return 24L + (array.LongLength * GetPrimitiveOrSimpleValueSize(elemType));
            }

            long arrSize = 24L + (array.LongLength * 8L);
            foreach (var item in array)
            {
                arrSize += EstimateGraphSize(item, visited, aotContexts);
            }

            return arrSize;
        }

        if (obj is CrdtMetadata metadata)
        {
            return EstimateCrdtMetadataSize(metadata, visited, aotContexts);
        }

        if (obj is IDictionary dict)
        {
            // Dictionary object + internal Entry[] and buckets[] overhead (~48 bytes per entry in 64-bit CLR)
            long dictSize = 80L + (dict.Count * 48L);
            foreach (DictionaryEntry entry in dict)
            {
                dictSize += EstimateGraphSize(entry.Key, visited, aotContexts);
                dictSize += EstimateGraphSize(entry.Value, visited, aotContexts);
            }

            return dictSize;
        }

        if (obj is ICollection collection)
        {
            long colSize = 48L + (collection.Count * 16L);
            foreach (var item in collection)
            {
                colSize += EstimateGraphSize(item, visited, aotContexts);
            }

            return colSize;
        }

        var aotTypeInfo = ResolveAotTypeInfo(type, aotContexts);
        if (aotTypeInfo != null)
        {
            long objSize = type.IsValueType ? 0L : 24L;
            foreach (var prop in aotTypeInfo.Properties.Values)
            {
                if (!prop.CanRead || prop.Getter == null)
                {
                    continue;
                }

                if (IsSimpleValueType(prop.PropertyType))
                {
                    objSize += GetPrimitiveOrSimpleValueSize(prop.PropertyType);
                }
                else
                {
                    objSize += type.IsValueType ? 0L : 8L;
                    var val = prop.Getter(obj);
                    objSize += EstimateGraphSize(val, visited, aotContexts);
                }
            }

            return Math.Max(objSize, type.IsValueType ? 8L : 24L);
        }

        if (obj is IEnumerable enumerable)
        {
            long enumSize = 48L;
            foreach (var item in enumerable)
            {
                enumSize += 16L + EstimateGraphSize(item, visited, aotContexts);
            }

            return enumSize;
        }

        return 32L;
    }

    private static long EstimateCrdtMetadataSize(CrdtMetadata metadata, HashSet<object> visited, CrdtAotContext[] aotContexts)
    {
        // CrdtMetadata instance + 13 ConcurrentDictionary/List field references
        long size = 128L;

        size += EstimateGraphSize(metadata.States, visited, aotContexts);
        size += EstimateGraphSize(metadata.VersionVector, visited, aotContexts);

        // SeenExceptions is a HashSet<CrdtOperation> (struct of ~64 bytes + string/object refs)
        if (metadata.SeenExceptions != null && visited.Add(metadata.SeenExceptions))
        {
            size += 64L + (metadata.SeenExceptions.Count * 72L);
            foreach (var op in metadata.SeenExceptions)
            {
                size += EstimateStringSize(op.ReplicaId);
                size += EstimateStringSize(op.JsonPath);
                size += EstimateGraphSize(op.Value, visited, aotContexts);
            }
        }

        return size;
    }

    private static CrdtTypeInfo? ResolveAotTypeInfo(Type type, CrdtAotContext[] aotContexts)
    {
        for (int i = 0; i < aotContexts.Length; i++)
        {
            var info = aotContexts[i].GetTypeInfo(type);
            if (info != null)
            {
                return info;
            }
        }

        return null;
    }

    private static bool IsSimpleValueType(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        return underlying.IsPrimitive
            || underlying.IsEnum
            || underlying == typeof(decimal)
            || underlying == typeof(Guid)
            || underlying == typeof(DateTime)
            || underlying == typeof(DateTimeOffset)
            || underlying == typeof(TimeSpan);
    }

    private static long GetPrimitiveOrSimpleValueSize(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        long extraNullable = Nullable.GetUnderlyingType(type) != null ? 4L : 0L;

        if (underlying == typeof(bool) || underlying == typeof(byte) || underlying == typeof(sbyte))
        {
            return 1L + extraNullable;
        }

        if (underlying == typeof(short) || underlying == typeof(ushort) || underlying == typeof(char))
        {
            return 2L + extraNullable;
        }

        if (underlying == typeof(int) || underlying == typeof(uint) || underlying == typeof(float))
        {
            return 4L + extraNullable;
        }

        if (underlying == typeof(long) || underlying == typeof(ulong) || underlying == typeof(double) || underlying == typeof(DateTime) || underlying == typeof(TimeSpan) || underlying == typeof(nint) || underlying == typeof(nuint))
        {
            return 8L + extraNullable;
        }

        if (underlying == typeof(decimal) || underlying == typeof(Guid) || underlying == typeof(DateTimeOffset))
        {
            return 16L + extraNullable;
        }

        if (underlying.IsEnum)
        {
            return GetPrimitiveOrSimpleValueSize(Enum.GetUnderlyingType(underlying)) + extraNullable;
        }

        return 8L + extraNullable;
    }

    private static string ResolveTypeAlias(IServiceProvider sp, string? explicitTypeAlias)
    {
        if (!string.IsNullOrWhiteSpace(explicitTypeAlias))
        {
            return explicitTypeAlias;
        }

        if (typeof(TState) == typeof(CrdtRegistryState))
        {
            return Constants.SystemRegistryDocumentId;
        }

        foreach (var factory in sp.GetServices<IDocumentFactory>())
        {
            if (factory.StateType == typeof(TState))
            {
                return factory.TypeAlias;
            }
        }

        return typeof(TState).Name;
    }

    private string GetStorageDocumentKey()
    {
        if (typeof(TState) == typeof(CrdtRegistryState) || string.IsNullOrWhiteSpace(TypeAlias))
        {
            return DocumentId;
        }

        return CrdtRegistryEntry.GetRegistryKey(TypeAlias, DocumentId);
    }
}