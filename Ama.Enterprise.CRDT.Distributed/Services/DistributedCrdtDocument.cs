namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.CRDT.Services.GarbageCollection;
using Ama.CRDT.Services.Pooling;
using Ama.CRDT.Services.Providers;
using Ama.CRDT.Services.Serialization;
using Ama.CRDT.Services.Versioning;
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
    
    private readonly IDocumentStateObserver<TState>[] stateObservers;
    private readonly IDocumentPatchObserver[] patchObservers;

    private readonly bool activeSyncEnabled;
    private readonly int activeSyncDebounceMs;
    private readonly bool avoidBlindWrites;
    private readonly TState initialState;
    
    private readonly object syncRoot = new();
    private readonly ICrdtDocumentBuffer<TState> documentBuffer;
    private int hasCompletedInitialStorageLoad;
    
    private readonly object debounceSyncRoot = new();
    private List<CrdtOperation>? debounceBuffer;
    private Timer? debounceTimer;

    private volatile bool isDirty;
    private volatile bool isDisposed;

    private CrdtAotContext[]? cachedAotContexts;

    /// <inheritdoc />
    public string ScopeReplicaId { get; }

    /// <inheritdoc />
    public string ShardId { get; }

    /// <inheritdoc />
    public string DocumentId { get; }

    /// <inheritdoc />
    public string TypeAlias { get; }

    /// <inheritdoc />
    public CrdtDocumentResidencyMode ResidencyMode => documentBuffer.Mode;

    /// <inheritdoc />
    public CrdtDocument<TState> CurrentDocument => ReadFullDocument();

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
        ICrdtStringPool? stringPool = null,
        CrdtDocumentResidencyMode? residencyMode = null)
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
        
        ArgumentNullException.ThrowIfNull(stateObservers);
        ArgumentNullException.ThrowIfNull(patchObservers);

        this.stateObservers = stateObservers as IDocumentStateObserver<TState>[] ?? new List<IDocumentStateObserver<TState>>(stateObservers).ToArray();
        this.patchObservers = patchObservers as IDocumentPatchObserver[] ?? new List<IDocumentPatchObserver>(patchObservers).ToArray();

        this.activeSyncEnabled = options.Value.ActiveSyncEnabled;
        this.activeSyncDebounceMs = options.Value.ActiveSyncDebounceMilliseconds;
        this.avoidBlindWrites = options.Value.AvoidBlindCheckpointWrites;
        this.initialState = initialState;

        var metadata = metadataManager.Initialize(initialState);
        var initialDocument = new CrdtDocument<TState>(initialState, metadata);

        var scopeContext = serviceProvider.GetService<CrdtScopeContext>();
        var replicaContext = serviceProvider.GetService<ReplicaContext>();
        var resolvedScopeReplicaId = !string.IsNullOrEmpty(scopeContext?.ReplicaId)
            ? scopeContext.ReplicaId
            : (replicaContext?.ReplicaId ?? string.Empty);

        ScopeReplicaId = this.stringPool.GetOrAdd(resolvedScopeReplicaId);
        DocumentId = this.stringPool.GetOrAdd(documentIdProvider.GetDocumentId(initialState).ToString() ?? string.Empty);
        TypeAlias = this.stringPool.GetOrAdd(ResolveTypeAlias(serviceProvider, typeAlias));
        ShardId = this.stringPool.GetOrAdd(initialState is CrdtRegistryState
            ? DocumentId
            : shardIdProvider.GetShardId(initialDocument) ?? string.Empty);

        var effectiveResidencyMode = typeof(TState) == typeof(CrdtRegistryState)
            ? CrdtDocumentResidencyMode.Unpacked
            : (residencyMode ?? options.Value.DefaultDocumentResidencyMode);

        this.documentBuffer = CreateDocumentBuffer(effectiveResidencyMode, initialDocument, metadataManager, serializer);
    }

    /// <inheritdoc />
    public TState ReadData()
    {
        lock (syncRoot)
        {
            return documentBuffer.GetData();
        }
    }

    /// <inheritdoc />
    public CrdtDocument<TState> ReadFullDocument()
    {
        lock (syncRoot)
        {
            return documentBuffer.UnpackFullDocument();
        }
    }

    /// <inheritdoc />
    public ValueTask<TState> GetDataAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<TState>(ReadData());
    }
    
    /// <inheritdoc />
    public ValueTask<CrdtDocument<TState>> GetDocumentAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<CrdtDocument<TState>>(ReadFullDocument());
    }

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (isDisposed) return;

        if (commandPipeline.IsCurrentThreadPipelineProcessor)
        {
            await ProcessInitializeInternalAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

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
    public Task<bool> ApplyIntentAsync(
        IOperationIntent intent,
        string targetProperty,
        IntentDeliveryMode deliveryMode = IntentDeliveryMode.Acknowledged,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(intent);

        var patcher = serviceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        return ApplyRemoteIntentAsync(intent, targetProperty, patcher, deliveryMode, cancellationToken);
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
        if (isDisposed || commandPipeline.IsCurrentThreadPipelineProcessor)
        {
            byte[] snapshotData;
            lock (syncRoot)
            {
                snapshotData = documentBuffer.GetSerializedBytes();
            }

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
        return MergeSnapshotAsync(snapshotData, globalState, null, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public Task MergeSnapshotAsync(byte[] snapshotData, ShardVersionVectorMap globalState, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        return MergeSnapshotAsync(snapshotData, globalState, null, deliveryMode, cancellationToken);
    }

    /// <inheritdoc />
    public async Task MergeSnapshotAsync(
        byte[] snapshotData,
        ShardVersionVectorMap globalState,
        IReadOnlyList<string>? expectedShardDocumentIds,
        IntentDeliveryMode deliveryMode = IntentDeliveryMode.Acknowledged,
        CancellationToken cancellationToken = default)
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
            ffCmd.ExpectedShardDocumentIds = expectedShardDocumentIds;

            await commandPipeline.EnqueueFireAndForgetAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            return;
        }

        var cmd = commandPipeline.RentCommand();
        cmd.Target = this;
        cmd.Type = DocumentCommandType.MergeSnapshot;
        cmd.SnapshotData = snapshotData;
        cmd.GlobalState = globalState;
        cmd.ExpectedShardDocumentIds = expectedShardDocumentIds;
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
    public Task CompactMetadataAsync(CancellationToken cancellationToken = default)
    {
        return CompactMetadataAsync(IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task CompactMetadataAsync(IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (isDisposed) return;

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            var ffCmd = commandPipeline.RentCommand();
            ffCmd.Target = this;
            ffCmd.Type = DocumentCommandType.CompactMetadata;

            await commandPipeline.EnqueueFireAndForgetAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            return;
        }

        var cmd = commandPipeline.RentCommand();
        cmd.Target = this;
        cmd.Type = DocumentCommandType.CompactMetadata;
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
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        List<CrdtOperation>? opsToBroadcast = null;

        lock (debounceSyncRoot)
        {
            if (debounceTimer is not null)
            {
                debounceTimer.Dispose();
                debounceTimer = null;
            }

            if (debounceBuffer != null && debounceBuffer.Count > 0)
            {
                opsToBroadcast = debounceBuffer;
                debounceBuffer = null;
            }
        }

        if (opsToBroadcast != null && opsToBroadcast.Count > 0)
        {
            var batchedPatch = new CrdtPatch(opsToBroadcast);
            await NotifyPatchGeneratedAsync(batchedPatch, cancellationToken).ConfigureAwait(false);
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

        var debounceCapacity = -1;
        lock (debounceSyncRoot)
        {
            if (debounceBuffer != null)
            {
                debounceCapacity = debounceBuffer.Capacity;
            }
        }

        var estimator = serviceProvider.GetService<ICrdtMemoryEstimator>() ?? new CrdtMemoryEstimator();
        var localState = GetLocalState();
        var aotContexts = GetAotContexts();

        DocumentMemoryFootprintDto footprint;
        lock (syncRoot)
        {
            footprint = documentBuffer.EstimateFootprint(
                estimator,
                DocumentId,
                ShardId,
                debounceCapacity,
                localState,
                aotContexts);
        }

        return new ValueTask<DocumentMemoryFootprintDto>(footprint);
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
            case DocumentCommandType.CompactMetadata:
                await ProcessCompactMetadataInternalAsync(cmd.CancellationToken).ConfigureAwait(false);
                break;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        isDisposed = true;
        lock (debounceSyncRoot)
        {
            debounceTimer?.Dispose();
            debounceTimer = null;
            debounceBuffer = null;
        }

        lock (syncRoot)
        {
            documentBuffer.Dispose();
        }
    }

    private static ICrdtDocumentBuffer<TState> CreateDocumentBuffer(
        CrdtDocumentResidencyMode residencyMode,
        CrdtDocument<TState> initialDocument,
        ICrdtMetadataManager metadataManager,
        ICrdtSerializer serializer)
    {
        return residencyMode switch
        {
            CrdtDocumentResidencyMode.PackedMetadata => new PackedMetadataDocumentBuffer<TState>(initialDocument, metadataManager, serializer),
            CrdtDocumentResidencyMode.FullySerialized => new FullySerializedDocumentBuffer<TState>(initialDocument, serializer),
            _ => new UnpackedDocumentBuffer<TState>(initialDocument, serializer)
        };
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
        for (int i = 0; i < stateObservers.Length; i++)
        {
            try
            {
                await stateObservers[i].OnStateChangedAsync(DocumentId, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error notifying state observer for document {DocumentId}.", DocumentId);
            }
        }
    }

    private async Task NotifyPatchGeneratedAsync(CrdtPatch patch, CancellationToken cancellationToken)
    {
        for (int i = 0; i < patchObservers.Length; i++)
        {
            try
            {
                await patchObservers[i].OnPatchGeneratedAsync(DocumentId, TypeAlias, patch, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error notifying patch observer for document {DocumentId}.", DocumentId);
            }
        }
    }

    private async Task ProcessInitializeInternalAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref hasCompletedInitialStorageLoad, 1) != 0)
        {
            return;
        }

        try
        {
            var isLocalShard = await distributionManager.IsShardLocalAsync(ShardId, cancellationToken).ConfigureAwait(false);
            if (!isLocalShard)
            {
                logger.LogWarning("Document initialized for a remote shard {ShardId}. The Orchestrator should handle remote requests.", this.ShardId);
            }

            var isMemoryStorage = storage is MemoryCrdtStorage
                || (storage is BufferedDistributedCrdtStorageDecorator decorator && decorator.InnerStorage is MemoryCrdtStorage);

            var storageDocKey = GetStorageDocumentKey();
            var loadedFromMigratedShard = false;

            byte[]? rawStoredBytes = null;
            CrdtDocument<TState>? localStoredDoc = null;

            if (ResidencyMode == CrdtDocumentResidencyMode.FullySerialized)
            {
                rawStoredBytes = await storage.LoadSerializedDocumentAsync(ShardId, storageDocKey, cancellationToken).ConfigureAwait(false);
                if ((rawStoredBytes == null || rawStoredBytes.Length == 0) && !string.Equals(storageDocKey, DocumentId, StringComparison.Ordinal))
                {
                    rawStoredBytes = await storage.LoadSerializedDocumentAsync(ShardId, DocumentId, cancellationToken).ConfigureAwait(false);
                }

                if (rawStoredBytes == null || rawStoredBytes.Length == 0)
                {
                    rawStoredBytes = await storage.LoadSerializedDocumentFromAnyShardAsync(storageDocKey, cancellationToken).ConfigureAwait(false);
                    if ((rawStoredBytes == null || rawStoredBytes.Length == 0) && !string.Equals(storageDocKey, DocumentId, StringComparison.Ordinal))
                    {
                        rawStoredBytes = await storage.LoadSerializedDocumentFromAnyShardAsync(DocumentId, cancellationToken).ConfigureAwait(false);
                    }

                    if (rawStoredBytes is { Length: > 0 })
                    {
                        loadedFromMigratedShard = true;
                        logger.LogInformation("Loaded serialized state for document {DocumentId} ({TypeAlias}) from a previous shard mapping into current shard {ShardId}.", DocumentId, TypeAlias, ShardId);
                    }
                }
            }

            if (rawStoredBytes == null || rawStoredBytes.Length == 0)
            {
                localStoredDoc = await storage.LoadDocumentAsync<TState>(ShardId, storageDocKey, cancellationToken).ConfigureAwait(false);
                if (localStoredDoc == null && !string.Equals(storageDocKey, DocumentId, StringComparison.Ordinal))
                {
                    localStoredDoc = await storage.LoadDocumentAsync<TState>(ShardId, DocumentId, cancellationToken).ConfigureAwait(false);
                }

                if (localStoredDoc == null)
                {
                    localStoredDoc = await storage.LoadDocumentFromAnyShardAsync<TState>(storageDocKey, cancellationToken).ConfigureAwait(false);
                    if (localStoredDoc == null && !string.Equals(storageDocKey, DocumentId, StringComparison.Ordinal))
                    {
                        localStoredDoc = await storage.LoadDocumentFromAnyShardAsync<TState>(DocumentId, cancellationToken).ConfigureAwait(false);
                    }

                    if (localStoredDoc != null)
                    {
                        loadedFromMigratedShard = true;
                        logger.LogInformation("Loaded state for document {DocumentId} ({TypeAlias}) from a previous shard mapping into current shard {ShardId}.", DocumentId, TypeAlias, ShardId);
                    }
                }
            }

            CrdtDocument<TState>? orphanedDoc = null;
            if (!isMemoryStorage)
            {
                orphanedDoc = await storage.LoadOrphanedDocumentAsync<TState>(ShardId, storageDocKey, cancellationToken).ConfigureAwait(false);
                if (orphanedDoc == null && !string.Equals(storageDocKey, DocumentId, StringComparison.Ordinal))
                {
                    orphanedDoc = await storage.LoadOrphanedDocumentAsync<TState>(ShardId, DocumentId, cancellationToken).ConfigureAwait(false);
                }
            }

            if (rawStoredBytes is { Length: > 0 } && orphanedDoc == null)
            {
                var hasUnpersistedLocalMutations = false;
                lock (syncRoot)
                {
                    if (!isDirty)
                    {
                        documentBuffer.StoreSerializedBytes(rawStoredBytes);
                        if (loadedFromMigratedShard)
                        {
                            isDirty = true;
                        }
                    }
                    else
                    {
                        hasUnpersistedLocalMutations = true;
                    }
                }

                if (!hasUnpersistedLocalMutations)
                {
                    CrdtDocument<TState> unpackedStoredDoc;
                    lock (syncRoot)
                    {
                        unpackedStoredDoc = documentBuffer.UnpackFullDocument();
                    }

                    MergeDocumentMetadataIntoShardVersionVector(unpackedStoredDoc, forceMergeForSingleDocumentShard: true);
                }
                else
                {
                    var storedDocFromRaw = serializer.DeserializeFromBytes<CrdtDocument<TState>>(rawStoredBytes);
                    CrdtDocument<TState> currentDoc;
                    lock (syncRoot)
                    {
                        currentDoc = documentBuffer.UnpackFullDocument();
                    }

                    await merger.MergeStateAsync(currentDoc, storedDocFromRaw, cancellationToken).ConfigureAwait(false);
                    MergeDocumentMetadataIntoShardVersionVector(currentDoc, forceMergeForSingleDocumentShard: true);

                    lock (syncRoot)
                    {
                        documentBuffer.StoreFullDocument(currentDoc);
                        isDirty = true;
                    }
                }

                await NotifyStateChangedAsync(cancellationToken).ConfigureAwait(false);
                logger.LogInformation("Loaded initial serialized state for document {DocumentId} ({TypeAlias}) from storage.", DocumentId, TypeAlias);
                return;
            }

            if (rawStoredBytes is { Length: > 0 } && localStoredDoc == null)
            {
                localStoredDoc = serializer.DeserializeFromBytes<CrdtDocument<TState>>(rawStoredBytes);
            }

            CrdtDocument<TState>? storedDoc = null;
            if (localStoredDoc != null && orphanedDoc != null)
            {
                var mergedStoredDoc = localStoredDoc.Value;
                await merger.MergeStateAsync(mergedStoredDoc, orphanedDoc.Value, cancellationToken).ConfigureAwait(false);
                storedDoc = mergedStoredDoc;
                logger.LogInformation("Merged local and orphaned state for document {DocumentId} ({TypeAlias}) from storage.", DocumentId, TypeAlias);
            }
            else if (localStoredDoc != null)
            {
                storedDoc = localStoredDoc;
            }
            else if (orphanedDoc != null)
            {
                storedDoc = orphanedDoc;
                logger.LogInformation("Loaded orphaned state for document {DocumentId} ({TypeAlias}) from storage.", DocumentId, TypeAlias);
            }

            if (storedDoc != null)
            {
                var hasUnpersistedLocalMutations = false;
                lock (syncRoot)
                {
                    if (!isDirty)
                    {
                        documentBuffer.StoreFullDocument(storedDoc.Value);
                        if ((localStoredDoc != null && orphanedDoc != null) || loadedFromMigratedShard)
                        {
                            isDirty = true;
                        }
                    }
                    else
                    {
                        hasUnpersistedLocalMutations = true;
                    }
                }

                if (!hasUnpersistedLocalMutations)
                {
                    MergeDocumentMetadataIntoShardVersionVector(storedDoc.Value, forceMergeForSingleDocumentShard: true);
                }
                else
                {
                    CrdtDocument<TState> currentDoc;
                    lock (syncRoot)
                    {
                        currentDoc = documentBuffer.UnpackFullDocument();
                    }

                    await merger.MergeStateAsync(currentDoc, storedDoc.Value, cancellationToken).ConfigureAwait(false);
                    MergeDocumentMetadataIntoShardVersionVector(currentDoc, forceMergeForSingleDocumentShard: true);

                    lock (syncRoot)
                    {
                        documentBuffer.StoreFullDocument(currentDoc);
                        isDirty = true;
                    }
                }

                await NotifyStateChangedAsync(cancellationToken).ConfigureAwait(false);
                logger.LogInformation("Loaded initial state for document {DocumentId} ({TypeAlias}) from storage.", DocumentId, TypeAlias);
            }
        }
        catch (Exception ex)
        {
            Interlocked.Exchange(ref hasCompletedInitialStorageLoad, 0);
            logger.LogError(ex, "Failed to load initial state for document {DocumentId} ({TypeAlias}) from storage.", DocumentId, TypeAlias);
            throw;
        }
    }

    /// <summary>
    /// Marks the document as initialized for a newly created local document without issuing storage lookup queries.
    /// </summary>
    internal void MarkAsNewlyCreatedLocalDocument()
    {
        Interlocked.Exchange(ref hasCompletedInitialStorageLoad, 1);
    }

    private void MergeDocumentMetadataIntoShardVersionVector(CrdtDocument<TState> doc, bool forceMergeForSingleDocumentShard = false)
    {
        var metadata = doc.Metadata;
        if (metadata?.VersionVector == null || metadata.VersionVector.Count == 0)
        {
            return;
        }

        // Document-level metadata VersionVector tracks per-document clocks (CrdtOperation.Clock),
        // whereas ShardVersionVectors track shard-wide clocks (CrdtOperation.ShardClock).
        // When a shard is re-promoted after demotion (or has a 1:1 document-to-shard mapping where no other
        // active document shares the shard), restoring the document's VersionVector prevents the shard DVV
        // from resetting to empty/0 and causing endless anti-entropy resync loops.
        var isSystemRegistry = typeof(TState) == typeof(CrdtRegistryState) ||
                               string.Equals(DocumentId, Constants.SystemRegistryDocumentId, StringComparison.Ordinal);

        if (!isSystemRegistry)
        {
            if (!forceMergeForSingleDocumentShard)
            {
                versionVectorManager.EnsureShardRegistered(ShardId);
                return;
            }
        }

        var versions = new Dictionary<string, long>(metadata.VersionVector, StringComparer.Ordinal);
        var dots = new Dictionary<string, ISet<long>>(StringComparer.Ordinal);

        if (metadata.SeenExceptions != null)
        {
            foreach (var op in metadata.SeenExceptions)
            {
                if (!string.IsNullOrEmpty(op.ReplicaId) && op.Clock > 0)
                {
                    if (!dots.TryGetValue(op.ReplicaId, out var dotSet))
                    {
                        dotSet = new HashSet<long>();
                        dots[op.ReplicaId] = dotSet;
                    }

                    dotSet.Add(op.Clock);
                }
            }
        }

        var map = new ShardVersionVectorMap
        {
            [ShardId] = new DottedVersionVector(versions, dots)
        };

        versionVectorManager.MergeShardState(ShardId, map);
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
                    docState = documentBuffer.UnpackFullDocument();
                }

                var propertySelector = GetOrAddPropertySelector(cmd.TargetProperty);
                var operation = await cmd.Patcher!.GenerateOperationAsync(docState, propertySelector, cmd.Intent!, cmd.CancellationToken).ConfigureAwait(false);
                generatedPatch = new CrdtPatch([operation]);
                cmd.Patch = generatedPatch;

                var result = syncApplicator.ApplyPatch(docState, generatedPatch);

                lock (syncRoot)
                {
                    documentBuffer.StoreFullDocument(result.Document);
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
            var patchToApply = cmd.Patch!.Value;

            await versionVectorManager.ExecuteShardLockedAsync(ShardId, () =>
            {
                CrdtDocument<TState> currentDoc;
                lock (syncRoot) { currentDoc = documentBuffer.UnpackFullDocument(); }

                var result = syncApplicator.ApplyPatch(currentDoc, patchToApply);

                lock (syncRoot)
                {
                    documentBuffer.StoreFullDocument(result.Document);
                    isDirty = true;
                }

                return Task.CompletedTask;
            }, cmd.CancellationToken).ConfigureAwait(false);

            if (patchToApply.Operations != null && patchToApply.Operations.Count > 0)
            {
                var storageDocKey = GetStorageDocumentKey();
                await storage.AppendAsync(storageDocKey, patchToApply.Operations, cmd.CancellationToken).ConfigureAwait(false);
            }

            telemetry.RecordDocumentPatchApplied(DocumentId);
            await NotifyStateChangedAsync(cmd.CancellationToken).ConfigureAwait(false);
            await TriggerActiveSyncBroadcastAsync(patchToApply, cmd.CancellationToken).ConfigureAwait(false);
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
                    if (isDisposed)
                    {
                        return;
                    }

                    debounceBuffer ??= new List<CrdtOperation>(patch.Operations.Count);
                    debounceBuffer.AddRange(patch.Operations);

                    if (debounceTimer is null)
                    {
                        var restoreFlow = false;
                        try
                        {
                            if (!ExecutionContext.IsFlowSuppressed())
                            {
                                ExecutionContext.SuppressFlow();
                                restoreFlow = true;
                            }

                            debounceTimer = new Timer(OnDebounceTimerFired, null, activeSyncDebounceMs, Timeout.Infinite);
                        }
                        finally
                        {
                            if (restoreFlow)
                            {
                                ExecutionContext.RestoreFlow();
                            }
                        }
                    }
                    else
                    {
                        debounceTimer.Change(activeSyncDebounceMs, Timeout.Infinite);
                    }
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
        List<CrdtOperation>? opsToBroadcast;

        lock (debounceSyncRoot)
        {
            if (debounceTimer is not null)
            {
                debounceTimer.Dispose();
                debounceTimer = null;
            }

            if (debounceBuffer == null || debounceBuffer.Count == 0)
            {
                debounceBuffer = null;
                return;
            }
            
            opsToBroadcast = debounceBuffer;
            debounceBuffer = null;
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
            var incomingOps = cmd.Patch!.Value.Operations;
            if (incomingOps == null || incomingOps.Count == 0)
            {
                return;
            }

            var replicaContext = serviceProvider.GetService<ReplicaContext>();
            var localReplicaId = replicaContext?.ReplicaId;
            var patchToApply = cmd.Patch.Value;

            if (!string.IsNullOrEmpty(localReplicaId))
            {
                List<CrdtOperation>? filteredOps = null;
                var droppedOwnOps = false;

                for (int i = 0; i < incomingOps.Count; i++)
                {
                    var op = incomingOps[i];
                    if (string.Equals(op.ReplicaId, localReplicaId, StringComparison.Ordinal))
                    {
                        if (!droppedOwnOps)
                        {
                            droppedOwnOps = true;
                            filteredOps = new List<CrdtOperation>(incomingOps.Count - 1);
                            for (int j = 0; j < i; j++)
                            {
                                filteredOps.Add(incomingOps[j]);
                            }
                        }
                    }
                    else if (droppedOwnOps)
                    {
                        filteredOps!.Add(op);
                    }
                }

                if (droppedOwnOps)
                {
                    if (filteredOps == null || filteredOps.Count == 0)
                    {
                        return;
                    }

                    patchToApply = new CrdtPatch(filteredOps);
                }
            }

            await versionVectorManager.ExecuteShardLockedAsync(ShardId, () =>
            {
                CrdtDocument<TState> currentDoc;
                lock (syncRoot) { currentDoc = documentBuffer.UnpackFullDocument(); }

                var result = syncApplicator.ApplyPatch(currentDoc, patchToApply);

                lock (syncRoot)
                {
                    documentBuffer.StoreFullDocument(result.Document);
                    isDirty = true;
                }

                return Task.CompletedTask;
            }, cmd.CancellationToken).ConfigureAwait(false);

            var storageDocKey = GetStorageDocumentKey();
            await storage.AppendAsync(storageDocKey, patchToApply.Operations!, cmd.CancellationToken).ConfigureAwait(false);

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
                lock (syncRoot) { currentDoc = documentBuffer.UnpackFullDocument(); }

                var result = syncApplicator.ApplyPatch(currentDoc, cmd.Patch!.Value);

                lock (syncRoot)
                {
                    documentBuffer.StoreFullDocument(result.Document);
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

    private Task ProcessGetSnapshotDataInternalAsync(PooledDocumentCommand cmd)
    {
        try
        {
            byte[] snapshotBytes;
            lock (syncRoot)
            {
                snapshotBytes = documentBuffer.GetSerializedBytes();
            }

            var globalState = GetLocalState();

            cmd.ResultSnapshotData = snapshotBytes;
            cmd.ResultGlobalState = globalState;
            return Task.CompletedTask;
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

            CrdtDocument<TState> currentDoc;
            bool wasAlreadyDirty;
            lock (syncRoot)
            {
                currentDoc = documentBuffer.UnpackFullDocument();
                wasAlreadyDirty = isDirty;
            }

            var stateChanged = !wasAlreadyDirty
                ? HasIncomingSnapshotAdvance(currentDoc.Metadata, snapshotDoc.Metadata, cmd.GlobalState)
                : true;

            if (stateChanged)
            {
                await merger.MergeStateAsync(currentDoc, snapshotDoc, cmd.CancellationToken).ConfigureAwait(false);

                lock (syncRoot)
                {
                    documentBuffer.StoreFullDocument(currentDoc);
                    isDirty = true;
                }
            }

            if (cmd.GlobalState != null && cmd.GlobalState.Count > 0)
            {
                var compositeDocId = GetStorageDocumentKey();
                IReadOnlyList<string> requiredShardDocIds;

                if (cmd.ExpectedShardDocumentIds is { Count: > 0 })
                {
                    if (ContainsOrdinal(cmd.ExpectedShardDocumentIds, compositeDocId))
                    {
                        requiredShardDocIds = cmd.ExpectedShardDocumentIds;
                    }
                    else
                    {
                        var copy = new List<string>(cmd.ExpectedShardDocumentIds.Count + 1);
                        for (int i = 0; i < cmd.ExpectedShardDocumentIds.Count; i++)
                        {
                            copy.Add(cmd.ExpectedShardDocumentIds[i]);
                        }
                        copy.Add(compositeDocId);
                        requiredShardDocIds = copy;
                    }
                }
                else
                {
                    // When a snapshot is received without an explicit multi-document batch list (e.g., single-document
                    // ProvideSnapshotAsync or demotion push), advance the shard DVV immediately for this document
                    // rather than blocking indefinitely waiting for other active documents in the shard.
                    requiredShardDocIds = [compositeDocId];
                }

                versionVectorManager.RecordDocumentSnapshotMerge(ShardId, compositeDocId, cmd.GlobalState, requiredShardDocIds);
            }
            else
            {
                MergeDocumentMetadataIntoShardVersionVector(currentDoc);
            }

            telemetry.RecordDocumentSnapshotMerged(DocumentId);
            if (stateChanged)
            {
                await NotifyStateChangedAsync(cmd.CancellationToken).ConfigureAwait(false);
                logger.LogInformation("Merged full state snapshot for document {DocumentId}.", DocumentId);
            }
            else
            {
                logger.LogDebug("Merged redundant state snapshot for document {DocumentId} without new causal state changes.", DocumentId);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to merge snapshot for document {DocumentId}.", DocumentId);
            throw;
        }
    }

    private bool HasIncomingSnapshotAdvance(CrdtMetadata? localMeta, CrdtMetadata? incomingMeta, ShardVersionVectorMap? incomingGlobalState)
    {
        if (incomingMeta == null)
        {
            return false;
        }

        if (localMeta == null)
        {
            return true;
        }

        var localVector = CreateDottedVersionVectorFromMetadata(localMeta);
        var incomingVector = CreateDottedVersionVectorFromMetadata(incomingMeta);

        if (IsVersionVectorEmpty(incomingVector))
        {
            if (IsVersionVectorEmpty(localVector))
            {
                // If both document-level version vectors are empty (e.g. a document initialized directly with data
                // via metadataManager.Initialize without applying incremental operations), check if the incoming
                // shard-level DVV carries any positive causal versions before skipping the merge.
                if (incomingGlobalState != null)
                {
                    if (incomingGlobalState.TryGetValue(ShardId, out var shardDvv) && !IsVersionVectorEmpty(shardDvv))
                    {
                        return true;
                    }

                    foreach (var dvv in incomingGlobalState.Values)
                    {
                        if (!IsVersionVectorEmpty(dvv))
                        {
                            return true;
                        }
                    }
                }

                return false;
            }

            return false;
        }

        var syncService = serviceProvider.GetService<IVersionVectorSyncService>();
        if (syncService != null)
        {
            var requirement = syncService.CalculateRequirement(
                "local",
                localVector,
                "incoming",
                incomingVector,
                ShardId);

            return requirement.IsBehind;
        }

        foreach (var kvp in incomingVector.Versions)
        {
            if (kvp.Value <= 0)
            {
                continue;
            }

            var localVer = localVector.Versions.TryGetValue(kvp.Key, out var lv) ? lv : 0L;
            if (kvp.Value > localVer)
            {
                return true;
            }
        }

        foreach (var kvp in incomingVector.Dots)
        {
            var localVer = localVector.Versions.TryGetValue(kvp.Key, out var lv) ? lv : 0L;
            localVector.Dots.TryGetValue(kvp.Key, out var localDots);

            foreach (var dot in kvp.Value)
            {
                if (dot > localVer && (localDots == null || !localDots.Contains(dot)))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static DottedVersionVector CreateDottedVersionVectorFromMetadata(CrdtMetadata metadata)
    {
        var versions = metadata.VersionVector != null && metadata.VersionVector.Count > 0
            ? new Dictionary<string, long>(metadata.VersionVector, StringComparer.Ordinal)
            : new Dictionary<string, long>(StringComparer.Ordinal);

        var dots = new Dictionary<string, ISet<long>>(StringComparer.Ordinal);
        if (metadata.SeenExceptions != null && metadata.SeenExceptions.Count > 0)
        {
            foreach (var op in metadata.SeenExceptions)
            {
                if (!string.IsNullOrEmpty(op.ReplicaId) && op.Clock > 0)
                {
                    if (!dots.TryGetValue(op.ReplicaId, out var dotSet))
                    {
                        dotSet = new HashSet<long>();
                        dots[op.ReplicaId] = dotSet;
                    }

                    dotSet.Add(op.Clock);
                }
            }
        }

        return new DottedVersionVector(versions, dots);
    }

    private static bool IsVersionVectorEmpty(DottedVersionVector vector)
    {
        if (vector.Versions != null)
        {
            foreach (var v in vector.Versions.Values)
            {
                if (v > 0)
                {
                    return false;
                }
            }
        }

        if (vector.Dots != null)
        {
            foreach (var dots in vector.Dots.Values)
            {
                if (dots != null && dots.Count > 0)
                {
                    return false;
                }
            }
        }

        return true;
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

    private static bool ContainsOrdinal(IReadOnlyList<string> list, string value)
    {
        for (int i = 0; i < list.Count; i++)
        {
            if (string.Equals(list[i], value, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private Task ProcessCheckpointInternalAsync(PooledDocumentCommand cmd)
    {
        return ExecuteCheckpointDirectAsync(cmd.CancellationToken);
    }

    private async Task ExecuteCheckpointDirectAsync(CancellationToken cancellationToken)
    {
        byte[]? snapshotBytes = null;
        var shouldSave = false;

        try
        {
            lock (syncRoot)
            {
                if (avoidBlindWrites && !isDirty)
                {
                    return;
                }

                // Obtain serialized document bytes inside the document lock (O(1) buffer copy in FullySerialized mode).
                snapshotBytes = documentBuffer.GetSerializedBytes();

                isDirty = false;
                shouldSave = true;
            }

            if (!shouldSave)
            {
                return;
            }

            var storageDocKey = GetStorageDocumentKey();
            await storage.SaveSerializedDocumentAsync<TState>(ShardId, storageDocKey, snapshotBytes!, cancellationToken).ConfigureAwait(false);

            telemetry.RecordDocumentCheckpointSaved(DocumentId);
            logger.LogDebug("Saved checkpoint to storage for document {DocumentId}.", DocumentId);
        }
        catch (TaskCanceledException)
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
            lock (syncRoot)
            {
                var currentDoc = documentBuffer.UnpackFullDocument();
                metadataManager.EvictReplica(currentDoc, cmd.ReplicaIdToEvict!);
                documentBuffer.StoreFullDocument(currentDoc);
                isDirty = true;
            }

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
            lock (syncRoot)
            {
                var metadata = metadataManager.Initialize(initialState);
                var resetDoc = new CrdtDocument<TState>(initialState, metadata);
                documentBuffer.StoreFullDocument(resetDoc);
                isDirty = true;
            }

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

    private Task ProcessCompactMetadataInternalAsync(CancellationToken cancellationToken)
    {
        try
        {
            var policyFactories = serviceProvider.GetServices<ICompactionPolicyFactory>();

            lock (syncRoot)
            {
                CrdtDocument<TState>? currentDoc = null;
                var compactedAny = false;
                foreach (var factory in policyFactories)
                {
                    currentDoc ??= documentBuffer.UnpackFullDocument();
                    var policy = factory.CreatePolicy();
                    metadataManager.Compact(currentDoc.Value, policy);
                    compactedAny = true;
                }

                if (compactedAny && currentDoc.HasValue)
                {
                    documentBuffer.StoreFullDocument(currentDoc.Value);
                    isDirty = true;
                }
            }

            telemetry.RecordDocumentMetadataCompacted(DocumentId);
            logger.LogTrace("Compacted CRDT metadata for document {DocumentId}.", DocumentId);
            return Task.CompletedTask;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to compact metadata for document {DocumentId}.", DocumentId);
            throw;
        }
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

        var registry = sp.GetService<IDocumentFactoryRegistry>();
        if (registry != null && registry.TryGetPrimaryAlias(typeof(TState), out var registeredAlias))
        {
            return registeredAlias;
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