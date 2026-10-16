namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
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
public sealed class DistributedCrdtDocument<TState> : IDistributedCrdtDocument<TState>, IDisposable where TState : class, new()
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
    private readonly ILogger<DistributedCrdtDocument<TState>> logger;
    
    private readonly IEnumerable<IDocumentStateObserver<TState>> stateObservers;
    private readonly IEnumerable<IDocumentPatchObserver> patchObservers;

    private readonly bool activeSyncEnabled;
    private readonly int activeSyncDebounceMs;
    private readonly bool avoidBlindWrites;
    private readonly TState initialState;
    
    private readonly object syncRoot = new();
    private CrdtDocument<TState> currentDocument;
    
    private readonly Channel<PooledDocumentCommand<TState>>? commandChannel;
    private readonly ConcurrentQueue<PooledDocumentCommand<TState>> commandPool = new();
    private readonly CancellationTokenSource disposeCts = new();
    
    private readonly object debounceSyncRoot = new();
    private readonly List<CrdtOperation>? debounceBuffer;
    private readonly Timer? debounceTimer;

    private volatile bool isDirty;
    private CrdtDocument<TState>? lastSavedDocument;

    private readonly Meter meter;
    private readonly Counter<long> patchAppliedCounter;
    private readonly Counter<long> snapshotsMergedCounter;
    private readonly Counter<long> checkPointSavedCounter;

    private readonly Counter<long> intentsAppliedCounter;
    private readonly Counter<long> intentsFailedCounter;
    private readonly Counter<long> replicaEvictionsCounter;
    private readonly Counter<long> stateResetsCounter;

    // Channel metrics
    private readonly Counter<long> commandsEnqueuedCounter;
    private readonly Counter<long> commandsProcessedCounter;
    private readonly Counter<long> commandFailuresCounter;

    /// <inheritdoc />
    public string ShardId { get; }

    /// <inheritdoc />
    public string DocumentId { get; }

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
        IEnumerable<IDocumentStateObserver<TState>> stateObservers,
        IEnumerable<IDocumentPatchObserver> patchObservers,
        IMeterFactory? meterFactory = null)
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
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        
        this.stateObservers = stateObservers ?? throw new ArgumentNullException(nameof(stateObservers));
        this.patchObservers = patchObservers ?? throw new ArgumentNullException(nameof(patchObservers));

        this.activeSyncEnabled = options.Value.ActiveSyncEnabled;
        this.activeSyncDebounceMs = options.Value.ActiveSyncDebounceMilliseconds;
        this.avoidBlindWrites = options.Value.AvoidBlindCheckpointWrites;
        this.initialState = initialState;

        var metadata = metadataManager.Initialize(initialState);
        currentDocument = new CrdtDocument<TState>(initialState, metadata);

        DocumentId = documentIdProvider.GetDocumentId(initialState).ToString() ?? string.Empty;
        ShardId = initialState is CrdtRegistryState
            ? DocumentId
            : shardIdProvider.GetShardId(currentDocument) ?? string.Empty;

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.DistributedCrdtDocument") ?? new Meter("Ama.Enterprise.CRDT.Distributed.DistributedCrdtDocument");
        this.patchAppliedCounter = this.meter.CreateCounter<long>("crdt.document.patches_applied", "patches", "Total local patches applied");
        this.snapshotsMergedCounter = this.meter.CreateCounter<long>("crdt.document.snapshots_merged", "snapshots", "Total incoming full snapshots merged");
        this.checkPointSavedCounter = this.meter.CreateCounter<long>("crdt.document.checkpoints_saved", "checkpoints", "Total underlying storage checkpoints saved");
        
        this.intentsAppliedCounter = this.meter.CreateCounter<long>("crdt.document.intents_applied", "intents", "Total operation intents applied to this document");
        this.intentsFailedCounter = this.meter.CreateCounter<long>("crdt.document.intents_failed", "intents", "Total operation intents that failed to apply");
        this.replicaEvictionsCounter = this.meter.CreateCounter<long>("crdt.document.replica_evictions", "evictions", "Total replicas evicted from this document state");
        this.stateResetsCounter = this.meter.CreateCounter<long>("crdt.document.state_resets", "resets", "Total local state resets performed on this document");

        this.commandsEnqueuedCounter = this.meter.CreateCounter<long>("crdt.document.channel.commands_enqueued", "commands", "Total document commands enqueued");
        this.commandsProcessedCounter = this.meter.CreateCounter<long>("crdt.document.channel.commands_processed", "commands", "Total document commands processed");
        this.commandFailuresCounter = this.meter.CreateCounter<long>("crdt.document.channel.command_failures", "errors", "Total document command processing errors");

        if (this.activeSyncEnabled && this.activeSyncDebounceMs > 0)
        {
            this.debounceBuffer = new List<CrdtOperation>();
            this.debounceTimer = new Timer(OnDebounceTimerFired, null, Timeout.Infinite, Timeout.Infinite);
        }

        var capacity = options.Value.ChannelCapacity;
        if (capacity > 0)
        {
            this.commandChannel = Channel.CreateBounded<PooledDocumentCommand<TState>>(new BoundedChannelOptions(capacity)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });
        }
        else
        {
            this.commandChannel = Channel.CreateUnbounded<PooledDocumentCommand<TState>>(new UnboundedChannelOptions 
            { 
                SingleReader = true, 
                SingleWriter = false 
            });
        }

        _ = this.meter.CreateObservableGauge("crdt.document.channel.queue_length", 
            () => new Measurement<int>(this.commandChannel.Reader.CanCount ? this.commandChannel.Reader.Count : 0, new KeyValuePair<string, object?>("document_id", this.DocumentId)), 
            "commands", "Current number of pending document commands");
            
        _ = this.meter.CreateObservableGauge("crdt.document.channel.pool_size", 
            () => new Measurement<int>(this.commandPool.Count, new KeyValuePair<string, object?>("document_id", this.DocumentId)), 
            "commands", "Current size of the document command object pool");

        _ = Task.Run(ProcessChannelAsync);
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
        if (commandChannel == null) return;

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.Initialize;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public async Task<bool> ApplyRemoteIntentAsync(IOperationIntent intent, string targetProperty, IAsyncCrdtPatcher patcher, IntentDeliveryMode deliveryMode = IntentDeliveryMode.Acknowledged, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(intent);
        ArgumentNullException.ThrowIfNull(patcher);

        if (commandChannel == null) return false;

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            var ffCmd = GetCommand();
            ffCmd.Type = DocumentCommandType.ApplyRemoteIntent;
            ffCmd.Intent = intent;
            ffCmd.TargetProperty = targetProperty;
            ffCmd.Patcher = patcher;
            ffCmd.IsFireAndForget = true;
            ffCmd.CancellationToken = CancellationToken.None;

            if (commandChannel.Writer.TryWrite(ffCmd))
            {
                commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
                return true;
            }

            await commandChannel.Writer.WriteAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            return true;
        }

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.ApplyRemoteIntent;
        cmd.Intent = intent;
        cmd.TargetProperty = targetProperty;
        cmd.Patcher = patcher;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            await cmd.ExecuteAsync().ConfigureAwait(false);
            return true;
        }
        finally
        {
            ReturnCommand(cmd);
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
        if (commandChannel == null) return;

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            var ffCmd = GetCommand();
            ffCmd.Type = DocumentCommandType.ApplyPatch;
            ffCmd.Patch = patch;
            ffCmd.IsFireAndForget = true;
            ffCmd.CancellationToken = CancellationToken.None;

            if (commandChannel.Writer.TryWrite(ffCmd))
            {
                commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
                return;
            }

            await commandChannel.Writer.WriteAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            return;
        }

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.ApplyPatch;
        cmd.Patch = patch;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
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
        if (commandChannel == null) return;

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            var ffCmd = GetCommand();
            ffCmd.Type = DocumentCommandType.ApplyRemotePatch;
            ffCmd.Patch = patch;
            ffCmd.IsFireAndForget = true;
            ffCmd.CancellationToken = CancellationToken.None;

            if (commandChannel.Writer.TryWrite(ffCmd))
            {
                commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
                return;
            }

            await commandChannel.Writer.WriteAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            return;
        }

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.ApplyRemotePatch;
        cmd.Patch = patch;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public async Task ApplyJournaledPatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default)
    {
        if (commandChannel == null) return;

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.ApplyJournaledPatch;
        cmd.Patch = patch;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public async Task<CrdtSnapshotDataDto> GetSnapshotDataAsync(CancellationToken cancellationToken = default)
    {
        if (commandChannel == null)
        {
            CrdtDocument<TState> currentDoc;
            lock (syncRoot) { currentDoc = currentDocument; }
            var snapshotData = serializer.SerializeToBytes(currentDoc);
            return new CrdtSnapshotDataDto(snapshotData, GetLocalState());
        }

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.GetSnapshotData;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            await cmd.ExecuteAsync().ConfigureAwait(false);
            return new CrdtSnapshotDataDto(cmd.ResultSnapshotData!, cmd.ResultGlobalState!);
        }
        finally
        {
            ReturnCommand(cmd);
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
        if (snapshotData == null || snapshotData.Length == 0 || commandChannel == null) return;
        ArgumentNullException.ThrowIfNull(globalState);

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            var ffCmd = GetCommand();
            ffCmd.Type = DocumentCommandType.MergeSnapshot;
            ffCmd.SnapshotData = snapshotData;
            ffCmd.GlobalState = globalState;
            ffCmd.IsFireAndForget = true;
            ffCmd.CancellationToken = CancellationToken.None;

            if (commandChannel.Writer.TryWrite(ffCmd))
            {
                commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
                return;
            }

            await commandChannel.Writer.WriteAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            return;
        }

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.MergeSnapshot;
        cmd.SnapshotData = snapshotData;
        cmd.GlobalState = globalState;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public async Task CheckpointAsync(CancellationToken cancellationToken = default)
    {
        if (commandChannel == null) return;

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.Checkpoint;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
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

        if (commandChannel == null) return;

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            var ffCmd = GetCommand();
            ffCmd.Type = DocumentCommandType.EvictReplica;
            ffCmd.ReplicaIdToEvict = replicaId;
            ffCmd.IsFireAndForget = true;
            ffCmd.CancellationToken = CancellationToken.None;

            if (commandChannel.Writer.TryWrite(ffCmd))
            {
                commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
                return;
            }

            await commandChannel.Writer.WriteAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            return;
        }

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.EvictReplica;
        cmd.ReplicaIdToEvict = replicaId;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public async Task ResetLocalStateAsync(string oldReplicaId, CancellationToken cancellationToken = default)
    {
        if (commandChannel == null) return;

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.ResetLocalState;
        cmd.OldReplicaId = oldReplicaId;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        debounceTimer?.Dispose();
        disposeCts.Cancel();
        commandChannel?.Writer.TryComplete();
        disposeCts.Dispose();
        meter.Dispose();
    }

    private static Expression<Func<TState, object>> GetOrAddPropertySelector(string? targetProperty)
    {
        var key = targetProperty ?? string.Empty;
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
                await observer.OnPatchGeneratedAsync(DocumentId, patch, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error notifying patch observer for document {DocumentId}.", DocumentId);
            }
        }
    }

    private PooledDocumentCommand<TState> GetCommand() => commandPool.TryDequeue(out var cmd) ? cmd : new PooledDocumentCommand<TState>();

    private void ReturnCommand(PooledDocumentCommand<TState> cmd)
    {
        cmd.Reset();
        commandPool.Enqueue(cmd);
    }

    private async Task ProcessChannelAsync()
    {
        if (commandChannel == null) return;
        
        try
        {
            await foreach (var cmd in commandChannel.Reader.ReadAllAsync(disposeCts.Token).ConfigureAwait(false))
            {
                try
                {
                    cmd.CancellationToken.ThrowIfCancellationRequested();

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

                    commandsProcessedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));

                    if (cmd.IsFireAndForget)
                    {
                        ReturnCommand(cmd);
                    }
                    else
                    {
                        cmd.SetResult();
                    }
                }
                catch (OperationCanceledException)
                {
                    if (cmd.IsFireAndForget)
                    {
                        ReturnCommand(cmd);
                    }
                    else
                    {
                        cmd.SetException(new OperationCanceledException("Document operation cancelled."));
                    }
                }
                catch (Exception ex)
                {
                    commandFailuresCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
                    if (cmd.IsFireAndForget)
                    {
                        ReturnCommand(cmd);
                    }
                    else
                    {
                        cmd.SetException(ex);
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Document background channel failed.");
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

            var storedDoc = await storage.LoadDocumentAsync<TState>(ShardId, DocumentId, cancellationToken).ConfigureAwait(false);
            
            if (storedDoc == null)
            {
                storedDoc = await storage.LoadOrphanedDocumentAsync<TState>(ShardId, DocumentId, cancellationToken).ConfigureAwait(false);
                if (storedDoc != null)
                {
                    logger.LogInformation("Loaded orphaned state for document {DocumentId} from storage.", DocumentId);
                }
            }

            if (storedDoc != null)
            {
                lock (syncRoot)
                {
                    currentDocument = storedDoc.Value;

                    if (avoidBlindWrites)
                    {
                        lastSavedDocument = storedDoc.Value;
                    }
                }

                await NotifyStateChangedAsync(cancellationToken).ConfigureAwait(false);
                logger.LogInformation("Loaded initial state for document {DocumentId} from storage.", DocumentId);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load initial state for document {DocumentId} from storage.", DocumentId);
            throw;
        }
    }

    private async Task ProcessApplyRemoteIntentInternalAsync(PooledDocumentCommand<TState> cmd)
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

                try
                {
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
                }
                catch (Exception ex)
                {
                    throw;
                }
            }, cmd.CancellationToken).ConfigureAwait(false);

            patchAppliedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            intentsAppliedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));

            await NotifyStateChangedAsync(cmd.CancellationToken).ConfigureAwait(false);
            await TriggerActiveSyncBroadcastAsync(generatedPatch, cmd.CancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            intentsFailedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            logger.LogError(ex, "Failed to apply forwarded intent to local document {DocumentId}.", DocumentId);
            throw;
        }
    }

    private async Task ProcessApplyPatchInternalAsync(PooledDocumentCommand<TState> cmd)
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

        patchAppliedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
        await NotifyStateChangedAsync(cmd.CancellationToken).ConfigureAwait(false);
        await TriggerActiveSyncBroadcastAsync(cmd.Patch!.Value, cmd.CancellationToken).ConfigureAwait(false);
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
            _ = Task.Run(() => NotifyPatchGeneratedAsync(batchedPatch, CancellationToken.None));
        }
    }

    private async Task ProcessApplyRemotePatchInternalAsync(PooledDocumentCommand<TState> cmd)
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

        patchAppliedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
        await NotifyStateChangedAsync(cmd.CancellationToken).ConfigureAwait(false);
    }

    private async Task ProcessApplyJournaledPatchInternalAsync(PooledDocumentCommand<TState> cmd)
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

        patchAppliedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
        await NotifyStateChangedAsync(cmd.CancellationToken).ConfigureAwait(false);
    }

    private async Task ProcessGetSnapshotDataInternalAsync(PooledDocumentCommand<TState> cmd)
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

    private async Task ProcessMergeSnapshotInternalAsync(PooledDocumentCommand<TState> cmd)
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

                if (cmd.GlobalState != null)
                {
                    var activeShardDocIds = GetActiveDocumentIdsForShard();
                    versionVectorManager.RecordDocumentSnapshotMerge(ShardId, DocumentId, cmd.GlobalState, activeShardDocIds);
                }
            }, cmd.CancellationToken).ConfigureAwait(false);

            snapshotsMergedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
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
        var orchestrator = serviceProvider.GetService<ICrdtDocumentOrchestrator>();
        if (orchestrator == null)
        {
            return [DocumentId];
        }

        try
        {
            var docs = orchestrator.GetActiveDocuments();
            var list = new List<string>();
            foreach (var doc in docs)
            {
                if (string.Equals(doc.ShardId, ShardId, StringComparison.Ordinal))
                {
                    list.Add(doc.DocumentId);
                }
            }

            if (!list.Contains(DocumentId))
            {
                list.Add(DocumentId);
            }

            return list;
        }
        catch (InvalidOperationException)
        {
            return [DocumentId];
        }
    }

    private async Task ProcessCheckpointInternalAsync(PooledDocumentCommand<TState> cmd)
    {
        CrdtDocument<TState> currentDoc;
        lock (syncRoot)
        {
            if (avoidBlindWrites && !isDirty) return;

            currentDoc = currentDocument;

            if (avoidBlindWrites && lastSavedDocument.HasValue)
            {
                if (EqualityComparer<CrdtDocument<TState>>.Default.Equals(lastSavedDocument.Value, currentDoc))
                {
                    isDirty = false;
                    return; 
                }
            }

            isDirty = false; 
        }

        try
        {
            await storage.SaveDocumentAsync(ShardId, DocumentId, currentDoc, cmd.CancellationToken).ConfigureAwait(false);
            
            if (avoidBlindWrites)
            {
                lock (syncRoot)
                {
                    lastSavedDocument = currentDoc;
                }
            }

            checkPointSavedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            logger.LogDebug("Saved checkpoint to storage for document {DocumentId}.", DocumentId);
        }
        catch (Exception)
        {
            lock (syncRoot) { isDirty = true; }
            throw;
        }
    }

    private async Task ProcessEvictReplicaInternalAsync(PooledDocumentCommand<TState> cmd)
    {
        lock (syncRoot)
        {
            metadataManager.EvictReplica(currentDocument, cmd.ReplicaIdToEvict!);
            isDirty = true;
        }
        
        replicaEvictionsCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
        await NotifyStateChangedAsync(cmd.CancellationToken).ConfigureAwait(false);
    }

    private async Task ProcessResetLocalStateInternalAsync(CancellationToken cancellationToken)
    {
        lock (syncRoot)
        {
            var metadata = metadataManager.Initialize(initialState);
            currentDocument = new CrdtDocument<TState>(initialState, metadata);
            isDirty = true;
        }
        
        stateResetsCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
        await NotifyStateChangedAsync(cancellationToken).ConfigureAwait(false);
        logger.LogInformation("Performed a hard reset for document {DocumentId}.", DocumentId);
    }
}