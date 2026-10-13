namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Providers;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Generic document manager responsible for maintaining consistency mapping isolated local causal models for a specific CRDT tree.
/// </summary>
public sealed class DistributedCrdtDocument<TState> : IDistributedCrdtDocument<TState>, IDisposable where TState : class, new()
{
    private readonly ReplicaContext replicaContext;
    private readonly IAsyncCrdtApplicator applicator;
    private readonly ICrdtApplicator syncApplicator;
    private readonly IAsyncCrdtPatcher patcher;
    private readonly IAsyncCrdtMerger merger;
    private readonly ICrdtMetadataManager metadataManager;
    private readonly ICrdtSerializer serializer;
    private readonly IDistributedCrdtStorage storage;
    private readonly ICrdtTimestampProvider timestampProvider;
    private readonly ILogger<DistributedCrdtDocument<TState>> logger;
    private readonly bool activeSyncEnabled;
    private readonly int activeSyncDebounceMs;
    private readonly bool avoidBlindWrites;
    private readonly TState initialState;
    private readonly bool isLocalShard;
    
    // Fast synchronous lock for atomic reference/flag swapping against torn struct reads exclusively.
    private readonly object syncRoot = new();
    private CrdtDocument<TState> currentDocument;
    
    // Lightweight execution synchronization completely bypassing unbounded channel allocations for remote shards securely
    private readonly SemaphoreSlim? lightweightLock;

    // Single-reader channel completely eliminating Thread locks allocating non-thread-safe applicator sequential streams
    private readonly Channel<PooledDocumentCommand<TState>>? commandChannel;
    private readonly ConcurrentQueue<PooledDocumentCommand<TState>> commandPool = new();
    private readonly CancellationTokenSource disposeCts = new();
    private readonly Task? processingTask;
    
    // Active sync debouncing components
    private readonly object debounceSyncRoot = new();
    private readonly List<CrdtOperation>? debounceBuffer;
    private readonly Timer? debounceTimer;

    private volatile bool isDirty;
    private CrdtDocument<TState>? lastSavedDocument;

    private readonly Meter meter;
    private readonly Counter<long> patchAppliedCounter;
    private readonly Counter<long> operationsAppliedCounter;
    private readonly Counter<long> snapshotsMergedCounter;
    private readonly Counter<long> checkPointSavedCounter;

    // Channel metrics
    private readonly Counter<long> commandsEnqueuedCounter;
    private readonly Counter<long> commandsProcessedCounter;
    private readonly Counter<long> commandFailuresCounter;
    private ObservableGauge<int>? channelQueueLengthGauge;
    private ObservableGauge<int>? commandPoolSizeGauge;

    /// <inheritdoc />
    public string ShardId { get; }

    /// <inheritdoc />
    public string DocumentId { get; }

    /// <inheritdoc />
    public CrdtDocument<TState> Document
    {
        get
        {
            lock (syncRoot) return currentDocument;
        }
    }

    /// <inheritdoc />
    public event EventHandler? StateChanged;

    /// <inheritdoc />
    public event EventHandler<CrdtPatch>? PatchGenerated;

    public DistributedCrdtDocument(
        TState initialState,
        ReplicaContext replicaContext,
        IAsyncCrdtApplicator applicator,
        ICrdtApplicator syncApplicator,
        IAsyncCrdtPatcher patcher,
        IAsyncCrdtMerger merger,
        ICrdtMetadataManager metadataManager,
        IOptions<DistributedCrdtOptions> options,
        ICrdtSerializer serializer,
        IDistributedCrdtStorage storage,
        ILogger<DistributedCrdtDocument<TState>> logger,
        IDocumentIdProvider documentIdProvider,
        IShardIdProvider shardIdProvider,
        ICrdtTimestampProvider timestampProvider,
        IShardDistributionManager distributionManager,
        IMeterFactory? meterFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(initialState);
        ArgumentNullException.ThrowIfNull(documentIdProvider);
        ArgumentNullException.ThrowIfNull(shardIdProvider);
        ArgumentNullException.ThrowIfNull(distributionManager);

        this.replicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));
        this.applicator = applicator ?? throw new ArgumentNullException(nameof(applicator));
        this.syncApplicator = syncApplicator ?? throw new ArgumentNullException(nameof(syncApplicator));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));
        this.merger = merger ?? throw new ArgumentNullException(nameof(merger));
        this.metadataManager = metadataManager ?? throw new ArgumentNullException(nameof(metadataManager));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        this.timestampProvider = timestampProvider ?? throw new ArgumentNullException(nameof(timestampProvider));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.activeSyncEnabled = options.Value.ActiveSyncEnabled;
        this.activeSyncDebounceMs = options.Value.ActiveSyncDebounceMilliseconds;
        this.avoidBlindWrites = options.Value.AvoidBlindCheckpointWrites;
        this.initialState = initialState;

        var metadata = metadataManager.Initialize(initialState);
        currentDocument = new CrdtDocument<TState>(initialState, metadata);

        DocumentId = documentIdProvider.GetDocumentId(initialState).ToString() ?? string.Empty;
        ShardId = shardIdProvider.GetShardId(currentDocument) ?? string.Empty;
        
        this.isLocalShard = distributionManager.IsShardLocal(this.ShardId);

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.DistributedCrdtDocument") ?? new Meter("Ama.Enterprise.CRDT.Distributed.DistributedCrdtDocument");
        this.patchAppliedCounter = this.meter.CreateCounter<long>("crdt.document.patches_applied", "patches", "Total local patches natively applied mapping intentions");
        this.operationsAppliedCounter = this.meter.CreateCounter<long>("crdt.document.operations_applied", "operations", "Total remote operations synchronized locally successfully");
        this.snapshotsMergedCounter = this.meter.CreateCounter<long>("crdt.document.snapshots_merged", "snapshots", "Total incoming full snapshots superseding states");
        this.checkPointSavedCounter = this.meter.CreateCounter<long>("crdt.document.checkpoints_saved", "checkpoints", "Total underlying storage checkpoint alignments executed");
        this.commandsEnqueuedCounter = this.meter.CreateCounter<long>("crdt.document.channel.commands_enqueued", "commands", "Total document commands enqueued to the lock-free channel");
        this.commandsProcessedCounter = this.meter.CreateCounter<long>("crdt.document.channel.commands_processed", "commands", "Total document commands processed by the channel");
        this.commandFailuresCounter = this.meter.CreateCounter<long>("crdt.document.channel.command_failures", "errors", "Total document command processing failures");

        if (this.isLocalShard)
        {
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

            this.channelQueueLengthGauge = this.meter.CreateObservableGauge("crdt.document.channel.queue_length", 
                () => new Measurement<int>(this.commandChannel.Reader.CanCount ? this.commandChannel.Reader.Count : 0, new KeyValuePair<string, object?>("document_id", this.DocumentId)), 
                "commands", "Current number of pending document commands");
                
            this.commandPoolSizeGauge = this.meter.CreateObservableGauge("crdt.document.channel.pool_size", 
                () => new Measurement<int>(this.commandPool.Count, new KeyValuePair<string, object?>("document_id", this.DocumentId)), 
                "commands", "Current size of the document command object pool");

            // Initiates the lock-free sequential execution loop immediately
            this.processingTask = Task.Run(ProcessChannelAsync);
        }
        else
        {
            // Initializes explicit simple state bounds avoiding costly channels optimizing natively completely smartly
            this.lightweightLock = new SemaphoreSlim(1, 1);
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
                        case DocumentCommandType.ApplyPatch:
                            await ProcessApplyPatchInternalAsync(cmd).ConfigureAwait(false);
                            break;
                        case DocumentCommandType.ApplyOperations:
                            await ProcessApplyOperationsInternalAsync(cmd).ConfigureAwait(false);
                            break;
                        case DocumentCommandType.ApplyJournaledOperations:
                            ProcessApplyJournaledOperationsInternal(cmd);
                            break;
                        case DocumentCommandType.GetSnapshotData:
                            ProcessGetSnapshotDataInternal(cmd);
                            break;
                        case DocumentCommandType.MergeSnapshot:
                            await ProcessMergeSnapshotInternalAsync(cmd).ConfigureAwait(false);
                            break;
                        case DocumentCommandType.Checkpoint:
                            await ProcessCheckpointInternalAsync(cmd).ConfigureAwait(false);
                            break;
                        case DocumentCommandType.EvictReplica:
                            ProcessEvictReplicaInternal(cmd);
                            break;
                        case DocumentCommandType.ResetLocalState:
                            ProcessResetLocalStateInternal();
                            break;
                    }

                    cmd.SetResult();
                    commandsProcessedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
                }
                catch (OperationCanceledException)
                {
                    cmd.SetException(new OperationCanceledException("Document operation cancelled."));
                }
                catch (Exception ex)
                {
                    commandFailuresCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
                    cmd.SetException(ex);
                }
            }
        }
        catch (OperationCanceledException) { /* Clean graceful termination */ }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Distributed CRDT document lock-free processing loop critically faulted.");
        }
    }

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (!isLocalShard || commandChannel == null) return;

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

    private async Task ProcessInitializeInternalAsync(CancellationToken cancellationToken)
    {
        try
        {
            var storedDoc = await storage.LoadDocumentAsync<TState>(ShardId, DocumentId, cancellationToken).ConfigureAwait(false);
            
            if (storedDoc == null)
            {
                storedDoc = await storage.LoadOrphanedDocumentAsync<TState>(ShardId, DocumentId, cancellationToken).ConfigureAwait(false);
                if (storedDoc != null)
                {
                    logger.LogInformation("Successfully bootstrapped orphaned state for document {DocumentId} from persistent storage.", DocumentId);
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

                StateChanged?.Invoke(this, EventArgs.Empty);
                logger.LogInformation("Successfully loaded initial state for document {DocumentId} from persistent storage.", DocumentId);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load initial state for document {DocumentId} from persistent storage.", DocumentId);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task ApplyPatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default)
    {
        if (!isLocalShard || commandChannel == null)
        {
            // Optimizes footprint applying structurally mapped states in-memory directly tracking lightweight synchronization securely natively
            await lightweightLock!.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var result = await applicator.ApplyPatchAsync(currentDocument, patch).ConfigureAwait(false);
                lock (syncRoot)
                {
                    currentDocument = result.Document;
                }
            }
            finally
            {
                lightweightLock.Release();
            }

            patchAppliedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            StateChanged?.Invoke(this, EventArgs.Empty);
            PatchGenerated?.Invoke(this, patch);
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

    private async Task ProcessApplyPatchInternalAsync(PooledDocumentCommand<TState> cmd)
    {
        CrdtDocument<TState> currentDoc;
        lock (syncRoot) { currentDoc = currentDocument; }

        var result = await applicator.ApplyPatchAsync(currentDoc, cmd.Patch!.Value).ConfigureAwait(false);

        lock (syncRoot)
        {
            currentDocument = result.Document;
            isDirty = true;
        }

        patchAppliedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
        StateChanged?.Invoke(this, EventArgs.Empty);

        if (activeSyncEnabled && cmd.Patch!.Value.Operations != null && cmd.Patch.Value.Operations.Count > 0)
        {
            if (activeSyncDebounceMs > 0)
            {
                lock (debounceSyncRoot)
                {
                    debounceBuffer!.AddRange(cmd.Patch.Value.Operations);
                    debounceTimer!.Change(activeSyncDebounceMs, Timeout.Infinite);
                }
            }
            else
            {
                PatchGenerated?.Invoke(this, cmd.Patch.Value);
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
            PatchGenerated?.Invoke(this, batchedPatch);
        }
    }

    /// <inheritdoc />
    public ShardVersionVectorMap GetLocalState()
    {
        var sourceMap = replicaContext.ShardVersionVectors;
        lock (sourceMap)
        {
            var map = new ShardVersionVectorMap();
            foreach (var shardKvp in sourceMap)
            {
                var copiedVersions = new Dictionary<string, long>();
                var copiedDots = new Dictionary<string, ISet<long>>();
                foreach (var kvp in shardKvp.Value.Versions)
                {
                    copiedVersions[kvp.Key] = kvp.Value;
                }
                if (shardKvp.Value.Dots != null)
                {
                    foreach (var kvp in shardKvp.Value.Dots)
                    {
                        copiedDots[kvp.Key] = new HashSet<long>(kvp.Value);
                    }
                }
                map[shardKvp.Key] = new DottedVersionVector(copiedVersions, copiedDots);
            }
            return map;
        }
    }

    /// <inheritdoc />
    public async Task ApplyOperationsAsync(IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0) return;

        if (!isLocalShard || commandChannel == null)
        {
            await lightweightLock!.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                async IAsyncEnumerable<JournaledOperation> GetStreamAsync()
                {
                    foreach (var op in operations) yield return new JournaledOperation(DocumentId, op);
                    await Task.CompletedTask;
                }
                
                var result = await applicator.ApplyOperationsAsync(currentDocument, GetStreamAsync()).ConfigureAwait(false);
                lock (syncRoot)
                {
                    currentDocument = result.Document;
                }
            }
            finally
            {
                lightweightLock.Release();
            }

            operationsAppliedCounter.Add(operations.Count, new KeyValuePair<string, object?>("document_id", DocumentId));
            StateChanged?.Invoke(this, EventArgs.Empty);
            return;
        }

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.ApplyOperations;
        cmd.Operations = operations;
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

    private async Task ProcessApplyOperationsInternalAsync(PooledDocumentCommand<TState> cmd)
    {
        CrdtDocument<TState> currentDoc;
        lock (syncRoot) { currentDoc = currentDocument; }

        async IAsyncEnumerable<JournaledOperation> GetStreamAsync()
        {
            foreach (var op in cmd.Operations!)
            {
                yield return new JournaledOperation(DocumentId, op);
            }
            await Task.CompletedTask;
        }

        var result = await applicator.ApplyOperationsAsync(currentDoc, GetStreamAsync()).ConfigureAwait(false);

        lock (syncRoot)
        {
            currentDocument = result.Document;
            isDirty = true;
        }

        operationsAppliedCounter.Add(cmd.Operations!.Count, new KeyValuePair<string, object?>("document_id", DocumentId));
        StateChanged?.Invoke(this, EventArgs.Empty);
    }

    /// <inheritdoc />
    public async Task ApplyJournaledOperationsAsync(IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0) return;

        if (!isLocalShard || commandChannel == null)
        {
            lock (syncRoot)
            {
                var result = syncApplicator.ApplyPatch(currentDocument, new CrdtPatch(operations));
                currentDocument = result.Document;
            }
            
            operationsAppliedCounter.Add(operations.Count, new KeyValuePair<string, object?>("document_id", DocumentId));
            StateChanged?.Invoke(this, EventArgs.Empty);
            return;
        }

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.ApplyJournaledOperations;
        cmd.Operations = operations;
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

    private void ProcessApplyJournaledOperationsInternal(PooledDocumentCommand<TState> cmd)
    {
        CrdtDocument<TState> currentDoc;
        lock (syncRoot) { currentDoc = currentDocument; }

        var result = syncApplicator.ApplyPatch(currentDoc, new CrdtPatch(cmd.Operations!));

        lock (syncRoot)
        {
            currentDocument = result.Document;
            isDirty = true;
        }

        operationsAppliedCounter.Add(cmd.Operations!.Count, new KeyValuePair<string, object?>("document_id", DocumentId));
        StateChanged?.Invoke(this, EventArgs.Empty);
    }

    /// <inheritdoc />
    public async Task<CrdtSnapshotDataDto> GetSnapshotDataAsync(CancellationToken cancellationToken = default)
    {
        if (!isLocalShard || commandChannel == null)
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

    private void ProcessGetSnapshotDataInternal(PooledDocumentCommand<TState> cmd)
    {
        CrdtDocument<TState> currentDoc;
        lock (syncRoot) { currentDoc = currentDocument; }
        
        var globalState = GetLocalState(); 

        cmd.ResultSnapshotData = serializer.SerializeToBytes(currentDoc);
        cmd.ResultGlobalState = globalState;
    }

    /// <inheritdoc />
    public async Task MergeSnapshotAsync(byte[] snapshotData, ShardVersionVectorMap globalState, CancellationToken cancellationToken = default)
    {
        if (snapshotData == null || snapshotData.Length == 0) return;
        ArgumentNullException.ThrowIfNull(globalState);

        if (!isLocalShard || commandChannel == null)
        {
            var snapshotDoc = serializer.DeserializeFromBytes<CrdtDocument<TState>>(snapshotData);
            
            await lightweightLock!.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await merger.MergeStateAsync(currentDocument, snapshotDoc, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                lightweightLock.Release();
            }

            lock (replicaContext.ShardVersionVectors)
            {
                foreach (var shardKvp in globalState)
                {
                    if (!replicaContext.ShardVersionVectors.TryGetValue(shardKvp.Key, out var dvv))
                    {
                        dvv = new DottedVersionVector();
                        replicaContext.ShardVersionVectors[shardKvp.Key] = dvv;
                    }
                    dvv.Merge(shardKvp.Value);
                }
            }

            snapshotsMergedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            StateChanged?.Invoke(this, EventArgs.Empty);
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

    private async Task ProcessMergeSnapshotInternalAsync(PooledDocumentCommand<TState> cmd)
    {
        try
        {
            var snapshotDoc = serializer.DeserializeFromBytes<CrdtDocument<TState>>(cmd.SnapshotData!);

            CrdtDocument<TState> currentDoc;
            lock (syncRoot) { currentDoc = currentDocument; }

            await merger.MergeStateAsync(currentDoc, snapshotDoc, cmd.CancellationToken).ConfigureAwait(false);

            lock (syncRoot)
            {
                currentDocument = currentDoc;
                isDirty = true;
            }

            lock (replicaContext.ShardVersionVectors)
            {
                foreach (var shardKvp in cmd.GlobalState!)
                {
                    if (!replicaContext.ShardVersionVectors.TryGetValue(shardKvp.Key, out var dvv))
                    {
                        dvv = new DottedVersionVector();
                        replicaContext.ShardVersionVectors[shardKvp.Key] = dvv;
                    }
                    dvv.Merge(shardKvp.Value);
                }
            }

            snapshotsMergedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
            StateChanged?.Invoke(this, EventArgs.Empty);

            logger.LogInformation("Successfully merged global state snapshot for document {DocumentId}.", DocumentId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process snapshot merger for document {DocumentId}.", DocumentId);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task CheckpointAsync(CancellationToken cancellationToken = default)
    {
        if (!isLocalShard || commandChannel == null) return;

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
            logger.LogDebug("Successfully saved checkpoint to persistent storage for document {DocumentId}.", DocumentId);
        }
        catch (Exception)
        {
            lock (syncRoot) { isDirty = true; }
            throw;
        }
    }

    /// <inheritdoc />
    public async Task EvictReplicaAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));

        if (!isLocalShard || commandChannel == null)
        {
            lock (syncRoot)
            {
                metadataManager.EvictReplica(currentDocument, replicaId);
            }
            StateChanged?.Invoke(this, EventArgs.Empty);
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

    private void ProcessEvictReplicaInternal(PooledDocumentCommand<TState> cmd)
    {
        lock (syncRoot)
        {
            metadataManager.EvictReplica(currentDocument, cmd.ReplicaIdToEvict!);
            isDirty = true;
        }
        StateChanged?.Invoke(this, EventArgs.Empty);
    }

    /// <inheritdoc />
    public async Task ResetLocalStateAsync(string oldReplicaId, CancellationToken cancellationToken = default)
    {
        if (!isLocalShard || commandChannel == null)
        {
            lock (syncRoot)
            {
                var metadata = metadataManager.Initialize(initialState);
                currentDocument = new CrdtDocument<TState>(initialState, metadata);
            }
            StateChanged?.Invoke(this, EventArgs.Empty);
            return;
        }

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

    private void ProcessResetLocalStateInternal()
    {
        lock (syncRoot)
        {
            var metadata = metadataManager.Initialize(initialState);
            currentDocument = new CrdtDocument<TState>(initialState, metadata);
            isDirty = true;
        }
        StateChanged?.Invoke(this, EventArgs.Empty);
        logger.LogInformation("Successfully performed a hard reset following identity re-bootstrap for document {DocumentId}.", DocumentId);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (isLocalShard)
        {
            debounceTimer?.Dispose();
            disposeCts.Cancel();
            commandChannel?.Writer.TryComplete();
            disposeCts.Dispose();
        }
        else
        {
            lightweightLock?.Dispose();
        }
        meter.Dispose();
    }
}