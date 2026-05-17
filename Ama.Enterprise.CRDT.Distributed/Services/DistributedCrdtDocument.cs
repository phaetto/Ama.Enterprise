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
    private readonly ICrdtMetadataManager metadataManager;
    private readonly ICrdtSerializer serializer;
    private readonly IDistributedCrdtStorage storage;
    private readonly ILogger<DistributedCrdtDocument<TState>> logger;
    private readonly bool activeSyncEnabled;
    private readonly TState initialState;
    
    // Fast synchronous lock for atomic reference/flag swapping against torn struct reads exclusively.
    private readonly object syncRoot = new();
    
    // Single-reader channel completely eliminating Thread locks allocating non-thread-safe applicator sequential streams
    private readonly Channel<PooledDocumentCommand<TState>> commandChannel;
    private readonly ConcurrentQueue<PooledDocumentCommand<TState>> commandPool = new();
    private readonly CancellationTokenSource disposeCts = new();
    private readonly Task processingTask;
    
    private volatile bool isDirty;

    private readonly Meter meter;
    private readonly Counter<long> patchAppliedCounter;
    private readonly Counter<long> operationsAppliedCounter;
    private readonly Counter<long> snapshotsMergedCounter;
    private readonly Counter<long> checkPointSavedCounter;

    /// <inheritdoc />
    public string DocumentId { get; }

    /// <inheritdoc />
    public CrdtDocument<TState> Document { get; private set; }

    /// <inheritdoc />
    public event EventHandler? StateChanged;

    /// <inheritdoc />
    public event EventHandler<IReadOnlyList<CrdtOperation>>? OperationsGenerated;

    public DistributedCrdtDocument(
        TState initialState,
        ReplicaContext replicaContext,
        IAsyncCrdtApplicator applicator,
        ICrdtMetadataManager metadataManager,
        IOptions<DistributedCrdtOptions> options,
        ICrdtSerializer serializer,
        IDistributedCrdtStorage storage,
        ILogger<DistributedCrdtDocument<TState>> logger,
        IDocumentIdProvider documentIdProvider,
        IMeterFactory? meterFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(initialState);
        ArgumentNullException.ThrowIfNull(documentIdProvider);

        this.replicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));
        this.applicator = applicator ?? throw new ArgumentNullException(nameof(applicator));
        this.metadataManager = metadataManager ?? throw new ArgumentNullException(nameof(metadataManager));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.activeSyncEnabled = options.Value.ActiveSyncEnabled;
        this.initialState = initialState;

        DocumentId = documentIdProvider.GetDocumentId(initialState);
        
        var metadata = metadataManager.Initialize(initialState);
        Document = new CrdtDocument<TState>(initialState, metadata);

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.DistributedCrdtDocument") ?? new Meter("Ama.Enterprise.CRDT.Distributed.DistributedCrdtDocument");
        this.patchAppliedCounter = this.meter.CreateCounter<long>("crdt.document.patches_applied", "patches", "Total local patches natively applied mapping intentions");
        this.operationsAppliedCounter = this.meter.CreateCounter<long>("crdt.document.operations_applied", "operations", "Total remote operations synchronized locally successfully");
        this.snapshotsMergedCounter = this.meter.CreateCounter<long>("crdt.document.snapshots_merged", "snapshots", "Total incoming full snapshots superseding states");
        this.checkPointSavedCounter = this.meter.CreateCounter<long>("crdt.document.checkpoints_saved", "checkpoints", "Total underlying storage checkpoint alignments executed");

        this.commandChannel = Channel.CreateUnbounded<PooledDocumentCommand<TState>>(new UnboundedChannelOptions 
        { 
            SingleReader = true, 
            SingleWriter = false 
        });

        // Initiates the lock-free sequential execution loop immediately
        this.processingTask = Task.Run(ProcessChannelAsync);
    }

    private PooledDocumentCommand<TState> GetCommand() => commandPool.TryDequeue(out var cmd) ? cmd : new PooledDocumentCommand<TState>();

    private void ReturnCommand(PooledDocumentCommand<TState> cmd)
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
                        case DocumentCommandType.Initialize:
                            await ProcessInitializeInternalAsync(cmd.CancellationToken).ConfigureAwait(false);
                            break;
                        case DocumentCommandType.ApplyPatch:
                            await ProcessApplyPatchInternalAsync(cmd).ConfigureAwait(false);
                            break;
                        case DocumentCommandType.ApplyOperations:
                            await ProcessApplyOperationsInternalAsync(cmd).ConfigureAwait(false);
                            break;
                        case DocumentCommandType.GetSnapshotData:
                            ProcessGetSnapshotDataInternal(cmd);
                            break;
                        case DocumentCommandType.MergeSnapshot:
                            ProcessMergeSnapshotInternal(cmd);
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
                }
                catch (OperationCanceledException)
                {
                    cmd.SetException(new OperationCanceledException("Document operation cancelled."));
                }
                catch (Exception ex)
                {
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
        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.Initialize;
        cmd.CancellationToken = cancellationToken;

        commandChannel.Writer.TryWrite(cmd);
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
        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.ApplyPatch;
        cmd.Patch = patch;
        cmd.CancellationToken = cancellationToken;

        commandChannel.Writer.TryWrite(cmd);
        try
        {
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
        lock (syncRoot) { currentDoc = Document; }

        var result = await applicator.ApplyPatchAsync(currentDoc, cmd.Patch!.Value).ConfigureAwait(false);

        lock (syncRoot)
        {
            Document = result.Document;
            isDirty = true;
        }

        patchAppliedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
        StateChanged?.Invoke(this, EventArgs.Empty);

        if (activeSyncEnabled && cmd.Patch!.Value.Operations != null && cmd.Patch.Value.Operations.Count > 0)
        {
            OperationsGenerated?.Invoke(this, cmd.Patch.Value.Operations);
        }
    }

    /// <inheritdoc />
    public DottedVersionVector GetLocalState()
    {
        var sourceDvv = replicaContext.GlobalVersionVector;
        lock (sourceDvv)
        {
            return sourceDvv.DeepClone();
        }
    }

    /// <inheritdoc />
    public async Task ApplyOperationsAsync(IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0) return;

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.ApplyOperations;
        cmd.Operations = operations;
        cmd.CancellationToken = cancellationToken;

        commandChannel.Writer.TryWrite(cmd);
        try
        {
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
        lock (syncRoot) { currentDoc = Document; }

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
            Document = result.Document;
            isDirty = true;
        }

        operationsAppliedCounter.Add(cmd.Operations!.Count, new KeyValuePair<string, object?>("document_id", DocumentId));
        StateChanged?.Invoke(this, EventArgs.Empty);
    }

    /// <inheritdoc />
    public async Task<CrdtSnapshotDataDto> GetSnapshotDataAsync(CancellationToken cancellationToken = default)
    {
        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.GetSnapshotData;
        cmd.CancellationToken = cancellationToken;

        commandChannel.Writer.TryWrite(cmd);
        try
        {
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
        lock (syncRoot) { currentDoc = Document; }
        
        var globalState = GetLocalState(); 

        cmd.ResultSnapshotData = serializer.SerializeToBytes(currentDoc);
        cmd.ResultGlobalState = globalState;
    }

    /// <inheritdoc />
    public async Task MergeSnapshotAsync(byte[] snapshotData, DottedVersionVector globalState, CancellationToken cancellationToken = default)
    {
        if (snapshotData == null || snapshotData.Length == 0) return;

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.MergeSnapshot;
        cmd.SnapshotData = snapshotData;
        cmd.GlobalState = globalState;
        cmd.CancellationToken = cancellationToken;

        commandChannel.Writer.TryWrite(cmd);
        try
        {
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    private void ProcessMergeSnapshotInternal(PooledDocumentCommand<TState> cmd)
    {
        try
        {
            var snapshotDoc = serializer.DeserializeFromBytes<CrdtDocument<TState>>(cmd.SnapshotData!);

            lock (syncRoot)
            {
                Document = snapshotDoc;
                isDirty = true;
            }

            lock (replicaContext.GlobalVersionVector)
            {
                replicaContext.GlobalVersionVector.Merge(cmd.GlobalState!);
            }

            snapshotsMergedCounter.Add(1, new KeyValuePair<string, object?>("document_id", DocumentId));
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
        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.Checkpoint;
        cmd.CancellationToken = cancellationToken;

        commandChannel.Writer.TryWrite(cmd);
        try
        {
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    private async Task ProcessCheckpointInternalAsync(PooledDocumentCommand<TState> cmd)
    {
        if (!isDirty) return;

        CrdtDocument<TState> currentDoc;
        lock (syncRoot)
        {
            currentDoc = Document;
            isDirty = false; 
        }

        try
        {
            await storage.SaveDocumentAsync(DocumentId, currentDoc, cmd.CancellationToken).ConfigureAwait(false);
            
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

        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.EvictReplica;
        cmd.ReplicaIdToEvict = replicaId;
        cmd.CancellationToken = cancellationToken;

        commandChannel.Writer.TryWrite(cmd);
        try
        {
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
            metadataManager.EvictReplica(Document, cmd.ReplicaIdToEvict!);
            isDirty = true;
        }
        StateChanged?.Invoke(this, EventArgs.Empty);
    }

    /// <inheritdoc />
    public async Task ResetLocalStateAsync(string oldReplicaId, CancellationToken cancellationToken = default)
    {
        var cmd = GetCommand();
        cmd.Type = DocumentCommandType.ResetLocalState;
        cmd.OldReplicaId = oldReplicaId;
        cmd.CancellationToken = cancellationToken;

        commandChannel.Writer.TryWrite(cmd);
        try
        {
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
            Document = new CrdtDocument<TState>(initialState, metadata);
            isDirty = true;
        }
        StateChanged?.Invoke(this, EventArgs.Empty);
        logger.LogInformation("Successfully performed a hard reset following identity re-bootstrap for document {DocumentId}.", DocumentId);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        disposeCts.Cancel();
        commandChannel.Writer.TryComplete();
        disposeCts.Dispose();
        meter.Dispose();
    }
}