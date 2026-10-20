namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Journaling;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Forwards journaling operations from the core CRDT pipeline to the unified distributed storage.
/// Ensures the active IJournalManager utilizes the shared registered storage backend implicitly.
/// Intercepts operations to reactively enforce strict size-based trimming bounds seamlessly preventing IO blocks natively.
/// </summary>
internal sealed class StorageJournalForwarder : ICrdtOperationJournal, IDisposable
{
    private readonly IDistributedCrdtStorage storage;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly IServiceProvider serviceProvider;
    private readonly ILogger<StorageJournalForwarder> logger;
    
    private readonly Meter meter;
    private readonly Counter<long> appendedOperationsCounter;
    private readonly Counter<long> aggressiveTrimsCounter;
    private readonly Counter<long> trimFailuresCounter;
    private readonly Counter<long> backpressureBlocksCounter;

    private long estimatedJournalCount;
    private int isTrimming;
    private int activeHardTrims;
    private CancellationTokenSource? softTrimCts;

    public StorageJournalForwarder(
        IDistributedCrdtStorage storage,
        IOptions<DistributedCrdtOptions> options,
        IServiceProvider serviceProvider,
        ILogger<StorageJournalForwarder> logger,
        IMeterFactory? meterFactory = null)
    {
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        
        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.StorageJournalForwarder") ?? new Meter("Ama.Enterprise.CRDT.Distributed.StorageJournalForwarder");
        this.appendedOperationsCounter = this.meter.CreateCounter<long>("crdt.journal.forwarded_operations", "operations", "Total operations effectively forwarded towards explicit backend configurations naturally");
        this.aggressiveTrimsCounter = this.meter.CreateCounter<long>("crdt.journal.aggressive_trims", "trims", "Total reactive aggressive trims executed bypassing standard checkpoint bounds natively");
        this.trimFailuresCounter = this.meter.CreateCounter<long>("crdt.journal.aggressive_trim_failures", "failures", "Total failures encountered during reactive aggressive journal trims natively");
        this.backpressureBlocksCounter = this.meter.CreateCounter<long>("crdt.journal.backpressure_blocks", "blocks", "Total times the hard threshold applied natural backpressure yielding the active threads natively");

        _ = Task.Run(async () =>
        {
            try
            {
                var versionVectorManager = this.serviceProvider.GetRequiredService<IShardVersionVectorManager>();
                long totalCount = 0;
                
                var shardIds = versionVectorManager.GetShardIds();

                foreach (var shardId in shardIds)
                {
                    totalCount += await this.storage.GetJournalCountAsync(shardId, CancellationToken.None).ConfigureAwait(false);
                }
                
                Interlocked.Add(ref this.estimatedJournalCount, totalCount);
            }
            catch (Exception ex)
            {
                this.logger.LogDebug(ex, "Failed to fetch initial journal count for threshold tracking natively.");
            }
        });
    }

    public void Append(IComparable documentId, IReadOnlyList<CrdtOperation> operations)
    {
        if (documentId is null)
        {
            throw new ArgumentNullException(nameof(documentId));
        }

        if (operations is null)
        {
            throw new ArgumentNullException(nameof(operations));
        }

        string docIdString = documentId.ToString() ?? string.Empty;
        if (string.IsNullOrEmpty(docIdString))
        {
            throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        }

        this.storage.Append(documentId, operations);
        this.appendedOperationsCounter.Add(operations.Count, new KeyValuePair<string, object?>("document_id", docIdString));
        
        this.EvaluateTriggersAndApplyBackpressureSync(operations.Count);
    }

    public async Task AppendAsync(IComparable documentId, IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default)
    {
        if (documentId is null)
        {
            throw new ArgumentNullException(nameof(documentId));
        }

        if (operations is null)
        {
            throw new ArgumentNullException(nameof(operations));
        }

        string docIdString = documentId.ToString() ?? string.Empty;
        if (string.IsNullOrEmpty(docIdString))
        {
            throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        }

        await this.storage.AppendAsync(documentId, operations, cancellationToken).ConfigureAwait(false);
        this.appendedOperationsCounter.Add(operations.Count, new KeyValuePair<string, object?>("document_id", docIdString));
        
        this.EvaluateTriggersAndApplyBackpressureSync(operations.Count);
    }

    public IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string shardId, string originReplicaId, long minShardClock, long maxShardClock, CancellationToken cancellationToken = default) 
        => this.storage.GetOperationsByRangeAsync(shardId, originReplicaId, minShardClock, maxShardClock, cancellationToken);

    public IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string shardId, string originReplicaId, IEnumerable<long> shardClocks, CancellationToken cancellationToken = default) 
        => this.storage.GetOperationsByDotsAsync(shardId, originReplicaId, shardClocks, cancellationToken);

    public void Dispose()
    {
        this.meter.Dispose();
        this.CancelSoftTrim();
    }

    private void EvaluateTriggersAndApplyBackpressureSync(int count)
    {
        var softThreshold = this.options.Value.JournalSoftTrimThreshold;
        var hardThreshold = this.options.Value.JournalHardTrimThreshold;
        
        if (softThreshold <= 0 && hardThreshold <= 0)
        {
            return;
        }

        var newCount = Interlocked.Add(ref this.estimatedJournalCount, count);
        
        bool isHard = hardThreshold > 0 && newCount >= hardThreshold;
        bool isSoft = softThreshold > 0 && newCount >= softThreshold;

        if (isHard)
        {
            this.CancelSoftTrim();

            if (Volatile.Read(ref this.activeHardTrims) == 0 && Interlocked.CompareExchange(ref this.isTrimming, 1, 0) == 0)
            {
                if (Volatile.Read(ref this.activeHardTrims) > 0)
                {
                    Interlocked.Exchange(ref this.isTrimming, 0);
                }
                else
                {
                    this.StartBackgroundHardTrim();
                }
            }

            this.backpressureBlocksCounter.Add(1);
            
            // Intentionally bypassing GlobalTrimLock wait here to prevent cyclic deadlocks with the single-reader channel.
            // Blocking the active reader thread blocks CheckpointAsync operations required by the trimmer to complete,
            // resulting in a complete standstill. Natural channel capacity throttling is sufficient backpressure.
        }
        else if (isSoft)
        {
            // If a hard trim is actively running or queued to run, do not queue a new soft trim
            if (Volatile.Read(ref this.activeHardTrims) > 0)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref this.isTrimming, 1, 0) == 0)
            {
                // Double check active hard trims after taking soft trim assignment lock
                if (Volatile.Read(ref this.activeHardTrims) > 0)
                {
                    Interlocked.Exchange(ref this.isTrimming, 0);
                    return;
                }
                
                this.StartBackgroundSoftTrim();
            }
        }
    }

    private async ValueTask EvaluateTriggersAndApplyBackpressureAsync(int count, CancellationToken cancellationToken)
    {
        var softThreshold = this.options.Value.JournalSoftTrimThreshold;
        var hardThreshold = this.options.Value.JournalHardTrimThreshold;
        
        if (softThreshold <= 0 && hardThreshold <= 0)
        {
            return;
        }

        var newCount = Interlocked.Add(ref this.estimatedJournalCount, count);
        
        bool isHard = hardThreshold > 0 && newCount >= hardThreshold;
        bool isSoft = softThreshold > 0 && newCount >= softThreshold;

        if (isHard)
        {
            this.CancelSoftTrim();

            if (Volatile.Read(ref this.activeHardTrims) == 0 && Interlocked.CompareExchange(ref this.isTrimming, 1, 0) == 0)
            {
                if (Volatile.Read(ref this.activeHardTrims) > 0)
                {
                    Interlocked.Exchange(ref this.isTrimming, 0);
                }
                else
                {
                    this.StartBackgroundHardTrim();
                }
            }

            this.backpressureBlocksCounter.Add(1);
            
            // Yield the caller to allow background tasks to initialize gracefully without deadlocking the pipeline natively.
            // We strictly avoid awaiting the GlobalTrimLock here because this thread is the active channel reader,
            // and the trim requires this reader to process CheckpointAsync channel commands to proceed, creating a cyclic deadlock.
            await Task.Yield();
        }
        else if (isSoft)
        {
            // If a hard trim is actively running or queued to run, do not queue a new soft trim
            if (Volatile.Read(ref this.activeHardTrims) > 0)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref this.isTrimming, 1, 0) == 0)
            {
                // Double check active hard trims after taking soft trim assignment lock
                if (Volatile.Read(ref this.activeHardTrims) > 0)
                {
                    Interlocked.Exchange(ref this.isTrimming, 0);
                    return;
                }
                
                this.StartBackgroundSoftTrim();
            }
        }
    }

    private void CancelSoftTrim()
    {
        var currentCts = Interlocked.Exchange(ref this.softTrimCts, null);
        if (currentCts != null)
        {
            try 
            {
                currentCts.Cancel();
                currentCts.Dispose();
            } 
            catch 
            { 
                // Ignore disposal issues during active cancellation bounds natively
            }
        }
    }

    private void StartBackgroundHardTrim()
    {
        _ = Task.Run(async () =>
        {
            try
            {
                await this.ExecuteHardTrimInlineAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                this.logger.LogWarning(ex, "Background hard trim encountered an exception executing constraints.");
            }
            finally
            {
                Interlocked.Exchange(ref this.isTrimming, 0);
            }
        });
    }

    private async Task ExecuteHardTrimInlineAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref this.activeHardTrims);
        try
        {
            this.CancelSoftTrim();

            // Wait on the global lock to ensure only one hard trim happens at a time across all active threads
            await CrdtTrimCoordinator.GlobalTrimLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var hardThreshold = this.options.Value.JournalHardTrimThreshold;
                
                // Double-check if the count is still above threshold since another queued thread might have just trimmed it
                if (hardThreshold > 0 && Volatile.Read(ref this.estimatedJournalCount) < hardThreshold)
                {
                    return;
                }

                await this.ExecuteAggressiveTrimAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Caller cancelled, propagate safely natively
                throw;
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Failed to execute inline aggressive journal trim.");
                this.TrackTrimFailure();
                Interlocked.Exchange(ref this.estimatedJournalCount, 0);
            }
            finally
            {
                CrdtTrimCoordinator.GlobalTrimLock.Release();
            }
        }
        finally
        {
            Interlocked.Decrement(ref this.activeHardTrims);
        }
    }

    private void StartBackgroundSoftTrim()
    {
        var cts = new CancellationTokenSource();
        this.softTrimCts = cts;

        _ = Task.Run(async () =>
        {
            try
            {
                // Wait for global lock, but respect cancellation if a hard trim preempts us and needs to jump the queue
                await CrdtTrimCoordinator.GlobalTrimLock.WaitAsync(cts.Token).ConfigureAwait(false);
                try
                {
                    if (cts.Token.IsCancellationRequested)
                    {
                        return;
                    }

                    await this.ExecuteAggressiveTrimAsync(cts.Token).ConfigureAwait(false);
                }
                finally
                {
                    CrdtTrimCoordinator.GlobalTrimLock.Release();
                }
            }
            catch (OperationCanceledException)
            {
                // Soft trim was cancelled and preempted by an inline hard trim. Expected behavior.
            }
            catch (ObjectDisposedException)
            {
                // Ignored natively during shutdown
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Failed to execute background soft journal trim.");
                this.TrackTrimFailure();
                Interlocked.Exchange(ref this.estimatedJournalCount, 0);
            }
            finally
            {
                Interlocked.Exchange(ref this.isTrimming, 0);
                
                var oldCts = Interlocked.CompareExchange(ref this.softTrimCts, null, cts);
                if (oldCts == cts)
                {
                    cts.Dispose();
                }
            }
        }, CancellationToken.None);
    }

    private async Task ExecuteAggressiveTrimAsync(CancellationToken cancellationToken)
    {
        var replicaContext = this.serviceProvider.GetRequiredService<ReplicaContext>();
        var versionVectorManager = this.serviceProvider.GetRequiredService<IShardVersionVectorManager>();
        var orchestrator = this.serviceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        
        var documents = orchestrator.GetActiveDocuments();
        var safelyPersistedMap = versionVectorManager.GetFullSnapshot();

        foreach (var shardGroup in documents.GroupBy(d => d.ShardId, StringComparer.Ordinal))
        {
            if (!string.IsNullOrEmpty(shardGroup.Key))
            {
                var freshShardSnapshot = versionVectorManager.GetShardSnapshot(shardGroup.Key);
                if (freshShardSnapshot.TryGetValue(shardGroup.Key, out var freshDvv))
                {
                    safelyPersistedMap[shardGroup.Key] = freshDvv;
                }
            }

            foreach (var document in shardGroup)
            {
                await document.CheckpointAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        await this.storage.SaveShardVersionVectorMapAsync(replicaContext.ReplicaId, safelyPersistedMap, cancellationToken).ConfigureAwait(false);

        long remainingCount = 0;
        foreach (var shardKvp in safelyPersistedMap)
        {
            await this.storage.TrimAsync(shardKvp.Key, shardKvp.Value.Versions.AsReadOnly(), cancellationToken).ConfigureAwait(false);
            remainingCount += await this.storage.GetJournalCountAsync(shardKvp.Key, cancellationToken).ConfigureAwait(false);
        }

        var countBeforeUpdate = Volatile.Read(ref this.estimatedJournalCount);
        Interlocked.Exchange(ref this.estimatedJournalCount, remainingCount);
        
        this.aggressiveTrimsCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaContext.ReplicaId));

        var thresholdUsed = (this.options.Value.JournalSoftTrimThreshold > 0 && countBeforeUpdate >= this.options.Value.JournalSoftTrimThreshold) 
            ? this.options.Value.JournalSoftTrimThreshold 
            : this.options.Value.JournalHardTrimThreshold;

        this.logger.LogWarning("[{ReplicaId}] Journal dynamically crossed real-time threshold ({Threshold}). Reactively executed immediate aggressive trim explicitly offloading constraints to snapshots.", replicaContext.ReplicaId, thresholdUsed);
    }

    private void TrackTrimFailure()
    {
        string? replicaId = null;
        try
        {
            replicaId = this.serviceProvider.GetService<ReplicaContext>()?.ReplicaId;
        }
        catch
        {
            // Ignore resolution errors during fallback exception logging natively
        }
        this.trimFailuresCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaId ?? "unknown"));
    }
}