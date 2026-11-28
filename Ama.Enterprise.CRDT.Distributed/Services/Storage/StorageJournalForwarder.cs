namespace Ama.Enterprise.CRDT.Distributed.Services.Storage;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Journaling;
using Ama.CRDT.Services.Versioning;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.AntiEntropy;
using Ama.Enterprise.CRDT.Distributed.Services.Documents;
using Ama.Enterprise.CRDT.Distributed.Services.Lifecycle;
using Ama.Enterprise.CRDT.Distributed.Services.Sharding;
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
    private readonly CrdtTrimCoordinator trimCoordinator;
    
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
        IMeterFactory? meterFactory = null,
        CrdtTrimCoordinator? trimCoordinator = null)
    {
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.trimCoordinator = trimCoordinator ?? this.serviceProvider.GetService<CrdtTrimCoordinator>() ?? new CrdtTrimCoordinator();
        
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
        
        this.EvaluateTriggersSync(operations.Count);
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
        
        var shouldInlineHardTrim = this.EvaluateTriggers(operations.Count);
        if (shouldInlineHardTrim && !this.IsHoldingAnyShardLock())
        {
            this.backpressureBlocksCounter.Add(1);
            await this.ExecuteHardTrimInlineAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Evaluates whether the hard journal threshold has been reached after a shard lock is released and executes an inline hard trim to apply synchronous backpressure on the command stripe worker.
    /// </summary>
    internal async Task WaitIfHardThresholdExceededAsync(int appendedCount, CancellationToken cancellationToken = default)
    {
        var hardThreshold = this.options.Value.JournalHardTrimThreshold;
        if (hardThreshold <= 0)
        {
            return;
        }

        if (Volatile.Read(ref this.estimatedJournalCount) >= hardThreshold && !this.IsHoldingAnyShardLock())
        {
            this.backpressureBlocksCounter.Add(1);
            await this.ExecuteHardTrimInlineAsync(cancellationToken).ConfigureAwait(false);
        }
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

    private void EvaluateTriggersSync(int count)
    {
        var shouldHardTrim = this.EvaluateTriggers(count);
        if (shouldHardTrim)
        {
            this.backpressureBlocksCounter.Add(1);
            if (!this.IsHoldingAnyShardLock())
            {
                this.ExecuteHardTrimInlineAsync(CancellationToken.None).GetAwaiter().GetResult();
            }
            else
            {
                this.TryScheduleBackgroundHardTrim();
            }
        }
    }

    private bool EvaluateTriggers(int count)
    {
        var softThreshold = this.options.Value.JournalSoftTrimThreshold;
        var hardThreshold = this.options.Value.JournalHardTrimThreshold;
        
        if (softThreshold <= 0 && hardThreshold <= 0)
        {
            return false;
        }

        var newCount = Interlocked.Add(ref this.estimatedJournalCount, count);
        
        bool isHard = hardThreshold > 0 && newCount >= hardThreshold;
        bool isSoft = softThreshold > 0 && newCount >= softThreshold;

        if (isHard)
        {
            this.CancelSoftTrim();

            if (this.IsHoldingAnyShardLock())
            {
                this.TryScheduleBackgroundHardTrim();
                return false;
            }

            return true;
        }

        if (isSoft)
        {
            // If a hard trim is actively running or queued to run, do not queue a new soft trim
            if (Volatile.Read(ref this.activeHardTrims) > 0)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref this.isTrimming, 1, 0) == 0)
            {
                // Double check active hard trims after taking soft trim assignment lock
                if (Volatile.Read(ref this.activeHardTrims) > 0)
                {
                    Interlocked.Exchange(ref this.isTrimming, 0);
                    return false;
                }
                
                this.StartBackgroundSoftTrim();
            }
        }

        return false;
    }

    private bool IsHoldingAnyShardLock()
    {
        var vvm = this.serviceProvider.GetService<IShardVersionVectorManager>();
        return vvm != null && vvm.IsCurrentContextHoldingAnyShardLock();
    }

    private void TryScheduleBackgroundHardTrim()
    {
        if (Volatile.Read(ref this.activeHardTrims) == 0 && Interlocked.CompareExchange(ref this.isTrimming, 1, 0) == 0)
        {
            if (Volatile.Read(ref this.activeHardTrims) > 0)
            {
                Interlocked.Exchange(ref this.isTrimming, 0);
            }
            else
            {
                this.CancelSoftTrim();
                this.StartBackgroundHardTrim();
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
            finally
            {
                Interlocked.Exchange(ref this.isTrimming, 0);
                this.ReevaluateThresholdAfterTrimCompletion();
            }
        }, CancellationToken.None);
    }

    private async Task ExecuteHardTrimInlineAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref this.activeHardTrims);
        try
        {
            this.CancelSoftTrim();

            // Wait on the node-wide lock to ensure only one hard trim happens at a time across all active threads on this node
            await this.trimCoordinator.WaitAsync(cancellationToken).ConfigureAwait(false);
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
                this.trimCoordinator.Release();
            }
        }
        finally
        {
            Interlocked.Decrement(ref this.activeHardTrims);
            this.ReevaluateThresholdAfterTrimCompletion();
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
                // Wait for node-wide lock, but respect cancellation if a hard trim preempts us and needs to jump the queue
                await this.trimCoordinator.WaitAsync(cts.Token).ConfigureAwait(false);
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
                    this.trimCoordinator.Release();
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

                this.ReevaluateThresholdAfterTrimCompletion();
            }
        }, CancellationToken.None);
    }

    private void ReevaluateThresholdAfterTrimCompletion()
    {
        var softThreshold = this.options.Value.JournalSoftTrimThreshold;
        var hardThreshold = this.options.Value.JournalHardTrimThreshold;
        var currentCount = Volatile.Read(ref this.estimatedJournalCount);

        if (hardThreshold > 0 && currentCount >= hardThreshold)
        {
            if (Volatile.Read(ref this.activeHardTrims) == 0 && Interlocked.CompareExchange(ref this.isTrimming, 1, 0) == 0)
            {
                if (Volatile.Read(ref this.activeHardTrims) > 0)
                {
                    Interlocked.Exchange(ref this.isTrimming, 0);
                }
                else
                {
                    this.CancelSoftTrim();
                    this.StartBackgroundHardTrim();
                }
            }
        }
        else if (softThreshold > 0 && currentCount >= softThreshold && Volatile.Read(ref this.activeHardTrims) == 0)
        {
            if (Interlocked.CompareExchange(ref this.isTrimming, 1, 0) == 0)
            {
                if (Volatile.Read(ref this.activeHardTrims) > 0)
                {
                    Interlocked.Exchange(ref this.isTrimming, 0);
                }
                else
                {
                    this.StartBackgroundSoftTrim();
                }
            }
        }
    }

    private async Task ExecuteAggressiveTrimAsync(CancellationToken cancellationToken)
    {
        var replicaContext = this.serviceProvider.GetRequiredService<ReplicaContext>();
        var versionVectorManager = this.serviceProvider.GetRequiredService<IShardVersionVectorManager>();
        var orchestrator = this.serviceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var clusterTracker = this.serviceProvider.GetService<IClusterStateTracker>();
        var syncService = this.serviceProvider.GetService<IVersionVectorSyncService>();

        var safelyPersistedMap = versionVectorManager.GetFullSnapshot();

        await CrdtCheckpointService.CheckpointActiveDocumentsInBatchesAsync(
            this.storage,
            orchestrator,
            cancellationToken,
            safelyPersistedMap).ConfigureAwait(false);

        if (this.storage is BufferedDistributedCrdtStorageDecorator bufferedDecorator)
        {
            var maxBatchSize = this.options.Value.WalMaxBatchSize > 0 ? this.options.Value.WalMaxBatchSize : 10000;
            while (bufferedDecorator.WriteAheadBuffer.PendingCount > 0 && !cancellationToken.IsCancellationRequested)
            {
                var flushed = await bufferedDecorator.FlushNextBatchAsync(maxBatchSize, TimeSpan.Zero, cancellationToken).ConfigureAwait(false);
                if (flushed == 0)
                {
                    break;
                }
            }
        }

        var isMemoryStorage = this.storage is MemoryCrdtStorage
            || (this.storage is BufferedDistributedCrdtStorageDecorator decorator && decorator.InnerStorage is MemoryCrdtStorage);
        if (!isMemoryStorage)
        {
            await this.storage.SaveShardVersionVectorMapAsync(replicaContext.ReplicaId, safelyPersistedMap, cancellationToken).ConfigureAwait(false);
        }

        var clusterStates = clusterTracker?.GetClusterStates();
        var countBeforeUpdate = Volatile.Read(ref this.estimatedJournalCount);
        var softThreshold = this.options.Value.JournalSoftTrimThreshold;
        var hardThreshold = this.options.Value.JournalHardTrimThreshold;
        var isThresholdExceeded = (softThreshold > 0 && countBeforeUpdate >= softThreshold) ||
                                  (hardThreshold > 0 && countBeforeUpdate >= hardThreshold);

        long remainingCount = 0;
        foreach (var shardKvp in safelyPersistedMap)
        {
            IReadOnlyDictionary<string, long> trimVector = shardKvp.Value.Versions.AsReadOnly();

            // Prefer trimming up to the Cluster Global Minimum Version Vector (GMVV) when peers are known
            // so lagging peers can still catch up via journaled patches instead of triggering full snapshot storms.
            if (clusterStates != null && clusterStates.Count > 0 && syncService != null)
            {
                var shardVectors = new List<DottedVersionVector>(clusterStates.Count + 1) { shardKvp.Value };
                for (int i = 0; i < clusterStates.Count; i++)
                {
                    if (clusterStates[i].TryGetValue(shardKvp.Key, out var peerDvv))
                    {
                        shardVectors.Add(peerDvv);
                    }
                }

                if (shardVectors.Count > 1)
                {
                    trimVector = syncService.CalculateGlobalMinimumVersionVector(shardVectors);
                }
            }

            await this.storage.TrimAsync(shardKvp.Key, trimVector, cancellationToken).ConfigureAwait(false);
            var shardJournalCount = await this.storage.GetJournalCountAsync(shardKvp.Key, cancellationToken).ConfigureAwait(false);

            remainingCount += shardJournalCount;
        }

        // If the total journal size across all shards still exceeds the configured soft or hard threshold after GMVV trimming
        // (for example, under hot operation bursts where peer DVV gossip lags behind local checkpointing),
        // force a trim up to the local checkpointed version vectors so the journal does not stay permanently above threshold.
        if (isThresholdExceeded &&
            ((softThreshold > 0 && remainingCount >= softThreshold) ||
             (hardThreshold > 0 && remainingCount >= hardThreshold)))
        {
            remainingCount = 0;
            foreach (var shardKvp in safelyPersistedMap)
            {
                if (shardKvp.Value.Versions.Count > 0)
                {
                    await this.storage.TrimAsync(shardKvp.Key, shardKvp.Value.Versions.AsReadOnly(), cancellationToken).ConfigureAwait(false);
                }

                remainingCount += await this.storage.GetJournalCountAsync(shardKvp.Key, cancellationToken).ConfigureAwait(false);
            }
        }

        Interlocked.Exchange(ref this.estimatedJournalCount, remainingCount);
        
        this.aggressiveTrimsCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaContext.ReplicaId));

        var thresholdUsed = (softThreshold > 0 && countBeforeUpdate >= softThreshold) 
            ? softThreshold 
            : hardThreshold;

        this.logger.LogWarning("[{ReplicaId}] Journal dynamically crossed real-time threshold ({Threshold}). Reactively executed immediate aggressive trim explicitly offloading constraints to snapshots.", replicaContext.ReplicaId, thresholdUsed);
    }

    private static void MergeRegistryVersionVector(
        IDistributedCrdtDocument<CrdtRegistryState> registryDoc,
        ShardVersionVectorMap safelyPersistedMap)
    {
        var shardId = registryDoc.ShardId;
        if (string.IsNullOrWhiteSpace(shardId))
        {
            return;
        }

        var fullRegistryDoc = registryDoc.ReadFullDocument();
        if (fullRegistryDoc.Metadata?.VersionVector == null || fullRegistryDoc.Metadata.VersionVector.Count == 0)
        {
            return;
        }

        var mergedDvv = safelyPersistedMap.TryGetValue(shardId, out var existing)
            ? existing
            : new DottedVersionVector();

        foreach (var kvp in fullRegistryDoc.Metadata.VersionVector)
        {
            if (kvp.Value > 0 &&
                (!mergedDvv.Versions.TryGetValue(kvp.Key, out var currentVer) || kvp.Value > currentVer))
            {
                mergedDvv.Versions[kvp.Key] = kvp.Value;
            }
        }

        if (mergedDvv.Versions.Count > 0)
        {
            safelyPersistedMap[shardId] = mergedDvv;
        }
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