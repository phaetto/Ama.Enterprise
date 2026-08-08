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
                var count = await this.storage.GetJournalCountAsync(CancellationToken.None).ConfigureAwait(false);
                Interlocked.Add(ref this.estimatedJournalCount, count);
            }
            catch (Exception ex)
            {
                this.logger.LogDebug(ex, "Failed to fetch initial journal count for threshold tracking natively.");
            }
        });
    }

    public void Append(string documentId, IReadOnlyList<CrdtOperation> operationsList)
    {
        if (string.IsNullOrEmpty(documentId))
        {
            throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        }

        this.storage.Append(documentId, operationsList);
        this.appendedOperationsCounter.Add(operationsList.Count, new KeyValuePair<string, object?>("document_id", documentId));
        
        this.EvaluateTriggersAndApplyBackpressureSync(operationsList.Count);
    }

    public async Task AppendAsync(string documentId, IReadOnlyList<CrdtOperation> operationsList, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(documentId))
        {
            throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        }

        await this.storage.AppendAsync(documentId, operationsList, cancellationToken).ConfigureAwait(false);
        this.appendedOperationsCounter.Add(operationsList.Count, new KeyValuePair<string, object?>("document_id", documentId));
        
        await this.EvaluateTriggersAndApplyBackpressureAsync(operationsList.Count, cancellationToken).ConfigureAwait(false);
    }

    public IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string originReplicaId, long minGlobalClock, long maxGlobalClock, CancellationToken cancellationToken = default) 
        => this.storage.GetOperationsByRangeAsync(originReplicaId, minGlobalClock, maxGlobalClock, cancellationToken);

    public IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string originReplicaId, IEnumerable<long> globalClocks, CancellationToken cancellationToken = default) 
        => this.storage.GetOperationsByDotsAsync(originReplicaId, globalClocks, cancellationToken);

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

                var countAtTrimStart = Volatile.Read(ref this.estimatedJournalCount);
                
                await this.ExecuteAggressiveTrimAsync(cancellationToken).ConfigureAwait(false);
                
                Interlocked.Add(ref this.estimatedJournalCount, -countAtTrimStart);
                if (Volatile.Read(ref this.estimatedJournalCount) < 0)
                {
                    Interlocked.Exchange(ref this.estimatedJournalCount, 0);
                }
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

        var countAtTrimStart = Volatile.Read(ref this.estimatedJournalCount);

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
                    
                    Interlocked.Add(ref this.estimatedJournalCount, -countAtTrimStart);
                    if (Volatile.Read(ref this.estimatedJournalCount) < 0)
                    {
                        Interlocked.Exchange(ref this.estimatedJournalCount, 0);
                    }
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
        var orchestrator = this.serviceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        
        var sourceDvv = replicaContext.GlobalVersionVector;
        var copiedVersions = new Dictionary<string, long>();
        var copiedDots = new Dictionary<string, ISet<long>>();
        
        lock (sourceDvv)
        {
            foreach (var kvp in sourceDvv.Versions)
            {
                copiedVersions[kvp.Key] = kvp.Value;
            }
            if (sourceDvv.Dots != null)
            {
                foreach (var kvp in sourceDvv.Dots)
                {
                    copiedDots[kvp.Key] = new HashSet<long>(kvp.Value);
                }
            }
        }
        
        var safelyPersistedDvv = new DottedVersionVector(copiedVersions, copiedDots);
        
        // Parallelize checkpointing across active documents to drastically reduce the trim duration lock time
        var documents = orchestrator.GetActiveDocuments().ToList();
        var chunks = documents.Chunk(Environment.ProcessorCount);
        
        foreach (var chunk in chunks)
        {
            await Task.WhenAll(chunk.Select(document => document.CheckpointAsync(cancellationToken))).ConfigureAwait(false);
        }

        await this.storage.SaveGlobalVersionVectorAsync(replicaContext.ReplicaId, safelyPersistedDvv, cancellationToken).ConfigureAwait(false);

        await this.storage.TrimAsync(safelyPersistedDvv.Versions.ToDictionary(k => k.Key, v => v.Value), cancellationToken).ConfigureAwait(false);
        
        this.aggressiveTrimsCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaContext.ReplicaId));

        var thresholdUsed = (this.options.Value.JournalSoftTrimThreshold > 0 && Volatile.Read(ref this.estimatedJournalCount) >= this.options.Value.JournalSoftTrimThreshold) 
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