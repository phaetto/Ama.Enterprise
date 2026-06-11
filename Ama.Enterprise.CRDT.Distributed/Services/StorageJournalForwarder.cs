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

    private long estimatedJournalCount;
    private int isTrimming;

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

        storage.Append(documentId, operationsList);
        appendedOperationsCounter.Add(operationsList.Count, new KeyValuePair<string, object?>("document_id", documentId));
        
        TrackAndTriggerTrim(operationsList.Count);
    }

    public async Task AppendAsync(string documentId, IReadOnlyList<CrdtOperation> operationsList, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(documentId))
        {
            throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        }

        await storage.AppendAsync(documentId, operationsList, cancellationToken).ConfigureAwait(false);
        appendedOperationsCounter.Add(operationsList.Count, new KeyValuePair<string, object?>("document_id", documentId));
        
        TrackAndTriggerTrim(operationsList.Count);
    }

    public IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string originReplicaId, long minGlobalClock, long maxGlobalClock, CancellationToken cancellationToken = default) 
        => storage.GetOperationsByRangeAsync(originReplicaId, minGlobalClock, maxGlobalClock, cancellationToken);

    public IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string originReplicaId, IEnumerable<long> globalClocks, CancellationToken cancellationToken = default) 
        => storage.GetOperationsByDotsAsync(originReplicaId, globalClocks, cancellationToken);

    public void Dispose()
    {
        meter.Dispose();
    }

    private void TrackAndTriggerTrim(int count)
    {
        var threshold = options.Value.JournalTrimThreshold;
        if (threshold <= 0) return;

        var newCount = Interlocked.Add(ref estimatedJournalCount, count);
        if (newCount >= threshold)
        {
            if (Interlocked.CompareExchange(ref isTrimming, 1, 0) == 0)
            {
                var countAtTrimStart = Volatile.Read(ref estimatedJournalCount);

                _ = Task.Run(async () =>
                {
                    try
                    {
                        await ExecuteAggressiveTrimAsync().ConfigureAwait(false);
                        
                        // Deduct the operations we effectively just trimmed, safely preserving the ones appended concurrently during the trim
                        Interlocked.Add(ref estimatedJournalCount, -countAtTrimStart);

                        // Safeguard against going negative due to extreme concurrency edge cases
                        if (Volatile.Read(ref estimatedJournalCount) < 0)
                        {
                            Interlocked.Exchange(ref estimatedJournalCount, 0);
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                        // Ignored during application shutdown natively
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to execute reactive aggressive journal trim.");

                        string? replicaId = null;
                        try
                        {
                            replicaId = serviceProvider.GetService<ReplicaContext>()?.ReplicaId;
                        }
                        catch
                        {
                            // Ignore DI resolution errors during fallback exception logging
                        }

                        trimFailuresCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaId ?? "unknown"));

                        // Force clearing lock conditions on failures avoiding perpetual backpressure limits
                        Interlocked.Exchange(ref estimatedJournalCount, 0);
                    }
                    finally
                    {
                        Interlocked.Exchange(ref isTrimming, 0);
                    }
                });
            }
        }
    }

    private async Task ExecuteAggressiveTrimAsync()
    {
        var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
        var orchestrator = serviceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        
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
        
        var documents = orchestrator.GetActiveDocuments();
        foreach (var document in documents)
        {
            await document.CheckpointAsync(CancellationToken.None).ConfigureAwait(false);
        }

        await storage.SaveGlobalVersionVectorAsync(replicaContext.ReplicaId, safelyPersistedDvv, CancellationToken.None).ConfigureAwait(false);

        await storage.TrimAsync(safelyPersistedDvv.Versions.ToDictionary(k => k.Key, v => v.Value), CancellationToken.None).ConfigureAwait(false);
        
        aggressiveTrimsCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaContext.ReplicaId));

        logger.LogWarning("[{ReplicaId}] Journal dynamically crossed real-time threshold ({Threshold}). Reactively executed immediate aggressive trim explicitly offloading constraints to snapshots.", replicaContext.ReplicaId, options.Value.JournalTrimThreshold);
    }
}