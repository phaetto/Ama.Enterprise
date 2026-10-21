namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Background service responsible for periodically compacting CRDT metadata (such as tombstones and seen exceptions) across active documents on a scheduled interval.
/// </summary>
public sealed class CrdtCompactionService : BackgroundService
{
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtCompactionService> logger;

    private readonly Meter meter;
    private readonly Counter<long> compactionCyclesCounter;
    private readonly Counter<long> documentsCompactedCounter;
    private readonly Counter<long> compactionErrorsCounter;

    /// <summary>
    /// Initializes a new instance of the <see cref="CrdtCompactionService"/> class.
    /// </summary>
    /// <param name="scopeManager">The scope manager tracking active CRDT replica scopes.</param>
    /// <param name="options">The distributed CRDT configuration options.</param>
    /// <param name="logger">The logger instance.</param>
    /// <param name="meterFactory">Optional meter factory for telemetry instrumentation.</param>
    public CrdtCompactionService(
        DistributedCrdtScopeManager scopeManager,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtCompactionService> logger,
        IMeterFactory? meterFactory = null)
    {
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtCompactionService")
            ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtCompactionService");
        this.compactionCyclesCounter = this.meter.CreateCounter<long>(
            "crdt.compaction.cycles",
            "cycles",
            "Total background metadata compaction cycles executed");
        this.documentsCompactedCounter = this.meter.CreateCounter<long>(
            "crdt.compaction.documents_compacted",
            "documents",
            "Total CRDT documents processed during scheduled metadata compaction");
        this.compactionErrorsCounter = this.meter.CreateCounter<long>(
            "crdt.compaction.errors",
            "errors",
            "Total errors encountered during scheduled metadata compaction");
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var intervalSeconds = options.Value.CompactionIntervalSeconds;
        var delay = intervalSeconds > 0 ? TimeSpan.FromSeconds(intervalSeconds) : TimeSpan.FromSeconds(300);

        logger.LogInformation("CRDT background metadata compaction service started with an interval of {Seconds} seconds.", delay.TotalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (options.Value.CompactionTtlSeconds <= 0)
            {
                continue;
            }

            var scopes = scopeManager.GetActiveScopes();

            foreach (var scope in scopes)
            {
                var tag = new KeyValuePair<string, object?>("replica_id", scope.ReplicaId);

                try
                {
                    compactionCyclesCounter.Add(1, tag);

                    var documents = scope.Orchestrator.GetActiveDocuments();
                    foreach (var document in documents)
                    {
                        stoppingToken.ThrowIfCancellationRequested();
                        await document.CompactMetadataAsync(IntentDeliveryMode.Acknowledged, stoppingToken).ConfigureAwait(false);
                        documentsCompactedCounter.Add(1, tag);
                    }

                    logger.LogTrace("[{ReplicaId}] Completed scheduled CRDT metadata compaction across {Count} active documents.", scope.ReplicaId, documents.Count);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    compactionErrorsCounter.Add(1, tag);
                    logger.LogError(ex, "[{ReplicaId}] An error occurred during scheduled CRDT metadata compaction.", scope.ReplicaId);
                }
            }
        }
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        meter.Dispose();
        base.Dispose();
    }
}