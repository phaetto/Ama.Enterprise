namespace Ama.Enterprise.CRDT.Distributed.Services.AntiEntropy;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Background service responsible for repeatedly broadcasting the local synchronization state while adapting to queue depth and sync backlog.
/// </summary>
public sealed class CrdtAntiEntropyService : BackgroundService
{
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtAntiEntropyService> logger;

    private readonly Meter meter;
    private readonly Counter<long> antiEntropyCyclesCounter;
    private readonly Counter<long> antiEntropySkippedCyclesCounter;

    public CrdtAntiEntropyService(
        DistributedCrdtScopeManager scopeManager,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtAntiEntropyService> logger,
        IMeterFactory? meterFactory = null)
    {
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtAntiEntropyService") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtAntiEntropyService");
        this.antiEntropyCyclesCounter = this.meter.CreateCounter<long>("crdt.anti_entropy.cycles_executed", "cycles", "Total localized periodic anti-entropy interactions dispatched");
        this.antiEntropySkippedCyclesCounter = this.meter.CreateCounter<long>("crdt.anti_entropy.cycles_skipped", "cycles", "Total periodic anti-entropy cycles skipped due to active sync or queue backlog");
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = options.Value;
        var initialDelay = TimeSpan.FromSeconds(Math.Max(0, opts.AntiEntropyInitialDelaySeconds));
        var jitterMs = Math.Max(0, opts.AntiEntropyInitialMaxJitterMilliseconds);
        var normalInterval = TimeSpan.FromSeconds(Math.Max(1, opts.AntiEntropyIntervalSeconds));
        var startupDuration = TimeSpan.FromSeconds(Math.Max(0, opts.AntiEntropyStartupDurationSeconds));
        var startupInterval = TimeSpan.FromSeconds(Math.Max(1, opts.AntiEntropyStartupIntervalSeconds));
        var maxAdaptiveInterval = TimeSpan.FromSeconds(Math.Max(opts.AntiEntropyIntervalSeconds, opts.AntiEntropyMaxAdaptiveIntervalSeconds));
        var delayPerThousandMs = Math.Max(0, opts.AntiEntropyAdaptiveDelayPerThousandItemsMilliseconds);

        if (initialDelay > TimeSpan.Zero || jitterMs > 0)
        {
            var actualDelay = initialDelay;
            if (jitterMs > 0)
            {
                actualDelay = actualDelay.Add(TimeSpan.FromMilliseconds(Random.Shared.Next(0, jitterMs + 1)));
            }
            await Task.Delay(actualDelay, stoppingToken).ConfigureAwait(false);
        }

        var startTimestamp = Stopwatch.GetTimestamp();

        while (!stoppingToken.IsCancellationRequested)
        {
            var scopes = scopeManager.GetActiveScopes();
            long maxPendingItemsAcrossScopes = 0;
            int maxQueueDepthAcrossScopes = 0;

            foreach (var scope in scopes)
            {
                try
                {
                    var workload = await scope.Orchestrator.EvaluateAndDispatchAntiEntropyStateAsync(stoppingToken).ConfigureAwait(false);
                    if (workload.EstimatedPendingSyncItems > maxPendingItemsAcrossScopes)
                    {
                        maxPendingItemsAcrossScopes = workload.EstimatedPendingSyncItems;
                    }

                    if (workload.PendingQueueDepth > maxQueueDepthAcrossScopes)
                    {
                        maxQueueDepthAcrossScopes = workload.PendingQueueDepth;
                    }

                    if (workload.IsBusy)
                    {
                        antiEntropySkippedCyclesCounter.Add(1, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
                    }
                    else
                    {
                        antiEntropyCyclesCounter.Add(1, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "[{ReplicaId}] An error occurred during CRDT targeted anti-entropy state dispatch.", scope.ReplicaId);
                }
            }

            try
            {
                var elapsed = Stopwatch.GetElapsedTime(startTimestamp);
                var baseInterval = elapsed < startupDuration ? startupInterval : normalInterval;
                var currentInterval = ComputeAdaptiveInterval(
                    baseInterval,
                    maxAdaptiveInterval,
                    delayPerThousandMs,
                    maxPendingItemsAcrossScopes,
                    maxQueueDepthAcrossScopes);

                await Task.Delay(currentInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    public override void Dispose()
    {
        meter.Dispose();
        base.Dispose();
    }

    private static TimeSpan ComputeAdaptiveInterval(
        TimeSpan baseInterval,
        TimeSpan maxInterval,
        int delayPerThousandMs,
        long estimatedPendingSyncItems,
        int pendingQueueDepth)
    {
        if (delayPerThousandMs <= 0)
        {
            return baseInterval;
        }

        var totalPendingWork = Math.Max(estimatedPendingSyncItems, pendingQueueDepth);
        if (totalPendingWork <= 0)
        {
            return baseInterval;
        }

        var extraMs = (totalPendingWork * delayPerThousandMs) / 1000.0;
        if (extraMs <= 0)
        {
            return baseInterval;
        }

        var candidate = baseInterval.Add(TimeSpan.FromMilliseconds(extraMs));
        if (maxInterval > baseInterval && candidate > maxInterval)
        {
            return maxInterval;
        }

        return candidate;
    }
}