namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Background service responsible for repeatedly broadcasting the local synchronization state iterating globally natively.
/// </summary>
public sealed class CrdtAntiEntropyService : BackgroundService
{
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtAntiEntropyService> logger;

    private readonly Meter meter;
    private readonly Counter<long> antiEntropyCyclesCounter;

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
        this.antiEntropyCyclesCounter = this.meter.CreateCounter<long>("crdt.anti_entropy.cycles_executed", "cycles", "Total localized periodic anti-entropy generic interactions dispatched natively");
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var initialDelay = TimeSpan.FromSeconds(Math.Max(0, options.Value.AntiEntropyInitialDelaySeconds));
        var interval = TimeSpan.FromSeconds(Math.Max(1, options.Value.AntiEntropyIntervalSeconds));

        if (initialDelay > TimeSpan.Zero)
        {
            await Task.Delay(initialDelay, stoppingToken).ConfigureAwait(false);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            var scopes = scopeManager.GetActiveScopes();
            
            foreach (var scope in scopes)
            {
                try
                {
                    await scope.Orchestrator.DispatchAntiEntropyStateAsync(stoppingToken).ConfigureAwait(false);
                    antiEntropyCyclesCounter.Add(1, new System.Collections.Generic.KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
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
                await Task.Delay(interval, stoppingToken).ConfigureAwait(false);
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
}