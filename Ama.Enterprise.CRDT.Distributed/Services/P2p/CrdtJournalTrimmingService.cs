namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Versioning;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Background service responsible for actively monitoring the entire connected cluster's combined bounds.
/// Calculates the Global Minimum Version Vector (GMVV) safely and trims the underlying journal accordingly securely dynamically.
/// </summary>
public sealed class CrdtJournalTrimmingService : BackgroundService
{
    private readonly DistributedCrdtScopeProvider scopeProvider;
    private readonly IClusterStateTracker clusterTracker;
    private readonly IDistributedCrdtStorage storage;
    private readonly IVersionVectorSyncService syncService;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtJournalTrimmingService> logger;

    public CrdtJournalTrimmingService(
        DistributedCrdtScopeProvider scopeProvider,
        IClusterStateTracker clusterTracker,
        IDistributedCrdtStorage storage,
        IVersionVectorSyncService syncService,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtJournalTrimmingService> logger)
    {
        this.scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
        this.clusterTracker = clusterTracker ?? throw new ArgumentNullException(nameof(clusterTracker));
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        this.syncService = syncService ?? throw new ArgumentNullException(nameof(syncService));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var initialDelay = TimeSpan.FromSeconds(Math.Max(0, options.Value.JournalTrimmingInitialDelaySeconds));
        var interval = TimeSpan.FromSeconds(Math.Max(1, options.Value.JournalTrimmingIntervalSeconds));

        if (initialDelay > TimeSpan.Zero)
        {
            // Initial delay to allow networks to aggressively sync the very first bounds before truncating actively natively.
            await Task.Delay(initialDelay, stoppingToken).ConfigureAwait(false);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();
                
                // Aggregating our own explicit bounds natively mapping alongside the remote retrieved bounds mappings implicitly
                var clusterStates = new List<DottedVersionVector>(clusterTracker.GetClusterStates())
                {
                    replicaContext.GlobalVersionVector
                };

                if (clusterStates.Count > 0)
                {
                    // Compute the safe boundary where ALL currently connected replicas implicitly definitively possess operations
                    var gmvv = syncService.CalculateGlobalMinimumVersionVector(clusterStates);

                    if (gmvv.Count > 0)
                    {
                        // Securely execute trimming mappings dynamically against explicit safe constraints completely independently.
                        await storage.TrimAsync(gmvv, stoppingToken).ConfigureAwait(false);
                        logger.LogTrace("Executed background mathematical journal trim matching cross-cluster overarching Global Minimum Version Vector bounds natively securely.");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred during CRDT background mathematically safe journal trimming process mapping bounds natively.");
            }

            try
            {
                // Delay between trimming evaluations
                await Task.Delay(interval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}