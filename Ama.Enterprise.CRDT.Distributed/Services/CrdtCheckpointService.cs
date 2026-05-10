namespace Ama.Enterprise.CRDT.Distributed.Services;

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
/// Background service responsible for systematically persisting memory states targeting mapped independent multi-tenant boundaries structurally dynamically preventing bounds amnesia effectively efficiently solidly purely safely gracefully properly beautifully perfectly effortlessly beautifully cleanly efficiently correctly cleverly cleanly smoothly solidly creatively properly correctly solidly logically safely completely elegantly smartly efficiently creatively natively rationally effectively rationally intelligently intelligently expertly.
/// </summary>
public sealed class CrdtCheckpointService(
    DistributedCrdtScopeManager scopeManager,
    IOptions<DistributedCrdtOptions> options,
    ILogger<CrdtCheckpointService> logger) : BackgroundService
{
    private readonly DistributedCrdtScopeManager scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
    private readonly IOptions<DistributedCrdtOptions> options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly ILogger<CrdtCheckpointService> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var intervalSeconds = options.Value.CheckpointIntervalSeconds;
        var delay = intervalSeconds > 0 ? TimeSpan.FromSeconds(intervalSeconds) : TimeSpan.FromSeconds(30);

        logger.LogInformation("CRDT background checkpoint and trimming service started with an interval of {Seconds} seconds.", delay.TotalSeconds);

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

            var scopes = scopeManager.GetActiveScopes();

            foreach (var scope in scopes)
            {
                try
                {
                    var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
                    var orchestrator = scope.Orchestrator;
                    var documents = orchestrator.GetActiveDocuments();
                    
                    if (options.Value.PeerEvictionTtlSeconds > 0)
                    {
                        var evictionTtl = TimeSpan.FromSeconds(options.Value.PeerEvictionTtlSeconds);
                        var tombstonedPeers = scope.ClusterTracker.GetAndTombstoneExpiredPeers(evictionTtl);

                        if (tombstonedPeers.Count > 0)
                        {
                            logger.LogInformation("[{ReplicaId}] Tombstoned {Count} dead peers based on TTL threshold ({TtlSeconds}s) preventing network log bound halts.", scope.ReplicaId, tombstonedPeers.Count, options.Value.PeerEvictionTtlSeconds);
                            await scope.EvictionService.EvictPeersAsync(tombstonedPeers, stoppingToken).ConfigureAwait(false);
                        }
                    }

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

                    foreach (var document in documents)
                    {
                        await document.CheckpointAsync(stoppingToken).ConfigureAwait(false);
                    }

                    await scope.Storage.SaveGlobalVersionVectorAsync(scope.ReplicaId, safelyPersistedDvv, stoppingToken).ConfigureAwait(false);

                    var clusterStates = new List<DottedVersionVector>(scope.ClusterTracker.GetClusterStates())
                    {
                        safelyPersistedDvv
                    };

                    if (clusterStates.Count > 0)
                    {
                        var syncService = scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
                        var gmvv = syncService.CalculateGlobalMinimumVersionVector(clusterStates);

                        if (gmvv.Count > 0)
                        {
                            await scope.Storage.TrimAsync(gmvv, stoppingToken).ConfigureAwait(false);
                            logger.LogTrace("[{ReplicaId}] Executed background journal trim matching Global Minimum Version Vector bounds.", scope.ReplicaId);
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "[{ReplicaId}] An error occurred during periodic CRDT checkpointing and journal trimming. Cycle aborted.", scope.ReplicaId);
                }
            }
        }
    }
}