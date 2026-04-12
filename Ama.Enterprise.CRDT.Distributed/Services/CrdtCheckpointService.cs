namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Linq;
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
/// Background service responsible for periodically saving the full in-memory state of all registered CRDTs to persistent storage
/// and explicitly trimming the underlying operational journals mapping across global cluster bounds to ensure overall consistency seamlessly.
/// </summary>
public sealed class CrdtCheckpointService : BackgroundService
{
    private readonly DistributedCrdtScopeProvider scopeProvider;
    private readonly IClusterStateTracker clusterTracker;
    private readonly IDistributedCrdtStorage storage;
    private readonly IVersionVectorSyncService syncService;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtCheckpointService> logger;

    public CrdtCheckpointService(
        DistributedCrdtScopeProvider scopeProvider,
        IClusterStateTracker clusterTracker,
        IDistributedCrdtStorage storage,
        IVersionVectorSyncService syncService,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtCheckpointService> logger)
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

            try
            {
                var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();
                var documents = scopeProvider.Scope.ServiceProvider.GetRequiredService<IEnumerable<IDistributedCrdtDocument>>().ToList();
                
                if (options.Value.PeerEvictionTtlSeconds > 0)
                {
                    var evictionTtl = TimeSpan.FromSeconds(options.Value.PeerEvictionTtlSeconds);
                    
                    // Replicas evicted this way are tombstoned correctly inherently preventing causal structural amnesia gracefully
                    var tombstonedPeers = clusterTracker.GetAndTombstoneExpiredPeers(evictionTtl);

                    if (tombstonedPeers.Count > 0)
                    {
                        logger.LogInformation("Tombstoned {Count} dead peers based on TTL threshold ({TtlSeconds}s) preventing network log bound halts.", tombstonedPeers.Count, options.Value.PeerEvictionTtlSeconds);

                        foreach (var document in documents)
                        {
                            foreach (var peerId in tombstonedPeers)
                            {
                                await document.EvictReplicaAsync(peerId, stoppingToken).ConfigureAwait(false);
                            }
                        }

                        var sourceDvvForEviction = replicaContext.GlobalVersionVector;
                        lock (sourceDvvForEviction)
                        {
                            var cleanedDvv = syncService.RemoveEvictedReplicas(sourceDvvForEviction, tombstonedPeers);
                            
                            sourceDvvForEviction.Versions.Clear();
                            foreach (var kvp in cleanedDvv.Versions)
                            {
                                sourceDvvForEviction.Versions[kvp.Key] = kvp.Value;
                            }
                            
                            sourceDvvForEviction.Dots.Clear();
                            if (cleanedDvv.Dots != null)
                            {
                                foreach (var kvp in cleanedDvv.Dots)
                                {
                                    sourceDvvForEviction.Dots[kvp.Key] = kvp.Value;
                                }
                            }
                        }
                    }
                }

                // 1. Capture a safe, isolated snapshot of the current local DVV so we don't trim operations we haven't flushed to disk yet
                var sourceDvv = replicaContext.GlobalVersionVector;
                var copiedVersions = new Dictionary<string, long>();
                var copiedDots = new Dictionary<string, ISet<long>>();
                
                // Lock inherently required to prevent InvalidOperationExceptions due to concurrent background sync modification payloads naturally
                lock (sourceDvv)
                {
                    foreach (var kvp in sourceDvv.Versions)
                    {
                        copiedVersions[kvp.Key] = kvp.Value;
                    }
                    foreach (var kvp in sourceDvv.Dots)
                    {
                        copiedDots[kvp.Key] = new HashSet<long>(kvp.Value);
                    }
                }
                
                var safelyPersistedDvv = new DottedVersionVector(copiedVersions, copiedDots);

                // 2. Save document states FIRST (Idempotency safety: Docs advance before the overarching watermark preventing log gap data loss)
                foreach (var document in documents)
                {
                    await document.CheckpointAsync(stoppingToken).ConfigureAwait(false);
                }

                // 3. Persist the overarching Global Version Vector explicitly after underlying documents inherently succeed securely
                await storage.SaveGlobalVersionVectorAsync(replicaContext.ReplicaId, safelyPersistedDvv, stoppingToken).ConfigureAwait(false);

                // 4. Perform mathematically safe journal trimming mapped explicitly using the exact snapshot bounds we just successfully stored
                var clusterStates = new List<DottedVersionVector>(clusterTracker.GetClusterStates())
                {
                    safelyPersistedDvv
                };

                if (clusterStates.Count > 0)
                {
                    var gmvv = syncService.CalculateGlobalMinimumVersionVector(clusterStates);

                    if (gmvv.Count > 0)
                    {
                        await storage.TrimAsync(gmvv, stoppingToken).ConfigureAwait(false);
                        logger.LogTrace("Executed background mathematically safe journal trim matching overarching Global Minimum Version Vector bounds natively securely.");
                    }
                }
            }
            catch (Exception ex)
            {
                // If any document or DVV persistence strictly throws, we intentionally abort the trimming process securely.
                logger.LogError(ex, "An error occurred during periodic CRDT checkpointing and journal trimming. Cycle aborted strictly ensuring data consistency.");
            }
        }
    }
}