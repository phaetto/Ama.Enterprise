namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
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
/// Background service responsible for systematically evaluating CRDT network state boundaries, evicting dead peers, and aggressively trimming unbounded multi-mesh journal backlogs based on GMVV.
/// </summary>
public sealed class CrdtMaintenanceService : BackgroundService
{
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtMaintenanceService> logger;

    private readonly Meter meter;
    private readonly Counter<long> maintenanceCyclesCounter;
    private readonly Counter<long> trimmedJournalsCounter;
    private readonly Counter<long> evictedPeersCounter;

    public CrdtMaintenanceService(
        DistributedCrdtScopeManager scopeManager,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtMaintenanceService> logger,
        IMeterFactory? meterFactory = null)
    {
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtMaintenanceService") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtMaintenanceService");
        this.maintenanceCyclesCounter = this.meter.CreateCounter<long>("crdt.maintenance.cycles", "cycles", "Total background maintenance cycles executed");
        this.trimmedJournalsCounter = this.meter.CreateCounter<long>("crdt.maintenance.trimmed_journals", "trims", "Total journal trims executed based on local thresholds or GMVV bounds");
        this.evictedPeersCounter = this.meter.CreateCounter<long>("crdt.maintenance.evicted_peers", "peers", "Total dead peers actively tombstoned due to TTL");
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var intervalSeconds = options.Value.MaintenanceIntervalSeconds;
        var delay = intervalSeconds > 0 ? TimeSpan.FromSeconds(intervalSeconds) : TimeSpan.FromSeconds(60);

        logger.LogInformation("CRDT background maintenance and trimming service started with an interval of {Seconds} seconds.", delay.TotalSeconds);

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
                    var topologyProvider = scope.ServiceProvider.GetRequiredService<IScopeTopologyProvider>();
                    
                    var tag = new KeyValuePair<string, object?>("replica_id", scope.ReplicaId);
                    maintenanceCyclesCounter.Add(1, tag);

                    if (options.Value.PeerEvictionTtlSeconds > 0)
                    {
                        var evictionTtl = TimeSpan.FromSeconds(options.Value.PeerEvictionTtlSeconds);
                        var tombstonedPeers = scope.ClusterTracker.GetAndTombstoneExpiredPeers(evictionTtl);

                        if (tombstonedPeers.Count > 0)
                        {
                            evictedPeersCounter.Add(tombstonedPeers.Count, tag);
                            logger.LogInformation("[{ReplicaId}] Tombstoned {Count} dead peers based on TTL threshold ({TtlSeconds}s) preventing network log bound halts.", scope.ReplicaId, tombstonedPeers.Count, options.Value.PeerEvictionTtlSeconds);
                            await scope.EvictionService.EvictPeersAsync(tombstonedPeers, stoppingToken).ConfigureAwait(false);
                        }
                    }

                    if (options.Value.PeerTombstoneCooldownSeconds > 0)
                    {
                        var cooldown = TimeSpan.FromSeconds(options.Value.PeerTombstoneCooldownSeconds);
                        scope.ClusterTracker.CleanupExpiredTombstones(cooldown);
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
                    
                    var currentLocalDvv = new DottedVersionVector(copiedVersions, copiedDots);

                    long journalCount = 0;
                    var softThreshold = options.Value.JournalSoftTrimThreshold;
                    var hardThreshold = options.Value.JournalHardTrimThreshold;

                    if (softThreshold > 0 || hardThreshold > 0)
                    {
                        journalCount = await scope.Storage.GetJournalCountAsync(stoppingToken).ConfigureAwait(false);
                    }

                    bool trimThresholdExceeded = (softThreshold > 0 && journalCount >= softThreshold) ||
                                                 (hardThreshold > 0 && journalCount >= hardThreshold);

                    if (trimThresholdExceeded)
                    {
                        // Force a journal trim up to the local logical bounds, intentionally dropping lagging peers to fallback snapshots
                        if (currentLocalDvv.Versions.Count > 0)
                        {
                            await CrdtTrimCoordinator.GlobalTrimLock.WaitAsync(stoppingToken).ConfigureAwait(false);
                            try
                            {
                                await scope.Storage.TrimAsync(currentLocalDvv.Versions.ToDictionary(), stoppingToken).ConfigureAwait(false);
                                trimmedJournalsCounter.Add(1, tag);
                                
                                var exceededThreshold = (softThreshold > 0 && journalCount >= softThreshold) ? softThreshold : hardThreshold;
                                logger.LogWarning("[{ReplicaId}] Journal size ({Count}) exceeded threshold ({Threshold}). Forced an aggressive journal trim up to the local logical bounds.", scope.ReplicaId, journalCount, exceededThreshold);
                            }
                            finally
                            {
                                CrdtTrimCoordinator.GlobalTrimLock.Release();
                            }
                        }
                    }
                    else
                    {
                        var exportedClusterState = scope.ClusterTracker.ExportState();
                        var validClusterStates = new List<DottedVersionVector> { currentLocalDvv };

                        foreach (var networkKvp in exportedClusterState.NetworkIdToReplicaId)
                        {
                            var isExpected = await topologyProvider.IsPeerExpectedAsync(networkKvp.Key, stoppingToken).ConfigureAwait(false);
                            if (isExpected)
                            {
                                if (exportedClusterState.PeerStates.TryGetValue(networkKvp.Value, out var peerStateDto))
                                {
                                    validClusterStates.Add(peerStateDto.State);
                                }
                            }
                        }

                        if (validClusterStates.Count > 0)
                        {
                            var syncService = scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
                            var gmvv = syncService.CalculateGlobalMinimumVersionVector(validClusterStates);

                            if (gmvv.Count > 0)
                            {
                                await CrdtTrimCoordinator.GlobalTrimLock.WaitAsync(stoppingToken).ConfigureAwait(false);
                                try
                                {
                                    await scope.Storage.TrimAsync(gmvv.ToDictionary(), stoppingToken).ConfigureAwait(false);
                                    trimmedJournalsCounter.Add(1, tag);
                                    logger.LogTrace("[{ReplicaId}] Executed background journal trim matching Global Minimum Version Vector bounds across expected topology peers.", scope.ReplicaId);
                                }
                                finally
                                {
                                    CrdtTrimCoordinator.GlobalTrimLock.Release();
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "[{ReplicaId}] An error occurred during periodic CRDT journal trimming and maintenance. Cycle aborted.", scope.ReplicaId);
                }
            }
        }
    }

    public override void Dispose()
    {
        meter.Dispose();
        base.Dispose();
    }
}