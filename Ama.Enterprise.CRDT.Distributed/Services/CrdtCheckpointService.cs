namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
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
/// Background service responsible for systematically persisting memory states targeting mapped independent multi-tenant boundaries structurally dynamically preventing bounds amnesia effectively.
/// </summary>
public sealed class CrdtCheckpointService : BackgroundService
{
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtCheckpointService> logger;

    private readonly Meter meter;
    private readonly Counter<long> checkpointCyclesCounter;
    private readonly Counter<long> trimmedJournalsCounter;
    private readonly Counter<long> evictedPeersCounter;

    public CrdtCheckpointService(
        DistributedCrdtScopeManager scopeManager,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtCheckpointService> logger,
        IMeterFactory? meterFactory = null)
    {
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtCheckpointService") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtCheckpointService");
        this.checkpointCyclesCounter = this.meter.CreateCounter<long>("crdt.checkpoint.cycles", "cycles", "Total background checkpoint cycles executed");
        this.trimmedJournalsCounter = this.meter.CreateCounter<long>("crdt.checkpoint.trimmed_journals", "trims", "Total journal trims executed based on GMVV bounds");
        this.evictedPeersCounter = this.meter.CreateCounter<long>("crdt.checkpoint.evicted_peers", "peers", "Total dead peers actively tombstoned due to TTL");
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

            var scopes = scopeManager.GetActiveScopes();

            foreach (var scope in scopes)
            {
                try
                {
                    var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
                    var orchestrator = scope.Orchestrator;
                    var documents = orchestrator.GetActiveDocuments();
                    
                    var tag = new KeyValuePair<string, object?>("replica_id", scope.ReplicaId);
                    checkpointCyclesCounter.Add(1, tag);

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

                    var exportedClusterState = scope.ClusterTracker.ExportState();
                    await scope.Storage.SaveClusterStateAsync(scope.ReplicaId, exportedClusterState, stoppingToken).ConfigureAwait(false);

                    long journalCount = 0;
                    if (options.Value.JournalTrimThreshold > 0)
                    {
                        journalCount = await scope.Storage.GetJournalCountAsync(stoppingToken).ConfigureAwait(false);
                    }

                    bool trimThresholdExceeded = options.Value.JournalTrimThreshold > 0 && journalCount >= options.Value.JournalTrimThreshold;

                    if (trimThresholdExceeded)
                    {
                        // Force a journal trim up to the local safely persisted DVV, intentionally dropping lagging peers to fallback snapshots
                        if (safelyPersistedDvv.Versions.Count > 0)
                        {
                            await scope.Storage.TrimAsync(safelyPersistedDvv.Versions.ToDictionary(), stoppingToken).ConfigureAwait(false);
                            trimmedJournalsCounter.Add(1, tag);
                            logger.LogWarning("[{ReplicaId}] Journal size ({Count}) exceeded threshold ({Threshold}). Forced an aggressive journal trim up to the local logical bounds.", scope.ReplicaId, journalCount, options.Value.JournalTrimThreshold);
                        }
                    }
                    else
                    {
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
                                await scope.Storage.TrimAsync(gmvv.ToDictionary(), stoppingToken).ConfigureAwait(false);
                                trimmedJournalsCounter.Add(1, tag);
                                logger.LogTrace("[{ReplicaId}] Executed background journal trim matching Global Minimum Version Vector bounds.", scope.ReplicaId);
                            }
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

    public override void Dispose()
    {
        meter.Dispose();
        base.Dispose();
    }
}