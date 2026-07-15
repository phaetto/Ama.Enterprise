namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Background service responsible for systematically persisting memory states.
/// </summary>
public sealed class CrdtCheckpointService : BackgroundService
{
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtCheckpointService> logger;

    private readonly Meter meter;
    private readonly Counter<long> checkpointCyclesCounter;

    private readonly Dictionary<string, DottedVersionVector> lastKnownDvvCache = new();
    private readonly Dictionary<string, ClusterStateSnapshotDto> lastKnownClusterStateCache = new();

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
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var intervalSeconds = options.Value.CheckpointIntervalSeconds;
        var delay = intervalSeconds > 0 ? TimeSpan.FromSeconds(intervalSeconds) : TimeSpan.FromSeconds(30);
        var avoidBlindWrites = options.Value.AvoidBlindCheckpointWrites;

        logger.LogInformation("CRDT background checkpoint service started with an interval of {Seconds} seconds. AvoidBlindWrites: {AvoidBlindWrites}", delay.TotalSeconds, avoidBlindWrites);

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
                    var exportedClusterState = scope.ClusterTracker.ExportState();

                    var dvvChanged = true;
                    var clusterStateChanged = true;

                    if (avoidBlindWrites)
                    {
                        if (lastKnownDvvCache.TryGetValue(scope.ReplicaId, out var cachedDvv))
                        {
                            dvvChanged = !AreDvvsEqual(cachedDvv, safelyPersistedDvv);
                        }

                        if (lastKnownClusterStateCache.TryGetValue(scope.ReplicaId, out var cachedState))
                        {
                            clusterStateChanged = !cachedState.Equals(exportedClusterState);
                        }
                    }

                    foreach (var document in documents)
                    {
                        await document.CheckpointAsync(stoppingToken).ConfigureAwait(false);
                    }

                    if (dvvChanged || !avoidBlindWrites)
                    {
                        await scope.Storage.SaveGlobalVersionVectorAsync(scope.ReplicaId, safelyPersistedDvv, stoppingToken).ConfigureAwait(false);
                        if (avoidBlindWrites)
                        {
                            lastKnownDvvCache[scope.ReplicaId] = safelyPersistedDvv;
                        }
                    }

                    if (clusterStateChanged || !avoidBlindWrites)
                    {
                        await scope.Storage.SaveClusterStateAsync(scope.ReplicaId, exportedClusterState, stoppingToken).ConfigureAwait(false);
                        if (avoidBlindWrites)
                        {
                            lastKnownClusterStateCache[scope.ReplicaId] = exportedClusterState;
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "[{ReplicaId}] An error occurred during periodic CRDT checkpointing. Cycle aborted.", scope.ReplicaId);
                }
            }
        }
    }

    public override void Dispose()
    {
        meter.Dispose();
        base.Dispose();
    }

    private static bool AreDvvsEqual(DottedVersionVector? a, DottedVersionVector? b)
    {
        if (ReferenceEquals(a, b)) return true;
        if (a is null || b is null) return false;

        if (a.Versions.Count != b.Versions.Count) return false;
        
        foreach (var kvp in a.Versions)
        {
            if (!b.Versions.TryGetValue(kvp.Key, out var bVal) || kvp.Value != bVal)
            {
                return false;
            }
        }

        if (a.Dots is null && b.Dots is null) return true;
        if (a.Dots is null || b.Dots is null) return false;

        if (a.Dots.Count != b.Dots.Count) return false;
        
        foreach (var kvp in a.Dots)
        {
            if (!b.Dots.TryGetValue(kvp.Key, out var bSet)) return false;
            if (kvp.Value.Count != bSet.Count) return false;
            
            foreach (var dot in kvp.Value)
            {
                if (!bSet.Contains(dot)) return false;
            }
        }

        return true;
    }
}