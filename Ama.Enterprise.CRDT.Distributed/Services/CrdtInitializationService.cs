namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
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
/// Hosted service responsible for isolating uncoupled replicas sequentially natively dynamically binding their discrete initial persistence scopes.
/// </summary>
public sealed class CrdtInitializationService : IHostedService, IDisposable
{
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly ILogger<CrdtInitializationService> logger;

    private readonly Meter meter;
    private readonly Counter<long> replayedOperationsCounter;
    private readonly Counter<long> globalDvvRestoredCounter;

    public CrdtInitializationService(
        DistributedCrdtScopeManager scopeManager,
        ILogger<CrdtInitializationService> logger,
        IMeterFactory? meterFactory = null)
    {
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtInitializationService") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtInitializationService");
        this.replayedOperationsCounter = this.meter.CreateCounter<long>("crdt.initialization.replayed_operations", "operations", "Total journaled operations actively replayed restoring states");
        this.globalDvvRestoredCounter = this.meter.CreateCounter<long>("crdt.initialization.global_dvv_restored", "events", "Total overarching cluster global bounds restored natively");
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Initializing distributed CRDT global state and dynamically orchestrated documents...");

        var scopes = scopeManager.GetActiveScopes();
        foreach (var scope in scopes)
        {
            try
            {
                var globalStorage = scope.Storage;
                var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
                var versionVectorManager = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
                
                var options = scope.ServiceProvider.GetRequiredService<IOptions<DistributedCrdtOptions>>().Value;
                var tombstoneCooldown = TimeSpan.FromSeconds(options.PeerTombstoneCooldownSeconds);

                // 0. Recover any uncommitted local disk WAL segments into the primary storage before loading checkpoints or replaying journals
                if (globalStorage is BufferedDistributedCrdtStorageDecorator bufferedDecorator)
                {
                    await bufferedDecorator.WriteAheadBuffer.RecoverAsync(bufferedDecorator.InnerStorage, cancellationToken).ConfigureAwait(false);
                }

                // 1. Initialize the global Dotted Version Vector explicitly extracting persistent DVV natively
                var savedMap = await globalStorage.LoadShardVersionVectorMapAsync(replicaContext.ReplicaId, cancellationToken).ConfigureAwait(false);
                if (savedMap != null)
                {
                    versionVectorManager.RestoreFullState(savedMap);
                    
                    globalDvvRestoredCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaContext.ReplicaId));
                    logger.LogInformation("Successfully re-initialized in-place CRDT global Dotted Version Vector for replica {ReplicaId}.", replicaContext.ReplicaId);
                }

                // 2. Load globally preserved tracking matrices restoring isolated tracking states securely and pruning expired limits
                var savedClusterState = await globalStorage.LoadClusterStateAsync(replicaContext.ReplicaId, cancellationToken).ConfigureAwait(false);
                if (savedClusterState != null)
                {
                    scope.ClusterTracker.ImportState(savedClusterState, tombstoneCooldown);
                    logger.LogInformation("Successfully re-initialized in-place cluster tracking state matrix for replica {ReplicaId}.", replicaContext.ReplicaId);
                }

                // 3. Initialize orchestrator limits natively bounding internal registry architectures
                var orchestrator = scope.Orchestrator;
                await orchestrator.InitializeAsync(cancellationToken).ConfigureAwait(false);

                // 4. Replay uncheckpointed WAL boundaries seamlessly projecting generic recovery logic directly
                var operationsByDocAndShard = new Dictionary<(string DocKey, string ShardId), List<CrdtOperation>>();
                
                await foreach (var jOp in globalStorage.GetAllJournaledOperationsAsync(cancellationToken).ConfigureAwait(false))
                {
                    var docIdStr = jOp.DocumentId?.ToString() ?? string.Empty;
                    var shardIdStr = jOp.Operation.ShardId ?? string.Empty;
                    var key = (docIdStr, shardIdStr);
                    if (!operationsByDocAndShard.TryGetValue(key, out var opList))
                    {
                        opList = new List<CrdtOperation>();
                        operationsByDocAndShard[key] = opList;
                    }
                    opList.Add(jOp.Operation);
                }

                if (operationsByDocAndShard.Count > 0)
                {
                    var totalReplayed = operationsByDocAndShard.Values.Sum(l => l.Count);
                    replayedOperationsCounter.Add(totalReplayed, new KeyValuePair<string, object?>("replica_id", replicaContext.ReplicaId));
                    logger.LogInformation("Replaying {Count} journaled operations for replica {ReplicaId}.", totalReplayed, replicaContext.ReplicaId);
                    
                    var registryOps = GetMatchingOperations(operationsByDocAndShard, orchestrator.Registry);
                    if (registryOps.Count > 0)
                    {
                        await orchestrator.Registry.ApplyJournaledPatchAsync(new CrdtPatch(registryOps), cancellationToken).ConfigureAwait(false);
                        await orchestrator.ReevaluateTopologyAsync(cancellationToken).ConfigureAwait(false);
                    }

                    var documents = orchestrator.GetActiveDocuments();
                    
                    foreach (var document in documents)
                    {
                        if (document.DocumentId != orchestrator.Registry.DocumentId)
                        {
                            var docOps = GetMatchingOperations(operationsByDocAndShard, document);
                            if (docOps.Count > 0)
                            {
                                await document.ApplyJournaledPatchAsync(new CrdtPatch(docOps), cancellationToken).ConfigureAwait(false);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred while initializing distributed CRDT documents for replica {ReplicaId}.", scope.ReplicaId);
            }
        }
        
        logger.LogInformation("Distributed CRDT replica matrices successfully initialized.");
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        meter.Dispose();
    }

    private static List<CrdtOperation> GetMatchingOperations(
        Dictionary<(string DocKey, string ShardId), List<CrdtOperation>> operationsByDocAndShard,
        IDistributedCrdtDocument document)
    {
        var compositeKey = CrdtRegistryEntry.GetRegistryKey(document.TypeAlias, document.DocumentId);

        if (operationsByDocAndShard.TryGetValue((compositeKey, document.ShardId), out var exactCompositeOps))
        {
            return exactCompositeOps;
        }

        if (operationsByDocAndShard.TryGetValue((document.DocumentId, document.ShardId), out var exactShardOps))
        {
            return exactShardOps;
        }

        if (operationsByDocAndShard.TryGetValue((compositeKey, string.Empty), out var fallbackCompositeOps))
        {
            return fallbackCompositeOps;
        }

        if (operationsByDocAndShard.TryGetValue((document.DocumentId, string.Empty), out var fallbackDocOps))
        {
            return fallbackDocOps;
        }

        return [];
    }
}