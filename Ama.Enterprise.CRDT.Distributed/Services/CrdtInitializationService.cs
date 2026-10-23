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
/// Hosted service responsible for initializing distributed CRDT replica scopes on application startup.
/// </summary>
public sealed class CrdtInitializationService : IHostedService, IDisposable
{
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly ILogger<CrdtInitializationService> logger;
    private readonly IHostApplicationLifetime? hostApplicationLifetime;

    private readonly Meter meter;
    private readonly Counter<long> replayedOperationsCounter;
    private readonly Counter<long> globalDvvRestoredCounter;

    public CrdtInitializationService(
        DistributedCrdtScopeManager scopeManager,
        ILogger<CrdtInitializationService> logger,
        IMeterFactory? meterFactory = null,
        IHostApplicationLifetime? hostApplicationLifetime = null)
    {
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.hostApplicationLifetime = hostApplicationLifetime;

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
            var options = scope.ServiceProvider.GetRequiredService<IOptions<DistributedCrdtOptions>>().Value;
            var maxRetries = Math.Max(1, options.InitializationMaxRetries);
            var retryDelayMs = Math.Max(0, options.InitializationRetryDelayMilliseconds);
            Exception? lastException = null;

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    await InitializeScopeAsync(scope, options, cancellationToken).ConfigureAwait(false);
                    lastException = null;
                    break;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    if (attempt < maxRetries)
                    {
                        logger.LogWarning(
                            ex,
                            "Attempt {Attempt} of {MaxRetries} failed while initializing distributed CRDT documents for replica {ReplicaId}. Retrying...",
                            attempt,
                            maxRetries,
                            scope.ReplicaId);

                        if (retryDelayMs > 0)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(retryDelayMs * attempt), cancellationToken).ConfigureAwait(false);
                        }
                    }
                }
            }

            if (lastException != null)
            {
                logger.LogCritical(
                    lastException,
                    "Failed to initialize distributed CRDT documents for replica {ReplicaId} after {MaxRetries} attempts. Terminating application startup.",
                    scope.ReplicaId,
                    maxRetries);

                hostApplicationLifetime?.StopApplication();
                throw new InvalidOperationException(
                    $"Failed to initialize distributed CRDT documents for replica '{scope.ReplicaId}' after {maxRetries} attempts.",
                    lastException);
            }
        }
        
        logger.LogInformation("Distributed CRDT replica matrices initialized.");
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

    private async Task InitializeScopeAsync(
        IDistributedCrdtScope scope,
        DistributedCrdtOptions options,
        CancellationToken cancellationToken)
    {
        var globalStorage = scope.Storage;
        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        var versionVectorManager = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        var tombstoneCooldown = TimeSpan.FromSeconds(options.PeerTombstoneCooldownSeconds);

        // 0. Recover any uncommitted local disk WAL segments into the primary storage before loading checkpoints or replaying journals
        if (globalStorage is BufferedDistributedCrdtStorageDecorator bufferedDecorator)
        {
            await bufferedDecorator.WriteAheadBuffer.RecoverAsync(bufferedDecorator.InnerStorage, cancellationToken).ConfigureAwait(false);
        }

        // 1. Initialize the global Dotted Version Vector from persistent storage
        var savedMap = await globalStorage.LoadShardVersionVectorMapAsync(replicaContext.ReplicaId, cancellationToken).ConfigureAwait(false);
        if (savedMap != null)
        {
            versionVectorManager.RestoreFullState(savedMap);
            
            globalDvvRestoredCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaContext.ReplicaId));
            logger.LogInformation("Re-initialized in-place CRDT global Dotted Version Vector for replica {ReplicaId}.", replicaContext.ReplicaId);
        }

        // 2. Load preserved cluster tracking state and prune expired tombstones
        var savedClusterState = await globalStorage.LoadClusterStateAsync(replicaContext.ReplicaId, cancellationToken).ConfigureAwait(false);
        if (savedClusterState != null)
        {
            scope.ClusterTracker.InitializeState(savedClusterState, tombstoneCooldown);
            logger.LogInformation("Re-initialized in-place cluster tracking state matrix for replica {ReplicaId}.", replicaContext.ReplicaId);
        }

        // 3. Initialize orchestrator and internal registry
        var orchestrator = scope.Orchestrator;
        await orchestrator.InitializeAsync(cancellationToken).ConfigureAwait(false);

        // 4. Replay uncheckpointed WAL operations
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