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
                
                var options = scope.ServiceProvider.GetRequiredService<IOptions<DistributedCrdtOptions>>().Value;
                var tombstoneCooldown = TimeSpan.FromSeconds(options.PeerTombstoneCooldownSeconds);

                // 1. Initialize the global Dotted Version Vector explicitly extracting persistent DVV naturally
                var savedDvv = await globalStorage.LoadGlobalVersionVectorAsync(replicaContext.ReplicaId, cancellationToken).ConfigureAwait(false);
                if (savedDvv != null)
                {
                    lock (replicaContext.GlobalVersionVector)
                    {
                        replicaContext.GlobalVersionVector.Versions.Clear();
                        foreach (var kvp in savedDvv.Versions)
                        {
                            replicaContext.GlobalVersionVector.Versions[kvp.Key] = kvp.Value;
                        }

                        replicaContext.GlobalVersionVector.Dots.Clear();
                        if (savedDvv.Dots != null)
                        {
                            foreach (var kvp in savedDvv.Dots)
                            {
                                replicaContext.GlobalVersionVector.Dots[kvp.Key] = new HashSet<long>(kvp.Value);
                            }
                        }
                    }
                    
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
                IDictionary<string, List<CrdtOperation>> operationsByDoc = new Dictionary<string, List<CrdtOperation>>();
                
                await foreach (var jOp in globalStorage.GetAllJournaledOperationsAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (!operationsByDoc.TryGetValue(jOp.DocumentId, out var opList))
                    {
                        opList = new List<CrdtOperation>();
                        operationsByDoc[jOp.DocumentId] = opList;
                    }
                    opList.Add(jOp.Operation);
                }

                if (operationsByDoc.Count > 0)
                {
                    var totalReplayed = operationsByDoc.Values.Sum(l => l.Count);
                    replayedOperationsCounter.Add(totalReplayed, new KeyValuePair<string, object?>("replica_id", replicaContext.ReplicaId));
                    logger.LogInformation("Replaying {Count} journaled operations for replica {ReplicaId}.", totalReplayed, replicaContext.ReplicaId);
                    
                    if (operationsByDoc.TryGetValue(orchestrator.Registry.DocumentId, out var registryOps))
                    {
                        await orchestrator.Registry.ApplyJournaledOperationsAsync(registryOps, cancellationToken).ConfigureAwait(false);
                        await orchestrator.SyncDocumentsAsync(cancellationToken).ConfigureAwait(false);
                    }

                    var documents = orchestrator.GetActiveDocuments();
                    
                    foreach (var document in documents)
                    {
                        if (document.DocumentId != orchestrator.Registry.DocumentId && operationsByDoc.TryGetValue(document.DocumentId, out var docOps))
                        {
                            await document.ApplyJournaledOperationsAsync(docOps, cancellationToken).ConfigureAwait(false);
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
}