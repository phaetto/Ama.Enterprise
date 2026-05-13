namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Versioning;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implementation explicitly enforcing bounded mapping limits structurally natively.
/// </summary>
public sealed class CrdtEvictionService : ICrdtEvictionService, IDisposable
{
    private readonly IServiceProvider serviceProvider;
    private readonly ILogger<CrdtEvictionService> logger;

    private readonly Meter meter;
    private readonly Counter<long> replicasEvictedCounter;
    private readonly Counter<long> identitiesRebootedCounter;

    public CrdtEvictionService(
        IServiceProvider serviceProvider,
        ILogger<CrdtEvictionService> logger,
        IMeterFactory? meterFactory = null)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtEvictionService") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtEvictionService");
        this.replicasEvictedCounter = this.meter.CreateCounter<long>("crdt.eviction.replicas_evicted", "replicas", "Total replica evictions processed natively across active structures");
        this.identitiesRebootedCounter = this.meter.CreateCounter<long>("crdt.eviction.identities_rebooted", "reboots", "Total local identities forcibly re-bootstrapped following cluster tombstones");
    }

    /// <inheritdoc />
    public async Task EvictPeersAsync(IReadOnlyList<string> replicaIds, CancellationToken cancellationToken = default)
    {
        if (replicaIds == null || replicaIds.Count == 0) return;

        var orchestrator = serviceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var documents = orchestrator.GetActiveDocuments();
        var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
        var syncService = serviceProvider.GetRequiredService<IVersionVectorSyncService>();

        foreach (var document in documents)
        {
            foreach (var replicaId in replicaIds)
            {
                await document.EvictReplicaAsync(replicaId, cancellationToken).ConfigureAwait(false);
            }
        }

        lock (replicaContext.GlobalVersionVector)
        {
            var cleanedDvv = syncService.RemoveEvictedReplicas(replicaContext.GlobalVersionVector, replicaIds);
            
            replicaContext.GlobalVersionVector.Versions.Clear();
            foreach (var kvp in cleanedDvv.Versions)
            {
                replicaContext.GlobalVersionVector.Versions[kvp.Key] = kvp.Value;
            }
            
            replicaContext.GlobalVersionVector.Dots.Clear();
            if (cleanedDvv.Dots != null)
            {
                foreach (var kvp in cleanedDvv.Dots)
                {
                    replicaContext.GlobalVersionVector.Dots[kvp.Key] = kvp.Value;
                }
            }
        }
        
        replicasEvictedCounter.Add(replicaIds.Count, new KeyValuePair<string, object?>("replica_id", replicaContext.ReplicaId));
        logger.LogInformation("[{LocalReplicaId}] Successfully applied eviction across {DocCount} documents for {ReplicaCount} replicas.", replicaContext.ReplicaId, documents.Count, replicaIds.Count);
    }

    /// <inheritdoc />
    public async Task RebootLocalIdentityAsync(CancellationToken cancellationToken = default)
    {
        var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
        var orchestrator = serviceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var documents = orchestrator.GetActiveDocuments();

        logger.LogCritical("CRITICAL: This replica ({ReplicaId}) has been permanently tombstoned by the cluster. Re-bootstrapping identity completely to prevent split-brain amnesia anomalies natively avoiding generic deadlocks structurally.", replicaContext.ReplicaId);
        
        var currentId = replicaContext.ReplicaId ?? string.Empty;
        var lastUnderscore = currentId.LastIndexOf('_');
        
        var prefix = (lastUnderscore >= 0 && currentId.Length - lastUnderscore - 1 == 32)
            ? currentId[..lastUnderscore]
            : currentId;

        replicaContext.ReplicaId = string.IsNullOrEmpty(prefix) 
            ? Guid.NewGuid().ToString("N") 
            : $"{prefix}_{Guid.NewGuid():N}";
            
        logger.LogCritical("CRITICAL: This replica is now internally routing active payloads mapped matching natively explicitly seamlessly utilizing logically identified inherently limits as: {ReplicaId}", replicaContext.ReplicaId);
        
        lock (replicaContext.GlobalVersionVector)
        {
            replicaContext.GlobalVersionVector.Versions.Clear();
            
            if (replicaContext.GlobalVersionVector.Dots != null)
            {
                replicaContext.GlobalVersionVector.Dots.Clear();
            }
        }

        foreach (var doc in documents)
        {
            await doc.ResetLocalStateAsync(currentId, cancellationToken).ConfigureAwait(false);
        }
        
        await orchestrator.DispatchAntiEntropyStateAsync(cancellationToken).ConfigureAwait(false);
        
        identitiesRebootedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaContext.ReplicaId));
        logger.LogInformation("Successfully completed re-bootstrap identity mechanisms explicitly extracting persistent capabilities mapping explicitly.");
    }

    public void Dispose()
    {
        meter.Dispose();
    }
}