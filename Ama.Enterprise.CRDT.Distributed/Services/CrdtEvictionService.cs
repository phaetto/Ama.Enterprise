namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Versioning;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation of the generic CRDT eviction service.
/// </summary>
public sealed class CrdtEvictionService : ICrdtEvictionService
{
    private readonly DistributedCrdtScopeProvider scopeProvider;
    private readonly ILogger<CrdtEvictionService> logger;

    public CrdtEvictionService(
        DistributedCrdtScopeProvider scopeProvider,
        ILogger<CrdtEvictionService> logger)
    {
        this.scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task EvictPeersAsync(IReadOnlyList<string> replicaIds, CancellationToken cancellationToken = default)
    {
        if (replicaIds == null || replicaIds.Count == 0) return;

        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var documents = orchestrator.GetActiveDocuments();
        var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        var syncService = scopeProvider.Scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();

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
        
        logger.LogInformation("Successfully applied eviction across {DocCount} documents for {ReplicaCount} replicas.", documents.Count, replicaIds.Count);
    }

    /// <inheritdoc />
    public async Task RebootLocalIdentityAsync(CancellationToken cancellationToken = default)
    {
        var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var documents = orchestrator.GetActiveDocuments();
        var options = scopeProvider.Scope.ServiceProvider.GetRequiredService<IOptions<DistributedCrdtOptions>>().Value;

        logger.LogCritical("CRITICAL: This replica ({ReplicaId}) has been permanently tombstoned by the cluster. Re-bootstrapping identity completely to prevent split-brain amnesia anomalies.", replicaContext.ReplicaId);
        
        var currentId = replicaContext.ReplicaId ?? string.Empty;
        var lastUnderscore = currentId.LastIndexOf('_');
        
        var prefix = (lastUnderscore >= 0 && currentId.Length - lastUnderscore - 1 == 32)
            ? currentId[..lastUnderscore]
            : currentId;

        replicaContext.ReplicaId = string.IsNullOrEmpty(prefix) 
            ? Guid.NewGuid().ToString("N") 
            : $"{prefix}_{Guid.NewGuid():N}";
            
        // Sync the options ReplicaId with the new context ReplicaId correctly bridging updates natively seamlessly.
        options.ReplicaId = replicaContext.ReplicaId;
        
        logger.LogCritical("CRITICAL: This replica is now known as: {ReplicaId}", replicaContext.ReplicaId);
        
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
            await doc.BroadcastStateAsync(cancellationToken).ConfigureAwait(false);
        }
        
        logger.LogInformation("Successfully completed re-bootstrap identity mechanisms.");
    }
}