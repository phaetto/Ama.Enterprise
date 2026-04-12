namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Versioning;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

/// <summary>
/// Observes network connections and instantly forces an anti-entropy synchronization for all registered CRDTs on the very first peer discovery.
/// Safely cleans up underlying cluster tracking states seamlessly when a peer drops inherently natively.
/// </summary>
public sealed class CrdtTopologyObserver : IPeerTopologyObserver
{
    private readonly DistributedCrdtScopeProvider scopeProvider;
    private readonly IClusterStateTracker clusterTracker;
    private readonly ILogger<CrdtTopologyObserver> logger;
    private int hasConnected;

    public CrdtTopologyObserver(
        DistributedCrdtScopeProvider scopeProvider,
        IClusterStateTracker clusterTracker,
        ILogger<CrdtTopologyObserver> logger)
    {
        this.scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
        this.clusterTracker = clusterTracker ?? throw new ArgumentNullException(nameof(clusterTracker));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task OnPeerJoinedAsync(PeerNode node, CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref hasConnected, 1, 0) == 0)
        {
            logger.LogInformation("Connected to first peer {PeerId}. Triggering immediate DVV state sync for all CRDTs.", node.Id);

            try
            {
                var documents = scopeProvider.Scope.ServiceProvider.GetRequiredService<IEnumerable<IDistributedCrdtDocument>>();

                foreach (var document in documents)
                {
                    await document.BroadcastStateAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to broadcast initial state sync upon connecting to first peer.");
            }
        }
    }

    /// <inheritdoc />
    public async Task OnPeerDepartedAsync(PeerId peerId, CancellationToken cancellationToken)
    {
        if (peerId.Value != Guid.Empty)
        {
            var stringId = peerId.Value.ToString();
            logger.LogInformation("Peer {PeerId} gracefully departed. Instantly tombstoning it to free GMVV tracking limits securely.", stringId);
            
            var replicaId = clusterTracker.TombstonePeerByNetworkId(stringId);

            if (!string.IsNullOrEmpty(replicaId))
            {
                try
                {
                    var documents = scopeProvider.Scope.ServiceProvider.GetRequiredService<IEnumerable<IDistributedCrdtDocument>>();
                    foreach (var document in documents)
                    {
                        await document.EvictReplicaAsync(replicaId, cancellationToken).ConfigureAwait(false);
                    }

                    var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();
                    var syncService = scopeProvider.Scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();

                    lock (replicaContext.GlobalVersionVector)
                    {
                        var cleanedDvv = syncService.RemoveEvictedReplicas(replicaContext.GlobalVersionVector, new[] { replicaId });
                        
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
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to properly clean underlying CRDT limits and DVV boundaries during an instant graceful peer departure.");
                }
            }
        }
    }

    /// <inheritdoc />
    public Task OnPeerStatusChangedAsync(PeerId peerId, PeerStatus newStatus, CancellationToken cancellationToken)
    {
        if (newStatus == PeerStatus.Dead)
        {
            if (peerId.Value != Guid.Empty)
            {
                var stringId = peerId.Value.ToString();
                logger.LogInformation("Peer {PeerId} marked as Dead (network drop). Unmapping network ID but explicitly preserving CRDT state to allow safe offline reconnect without forcing identity re-bootstraps.", stringId);
                
                // Intentionally DO NOT tombstone here to prevent data loss. The background TTL service will clean it if it doesn't return.
                clusterTracker.RemovePeerByNetworkId(stringId);
            }
        }
        return Task.CompletedTask;
    }
}