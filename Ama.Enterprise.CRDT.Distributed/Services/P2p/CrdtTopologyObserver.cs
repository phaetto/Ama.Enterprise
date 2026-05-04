namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

/// <summary>
/// Observes network connections and instantly forces an anti-entropy synchronization for all registered CRDTs on the very first peer discovery.
/// Safely cleans up underlying cluster tracking states seamlessly when a peer drops inherently natively.
/// </summary>
public sealed class CrdtTopologyObserver(
    DistributedCrdtScopeProvider scopeProvider,
    IClusterStateTracker clusterTracker,
    ICrdtEvictionService evictionService,
    ILogger<CrdtTopologyObserver> logger) : IPeerTopologyObserver
{
    private readonly DistributedCrdtScopeProvider scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
    private readonly IClusterStateTracker clusterTracker = clusterTracker ?? throw new ArgumentNullException(nameof(clusterTracker));
    private readonly ICrdtEvictionService evictionService = evictionService ?? throw new ArgumentNullException(nameof(evictionService));
    private readonly ILogger<CrdtTopologyObserver> logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private int hasConnected;

    /// <inheritdoc />
    public async Task OnPeerJoinedAsync(string meshId, PeerNode node, CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref hasConnected, 1, 0) == 0)
        {
            logger.LogInformation("[{MeshId}] Connected to first peer {PeerId}. Triggering immediate global DVV state sync.", meshId, node.Id);

            try
            {
                var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
                await orchestrator.DispatchAntiEntropyStateAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Failed to broadcast initial global state sync upon connecting to first peer.", meshId);
            }
        }
    }

    /// <inheritdoc />
    public Task OnPeerDepartedAsync(string meshId, PeerId peerId, CancellationToken cancellationToken)
    {
        if (peerId.Value != Guid.Empty)
        {
            var stringId = peerId.Value.ToString();
            logger.LogInformation("[{MeshId}] Peer {PeerId} departed. Unmapping network ID but preserving CRDT state.", meshId, stringId);
            
            // Intentionally DO NOT tombstone here to prevent massive cluster amnesia anomalies during rolling restarts.
            // The background TTL expiration threshold handles actual dead nodes securely.
            clusterTracker.RemovePeerByNetworkId(stringId);
        }
        
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task OnPeerStatusChangedAsync(string meshId, PeerId peerId, PeerStatus newStatus, CancellationToken cancellationToken)
    {
        if (newStatus == PeerStatus.Dead)
        {
            if (peerId.Value != Guid.Empty)
            {
                var stringId = peerId.Value.ToString();
                logger.LogInformation("[{MeshId}] Peer {PeerId} marked as Dead (network drop). Unmapping network ID but explicitly preserving CRDT state to allow safe offline reconnect without forcing identity re-bootstraps.", meshId, stringId);
                
                // Intentionally DO NOT tombstone here to prevent data loss. The background TTL service will clean it if it doesn't return.
                clusterTracker.RemovePeerByNetworkId(stringId);
            }
        }
        return Task.CompletedTask;
    }
}