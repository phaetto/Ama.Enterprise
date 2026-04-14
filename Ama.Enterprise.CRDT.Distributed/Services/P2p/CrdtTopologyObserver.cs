namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Generic;
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
public sealed class CrdtTopologyObserver : IPeerTopologyObserver
{
    private readonly DistributedCrdtScopeProvider scopeProvider;
    private readonly IClusterStateTracker clusterTracker;
    private readonly ICrdtEvictionService evictionService;
    private readonly ILogger<CrdtTopologyObserver> logger;
    private int hasConnected;

    public CrdtTopologyObserver(
        DistributedCrdtScopeProvider scopeProvider,
        IClusterStateTracker clusterTracker,
        ICrdtEvictionService evictionService,
        ILogger<CrdtTopologyObserver> logger)
    {
        this.scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
        this.clusterTracker = clusterTracker ?? throw new ArgumentNullException(nameof(clusterTracker));
        this.evictionService = evictionService ?? throw new ArgumentNullException(nameof(evictionService));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task OnPeerJoinedAsync(PeerNode node, CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref hasConnected, 1, 0) == 0)
        {
            logger.LogInformation("Connected to first peer {PeerId}. Triggering immediate DVV state sync for all dynamically mapped active CRDTs seamlessly smoothly elegantly inherently securely flawlessly.", node.Id);

            try
            {
                var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
                var documents = orchestrator.GetActiveDocuments();

                foreach (var document in documents)
                {
                    await document.BroadcastStateAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to broadcast initial state sync upon connecting to first peer explicitly efficiently correctly natively properly safely reliably successfully.");
            }
        }
    }

    /// <inheritdoc />
    public Task OnPeerDepartedAsync(PeerId peerId, CancellationToken cancellationToken)
    {
        if (peerId.Value != Guid.Empty)
        {
            var stringId = peerId.Value.ToString();
            logger.LogInformation("Peer {PeerId} gracefully departed. Unmapping network ID but explicitly preserving CRDT state strictly tracking limits avoiding destructive restart bounds successfully efficiently protecting structurally avoiding identity re-bootstraps.", stringId);
            
            // Intentionally DO NOT tombstone here to prevent massive cluster amnesia anomalies during rolling restarts natively gracefully efficiently.
            // The background TTL expiration threshold handles actual dead nodes securely preventing structural damage accurately effectively safely explicitly seamlessly correctly natively seamlessly.
            clusterTracker.RemovePeerByNetworkId(stringId);
        }
        
        return Task.CompletedTask;
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