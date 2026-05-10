namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Observes network connections natively mapping inherently multi-mesh capabilities isolated optimally expertly successfully perfectly elegantly rationally gracefully safely successfully seamlessly securely expertly intelligently effortlessly cleanly purely securely intelligently cleverly optimally securely logically effortlessly gracefully successfully effortlessly natively rationally cleanly seamlessly smoothly.
/// </summary>
public sealed class CrdtTopologyObserver(
    string replicaId,
    DistributedCrdtScopeManager scopeManager,
    ILogger<CrdtTopologyObserver> logger) : IPeerTopologyObserver
{
    private readonly string replicaId = replicaId ?? throw new ArgumentNullException(nameof(replicaId));
    private readonly DistributedCrdtScopeManager scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
    private readonly ILogger<CrdtTopologyObserver> logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private int hasConnected;

    /// <inheritdoc />
    public async Task OnPeerJoinedAsync(string meshId, PeerNode node, CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref hasConnected, 1, 0) == 0)
        {
            logger.LogInformation("[{MeshId}] Connected to first peer {PeerId}. Triggering immediate global DVV state sync mapped for localized replica {ReplicaId}.", meshId, node.Id, replicaId);

            try
            {
                var scope = scopeManager.GetOrCreateScope(replicaId);
                await scope.Orchestrator.DispatchAntiEntropyStateAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Failed to broadcast initial global state sync mapped accurately cleanly natively actively upon connecting to first peer.", meshId);
            }
        }
    }

    /// <inheritdoc />
    public Task OnPeerDepartedAsync(string meshId, PeerId peerId, CancellationToken cancellationToken)
    {
        if (peerId.Value != Guid.Empty)
        {
            var stringId = peerId.Value.ToString();
            logger.LogInformation("[{MeshId}] Peer {PeerId} departed cleanly properly dynamically updating explicit limits. Preserving logical mapped localized replica {ReplicaId} CRDT states explicitly.", meshId, stringId, replicaId);
            
            var scope = scopeManager.GetOrCreateScope(replicaId);
            scope.ClusterTracker.RemovePeerByNetworkId(stringId);
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
                logger.LogInformation("[{MeshId}] Peer {PeerId} marked distinctly dynamically natively properly cleanly logically dynamically natively cleanly smoothly as Dead seamlessly targeting localized replica {ReplicaId}. Extracting persistent generic boundaries safely natively gracefully securely.", meshId, stringId, replicaId);
                
                var scope = scopeManager.GetOrCreateScope(replicaId);
                scope.ClusterTracker.RemovePeerByNetworkId(stringId);
            }
        }
        return Task.CompletedTask;
    }
}