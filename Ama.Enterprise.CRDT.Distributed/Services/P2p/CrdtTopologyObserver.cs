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
    public Task OnPeerDepartedAsync(PeerId peerId, CancellationToken cancellationToken)
    {
        if (peerId.Value != Guid.Empty)
        {
            var stringId = peerId.Value.ToString();
            logger.LogInformation("Removing detached peer {PeerId} from active underlying GMVV tracker matrices.", stringId);
            clusterTracker.RemovePeerState(stringId);
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
                logger.LogInformation("Cleaning up dead offline peer {PeerId} mapping natively adjusting cluster tracking states.", stringId);
                clusterTracker.RemovePeerState(stringId);
            }
        }
        return Task.CompletedTask;
    }
}