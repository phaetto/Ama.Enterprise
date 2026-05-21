namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Observes network connections natively mapping inherently multi-mesh capabilities isolated explicitly gracefully.
/// </summary>
public sealed class CrdtTopologyObserver : IPeerTopologyObserver, IDisposable
{
    private readonly string replicaId;
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly ILogger<CrdtTopologyObserver> logger;
    private int hasConnected;

    private readonly Meter meter;
    private readonly Counter<long> peersJoinedCounter;
    private readonly Counter<long> peersDepartedCounter;

    public CrdtTopologyObserver(
        string replicaId,
        DistributedCrdtScopeManager scopeManager,
        ILogger<CrdtTopologyObserver> logger,
        IMeterFactory? meterFactory = null)
    {
        this.replicaId = replicaId ?? throw new ArgumentNullException(nameof(replicaId));
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtTopologyObserver") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtTopologyObserver");
        this.peersJoinedCounter = this.meter.CreateCounter<long>("crdt.topology.peers_joined", "peers", "Total dynamically resolved peer connections evaluating generic state maps");
        this.peersDepartedCounter = this.meter.CreateCounter<long>("crdt.topology.peers_departed", "peers", "Total explicitly dropped peering profiles preserving log matrices gracefully");
    }

    /// <inheritdoc />
    public async Task OnPeerJoinedAsync(string meshId, PeerNode node, CancellationToken cancellationToken)
    {
        peersJoinedCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", meshId));

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
                logger.LogError(ex, "[{MeshId}] Failed to broadcast initial global state sync mapped accurately natively actively upon connecting to first peer.", meshId);
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
            peersDepartedCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", meshId));
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
                logger.LogInformation("[{MeshId}] Peer {PeerId} marked distinctly dynamically natively properly cleanly logically dynamically natively cleanly as Dead seamlessly targeting localized replica {ReplicaId}. Extracting persistent generic boundaries safely natively gracefully securely.", meshId, stringId, replicaId);
                
                var scope = scopeManager.GetOrCreateScope(replicaId);
                scope.ClusterTracker.RemovePeerByNetworkId(stringId);
                peersDepartedCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", meshId));
            }
        }
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        meter.Dispose();
    }
}