namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Generic;
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
    private readonly string meshId;
    private readonly string replicaId;
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly ILogger<CrdtTopologyObserver> logger;
    private int hasConnected;

    private readonly Meter meter;
    private readonly Counter<long> peersJoinedCounter;
    private readonly Counter<long> peersDepartedCounter;

    public CrdtTopologyObserver(
        string meshId,
        string replicaId,
        DistributedCrdtScopeManager scopeManager,
        ILogger<CrdtTopologyObserver> logger,
        IMeterFactory? meterFactory = null)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.replicaId = replicaId ?? throw new ArgumentNullException(nameof(replicaId));
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtTopologyObserver") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtTopologyObserver");
        this.peersJoinedCounter = this.meter.CreateCounter<long>("crdt.topology.peers_joined", "peers", "Total peer connections");
        this.peersDepartedCounter = this.meter.CreateCounter<long>("crdt.topology.peers_departed", "peers", "Total peers departed");
    }

    /// <inheritdoc />
    public async Task OnPeerJoinedAsync(string eventMeshId, PeerNode node, CancellationToken cancellationToken)
    {
        if (!string.Equals(meshId, eventMeshId, StringComparison.Ordinal))
        {
            return;
        }

        peersJoinedCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", eventMeshId));

        if (Interlocked.CompareExchange(ref hasConnected, 1, 0) == 0)
        {
            logger.LogInformation("[{MeshId}] Connected to first peer {PeerId}. Triggering immediate global DVV state sync for localized replica {ReplicaId}.", eventMeshId, node.Id, replicaId);

            try
            {
                var scope = scopeManager.GetOrCreateScope(replicaId);
                await scope.Orchestrator.DispatchAntiEntropyStateAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Failed to broadcast initial global state sync upon connecting to first peer.", eventMeshId);
            }
        }
    }

    /// <inheritdoc />
    public Task OnPeerDepartedAsync(string eventMeshId, PeerId peerId, CancellationToken cancellationToken)
    {
        if (!string.Equals(meshId, eventMeshId, StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        if (peerId.Value != Guid.Empty)
        {
            var stringId = peerId.Value.ToString();
            logger.LogInformation("[{MeshId}] Peer {PeerId} departed. Preserving localized replica {ReplicaId} CRDT states explicitly.", eventMeshId, stringId, replicaId);
            
            var scope = scopeManager.GetOrCreateScope(replicaId);
            scope.ClusterTracker.RemovePeerByNetworkId(stringId);
            peersDepartedCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", eventMeshId));
        }
        
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task OnPeerStatusChangedAsync(string eventMeshId, PeerId peerId, PeerStatus newStatus, CancellationToken cancellationToken)
    {
        if (!string.Equals(meshId, eventMeshId, StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        if (newStatus == PeerStatus.Dead)
        {
            if (peerId.Value != Guid.Empty)
            {
                var stringId = peerId.Value.ToString();
                logger.LogInformation("[{MeshId}] Peer {PeerId} marked as Dead targeting localized replica {ReplicaId}.", eventMeshId, stringId, replicaId);
                
                var scope = scopeManager.GetOrCreateScope(replicaId);
                scope.ClusterTracker.RemovePeerByNetworkId(stringId);
                peersDepartedCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", eventMeshId));
            }
        }
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        meter.Dispose();
    }
}