namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Observes network topologies explicitly triggering orchestrator re-evaluations dynamically promoting or demoting documents.
/// </summary>
public sealed class OrchestratorTopologyObserver : IPeerTopologyObserver
{
    private readonly string meshId;
    private readonly string replicaId;
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly ILogger<OrchestratorTopologyObserver> logger;

    public OrchestratorTopologyObserver(
        string meshId,
        string replicaId,
        DistributedCrdtScopeManager scopeManager,
        ILogger<OrchestratorTopologyObserver> logger)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.replicaId = replicaId ?? throw new ArgumentNullException(nameof(replicaId));
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task OnPeerJoinedAsync(string eventMeshId, PeerNode node, CancellationToken cancellationToken)
    {
        if (!string.Equals(meshId, eventMeshId, StringComparison.Ordinal))
        {
            return;
        }

        logger.LogInformation("[{MeshId}] Peer {PeerId} joined. Triggering orchestrator topology re-evaluation for replica {ReplicaId}.", eventMeshId, node.Id, replicaId);
        await ReevaluateAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task OnPeerDepartedAsync(string eventMeshId, PeerId peerId, CancellationToken cancellationToken)
    {
        if (!string.Equals(meshId, eventMeshId, StringComparison.Ordinal))
        {
            return;
        }

        logger.LogInformation("[{MeshId}] Peer {PeerId} departed. Triggering orchestrator topology re-evaluation for replica {ReplicaId}.", eventMeshId, peerId.Value, replicaId);
        await ReevaluateAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task OnPeerStatusChangedAsync(string eventMeshId, PeerId peerId, PeerStatus newStatus, CancellationToken cancellationToken)
    {
        if (!string.Equals(meshId, eventMeshId, StringComparison.Ordinal))
        {
            return;
        }

        if (newStatus == PeerStatus.Dead)
        {
            logger.LogInformation("[{MeshId}] Peer {PeerId} marked dead. Triggering orchestrator topology re-evaluation for replica {ReplicaId}.", eventMeshId, peerId.Value, replicaId);
            await ReevaluateAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ReevaluateAsync(CancellationToken cancellationToken)
    {
        try
        {
            var scope = scopeManager.GetOrCreateScope(replicaId);
            await scope.Orchestrator.ReevaluateTopologyAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to re-evaluate orchestrator topology.");
        }
    }
}