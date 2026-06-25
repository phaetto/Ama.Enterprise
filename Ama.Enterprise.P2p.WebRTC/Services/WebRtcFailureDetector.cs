namespace Ama.Enterprise.P2p.WebRTC.Services;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.Models;
using Microsoft.Extensions.Logging;

/// <summary>
/// Determines peer health based on the state of the active WebRTC data channels natively, bypassing time-based heartbeats.
/// </summary>
public sealed class WebRtcFailureDetector : IFailureDetector
{
    private readonly string meshId;
    private readonly IPeerRegistry peerRegistry;
    private readonly IWebRtcConnectionManager connectionManager;
    private readonly ILogger<WebRtcFailureDetector> logger;

    public WebRtcFailureDetector(
        string meshId,
        IPeerRegistry peerRegistry,
        IWebRtcConnectionManager connectionManager,
        ILogger<WebRtcFailureDetector> logger)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(peerRegistry);
        ArgumentNullException.ThrowIfNull(connectionManager);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.peerRegistry = peerRegistry;
        this.connectionManager = connectionManager;
        this.logger = logger;
    }

    /// <inheritdoc />
    public Task RecordHeartbeatAsync(PeerId peerId, CancellationToken cancellationToken)
    {
        // WebRTC maintains connection health via persistent DataChannels natively.
        // Heartbeats are intentionally ignored to prevent unnecessary overhead.
        logger.LogTrace("[{MeshId}] Ignored heartbeat for WebRTC peer {PeerId}.", meshId, peerId.Value);
        
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<PeerStatus> EvaluatePeerHealthAsync(PeerId peerId, CancellationToken cancellationToken)
    {
        if (peerId.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID cannot be empty.", nameof(peerId));
        }

        var peers = await peerRegistry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
        var peerNode = Enumerable.FirstOrDefault(peers, p => p.Id == peerId);
        
        if (peerNode.Endpoint is not WebRtcPeerEndpoint webRtcEndpoint)
        {
            logger.LogWarning("[{MeshId}] Peer {PeerId} not found or endpoint is not WebRTC.", meshId, peerId.Value);
            return PeerStatus.Dead;
        }

        bool isActive = connectionManager.IsConnectionActive(webRtcEndpoint.ConnectionId);

        if (!isActive)
        {
            logger.LogDebug("[{MeshId}] WebRTC connection {ConnectionId} for peer {PeerId} is inactive.", meshId, webRtcEndpoint.ConnectionId, peerId.Value);
            return PeerStatus.Dead;
        }

        return PeerStatus.Active;
    }
}