namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation of <see cref="IDirectMessageSender"/> providing targeted point-to-point delivery.
/// </summary>
public sealed class DirectMessageSender(
    IServiceProvider serviceProvider,
    IEnumerable<P2pMeshMetadata> meshes,
    IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
    ILogger<DirectMessageSender> logger) : IDirectMessageSender
{
    private readonly IServiceProvider serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    private readonly IEnumerable<P2pMeshMetadata> meshes = meshes ?? throw new ArgumentNullException(nameof(meshes));
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
    private readonly ILogger<DirectMessageSender> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task SendDirectAsync(PeerId targetPeerId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var registry = serviceProvider.GetRequiredService<IPeerRegistry>();
        
        foreach (var mesh in meshes)
        {
            var meshId = mesh.MeshId;
            var peers = await registry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
            var targetPeer = peers.FirstOrDefault(p => p.Id.Equals(targetPeerId));

            if (targetPeer != null)
            {
                await SendToPeerInternalAsync(meshId, targetPeer, payload, cancellationToken).ConfigureAwait(false);
                return;
            }
        }
        
        logger.LogWarning("Cannot send targeted direct message. Peer {PeerId} not found in any active mesh topology.", targetPeerId.Value);
    }

    /// <inheritdoc />
    public async Task SendToRandomPeerAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        foreach (var mesh in meshes)
        {
            var meshId = mesh.MeshId;
            var selector = serviceProvider.GetKeyedService<IPeerSelector>(meshId);
            
            if (selector != null)
            {
                var peers = (await selector.GetPeersAsync(1, cancellationToken).ConfigureAwait(false)).ToList();
                if (peers.Count > 0)
                {
                    await SendToPeerInternalAsync(meshId, peers[0], payload, cancellationToken).ConfigureAwait(false);
                    return;
                }
            }
        }
        
        logger.LogDebug("No peers available across any active mesh for random direct message delivery.");
    }

    private async Task SendToPeerInternalAsync(string meshId, PeerNode peer, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        var router = serviceProvider.GetRequiredKeyedService<ITransportRouter>(meshId);

        // Wrap as a single-hop packet. A TTL of 1 mathematically ensures it dies exactly after delivery avoiding propagation.
        var message = new GossipMessage(
            meshId,
            Constants.ProtocolVersion,
            Guid.NewGuid(),
            new PeerId(nodeOptions.LocalPeerId),
            1, 
            GossipMessageType.Broadcast,
            null,
            payload);

        logger.LogTrace("[{MeshId}] Sending direct point-to-point message {MessageId} to {TargetPeerId}", meshId, message.MessageId, peer.Id.Value);
        
        await router.SendAsync(peer.Endpoint, message, cancellationToken).ConfigureAwait(false);
    }
}