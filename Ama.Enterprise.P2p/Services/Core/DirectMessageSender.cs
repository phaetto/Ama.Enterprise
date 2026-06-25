namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Algorithms;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation of <see cref="IDirectMessageSender"/> providing targeted point-to-point delivery.
/// </summary>
public sealed class DirectMessageSender : IDirectMessageSender, IDisposable
{
    private readonly IServiceProvider serviceProvider;
    private readonly IEnumerable<P2pMeshMetadata> meshes;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly ILogger<DirectMessageSender> logger;

    private readonly Meter meter;
    private readonly Counter<long> messagesSentCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    public DirectMessageSender(
        IServiceProvider serviceProvider,
        IEnumerable<P2pMeshMetadata> meshes,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        ILogger<DirectMessageSender> logger)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.meshes = meshes ?? throw new ArgumentNullException(nameof(meshes));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var meterFactory = serviceProvider.GetService<IMeterFactory>();
        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.DirectMessageSender") ?? new Meter("Ama.Enterprise.P2p.DirectMessageSender");
        this.messagesSentCounter = this.meter.CreateCounter<long>("p2p.direct_sender.messages_sent", "messages", "Total targeted direct messages sent");
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>("p2p.direct_sender.payload_bytes", "bytes", "Size of outbound direct payload in bytes");
    }

    /// <inheritdoc />
    public async Task SendDirectAsync(PeerId targetPeerId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var registry = serviceProvider.GetRequiredService<IPeerRegistry>();
        
        foreach (var mesh in meshes)
        {
            var meshId = mesh.MeshId;
            var peers = await registry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
            var targetPeer = peers.FirstOrDefault(p => p.Id.Equals(targetPeerId));

            if (targetPeer.Id.Value != Guid.Empty)
            {
                await SendToPeerInternalAsync(meshId, targetPeer, payload, cancellationToken).ConfigureAwait(false);
                return;
            }
        }
        
        logger.LogWarning("Cannot send targeted direct message. Peer {PeerId} not found in any active mesh topology.", targetPeerId.Value);
    }

    /// <inheritdoc />
    public async Task SendDirectAsync(string meshId, PeerId targetPeerId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(meshId))
        {
            throw new ArgumentException("Mesh ID cannot be null or whitespace.", nameof(meshId));
        }

        var registry = serviceProvider.GetRequiredService<IPeerRegistry>();
        var peers = await registry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
        var targetPeer = peers.FirstOrDefault(p => p.Id.Equals(targetPeerId));

        if (targetPeer.Id.Value != Guid.Empty)
        {
            await SendToPeerInternalAsync(meshId, targetPeer, payload, cancellationToken).ConfigureAwait(false);
            return;
        }
        
        logger.LogWarning("Cannot send targeted direct message. Peer {PeerId} not found in the explicit mesh topology {MeshId}.", targetPeerId.Value, meshId);
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

        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
        messagesSentCounter.Add(1, tags);
        payloadBytesHistogram.Record(payload.Length, tags);

        logger.LogTrace("[{MeshId}] Sending direct point-to-point message {MessageId} to {TargetPeerId}", meshId, message.MessageId, peer.Id.Value);
        
        await router.SendAsync(peer.Endpoint, message, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        meter.Dispose();
    }
}