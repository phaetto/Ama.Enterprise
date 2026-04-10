namespace Ama.Enterprise.P2p.WebRTC.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implements outbound gossip transport dynamically mapping messages across isolated WebRTC Data Channels.
/// </summary>
public sealed class WebRtcTransport : ITransport<GossipMessage>
{
    private readonly string meshId;
    private readonly IWebRtcConnectionManager connectionManager;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<WebRtcTransport> logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="WebRtcTransport"/> class.
    /// </summary>
    public WebRtcTransport(
        string meshId,
        IWebRtcConnectionManager connectionManager,
        ICrdtSerializer serializer,
        ILogger<WebRtcTransport> logger)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint) => endpoint is WebRtcPeerEndpoint;

    /// <inheritdoc />
    public Task SendAsync(PeerEndpoint endpoint, GossipMessage message, CancellationToken cancellationToken)
    {
        if (endpoint is not WebRtcPeerEndpoint webrtcEndpoint)
        {
            logger.LogWarning("[{MeshId}] Cannot send WebRTC message. Target endpoint is not WebRtcPeerEndpoint.", meshId);
            return Task.CompletedTask;
        }

        var payload = serializer.SerializeToBytes(message);
        return connectionManager.SendMessageAsync(webrtcEndpoint.ConnectionId, payload, cancellationToken);
    }
}