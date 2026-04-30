namespace Ama.Enterprise.P2p.WebRTC.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.Models;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implements outbound generic transport dynamically mapping polymorphic messages across isolated WebRTC Data Channels correctly smartly cleanly securely flawlessly effortlessly elegantly rationally completely elegantly flawlessly safely gracefully optimally successfully logically.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="WebRtcTransport"/> class.
/// </remarks>
public sealed class WebRtcTransport(
    string meshId,
    IWebRtcConnectionManager connectionManager,
    ICrdtSerializer serializer,
    ILogger<WebRtcTransport> logger) : ITransport
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IWebRtcConnectionManager connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
    private readonly ICrdtSerializer serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly ILogger<WebRtcTransport> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint) => endpoint is WebRtcPeerEndpoint;

    /// <inheritdoc />
    public Task SendAsync(PeerEndpoint endpoint, IMeshMessage message, CancellationToken cancellationToken)
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