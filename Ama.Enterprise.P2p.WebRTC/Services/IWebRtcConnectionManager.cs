namespace Ama.Enterprise.P2p.WebRTC.Services;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Internally manages active WebRTC RTCPeerConnections and proxies their Data Channels to the Transport layer.
/// </summary>
public interface IWebRtcConnectionManager
{
    /// <summary>
    /// Event triggered when a valid gossip payload is received on any data channel.
    /// </summary>
    event Func<Guid, byte[], Task>? OnMessageReceived;

    /// <summary>
    /// Sends a payload over the specified WebRTC data channel implicitly handling handshake prefixes.
    /// </summary>
    /// <param name="connectionId">The local connection identifier tracking the remote peer.</param>
    /// <param name="payload">The message payload to send.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous send operation.</returns>
    Task SendMessageAsync(Guid connectionId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);
}