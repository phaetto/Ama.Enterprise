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
    /// Event triggered when the underlying WebRTC connection state changes (e.g., connecting, connected, failed).
    /// Provides the connection ID and the string representation of the RTC peer connection state.
    /// </summary>
    event Action<Guid, string>? OnConnectionStateChanged;

    /// <summary>
    /// Sends a payload over the specified WebRTC data channel implicitly handling handshake prefixes.
    /// </summary>
    /// <param name="connectionId">The local connection identifier tracking the remote peer.</param>
    /// <param name="payload">The message payload to send.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous send operation.</returns>
    Task SendMessageAsync(Guid connectionId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);

    /// <summary>
    /// Checks if the underlying WebRTC connection and its data channel are currently open and active.
    /// </summary>
    /// <param name="connectionId">The local connection identifier tracking the remote peer.</param>
    /// <returns>True if the connection and data channel are active; otherwise, false.</returns>
    bool IsConnectionActive(Guid connectionId);
}