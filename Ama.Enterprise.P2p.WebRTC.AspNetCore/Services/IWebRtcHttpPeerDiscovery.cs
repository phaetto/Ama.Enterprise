namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Contract for a service that orchestrates the out-of-band WebRTC signaling workflow against a specific remote HTTP endpoint natively.
/// </summary>
public interface IWebRtcHttpPeerDiscovery
{
    /// <summary>
    /// Executes the full out-of-band WebRTC signaling exchange explicitly connecting to the remote peer.
    /// </summary>
    /// <param name="peerUri">The base HTTP URI of the target peer's signaling server.</param>
    /// <param name="pathPrefix">The explicit path prefix configured on the peer (optional).</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>True if the WebRTC signaling negotiation was resolved and the connection is active, otherwise false.</returns>
    Task<bool> DiscoverPeerAsync(Uri peerUri, string? pathPrefix = null, CancellationToken cancellationToken = default);
}