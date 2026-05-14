namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.WebRTC.Models;

/// <summary>
/// Defines a client for invoking explicit HTTP WebRTC signaling API endpoints managed by the decentralized topology.
/// </summary>
public interface IWebRtcSignalingClient
{
    /// <summary>
    /// Requests a new WebRTC invitation offer from a remote peer explicitly.
    /// </summary>
    /// <param name="peerUri">The base URI of the target peer's signaling server.</param>
    /// <param name="meshId">The identifier of the active P2P mesh architecture.</param>
    /// <param name="pathPrefix">The explicit path prefix configured on the peer (optional).</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>A parsed WebRTC invitation offer if successful, otherwise null.</returns>
    Task<WebRtcInvitationOffer?> RequestOfferAsync(Uri peerUri, string meshId, string? pathPrefix = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a local WebRTC invitation offer to a remote peer to obtain their explicit answer.
    /// </summary>
    /// <param name="peerUri">The base URI of the target peer's signaling server.</param>
    /// <param name="meshId">The identifier of the active P2P mesh architecture.</param>
    /// <param name="pathPrefix">The explicit path prefix configured on the peer (optional).</param>
    /// <param name="offer">The generated local WebRTC offer.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>A parsed WebRTC invitation answer if successful, otherwise null.</returns>
    Task<WebRtcInvitationAnswer?> SendOfferAsync(Uri peerUri, string meshId, string? pathPrefix, WebRtcInvitationOffer offer, CancellationToken cancellationToken = default);

    /// <summary>
    /// Finalizes a negotiated WebRTC invitation explicitly mapping the remote answer.
    /// </summary>
    /// <param name="peerUri">The base URI of the target peer's signaling server.</param>
    /// <param name="meshId">The identifier of the active P2P mesh architecture.</param>
    /// <param name="pathPrefix">The explicit path prefix configured on the peer (optional).</param>
    /// <param name="answer">The active WebRTC answer to finalize.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>True if the finalization was acknowledged successfully, otherwise false.</returns>
    Task<bool> FinalizeInvitationAsync(Uri peerUri, string meshId, string? pathPrefix, WebRtcInvitationAnswer answer, CancellationToken cancellationToken = default);
}