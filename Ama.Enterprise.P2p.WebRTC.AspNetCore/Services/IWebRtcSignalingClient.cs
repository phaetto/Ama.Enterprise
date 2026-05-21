namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.WebRTC.Models;

/// <summary>
/// Defines a client for invoking explicit WebSockets WebRTC signaling API endpoints managed by the decentralized topology natively.
/// </summary>
public interface IWebRtcSignalingClient
{
    /// <summary>
    /// Initiates a persistent WebSockets connection to the remote peer to explicitly orchestrate an out-of-band WebRTC handshake safely executing Answer evaluations natively.
    /// </summary>
    /// <param name="peerUri">The base URI of the target peer's signaling server.</param>
    /// <param name="meshId">The identifier of the active P2P mesh architecture.</param>
    /// <param name="pathPrefix">The explicit path prefix configured on the peer (optional).</param>
    /// <param name="answerFactory">A delegate orchestrator invoking the local SDP answer evaluations based on the remote peer's explicit offer effectively.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>The remote peer connection ID if the negotiation concluded properly, otherwise null.</returns>
    Task<string?> NegotiateOfferAsync(
        Uri peerUri, 
        string meshId, 
        string? pathPrefix, 
        Func<WebRtcInvitationOffer, CancellationToken, Task<WebRtcInvitationAnswer>> answerFactory, 
        CancellationToken cancellationToken = default);
}