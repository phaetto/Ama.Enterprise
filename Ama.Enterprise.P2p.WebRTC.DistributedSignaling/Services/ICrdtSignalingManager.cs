namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.WebRTC.Models;

/// <summary>
/// Contract managing the WebRTC out-of-band signaling drop-box backed by a distributed CRDT state.
/// </summary>
public interface ICrdtSignalingManager
{
    /// <summary>
    /// Triggered when the overarching signaling document state changes structurally across the mesh.
    /// </summary>
    event EventHandler? StateChanged;

    /// <summary>
    /// Retrieves the current map of available WebRTC SDP offers.
    /// </summary>
    /// <param name="documentId">The target document ID (defaulting to the global signaling hub if null/empty).</param>
    /// <returns>A read-only dictionary of mapped offers.</returns>
    IReadOnlyDictionary<string, WebRtcInvitationOffer> GetOffers(string? documentId = null);

    /// <summary>
    /// Retrieves the current map of available WebRTC SDP answers.
    /// </summary>
    /// <param name="documentId">The target document ID (defaulting to the global signaling hub if null/empty).</param>
    /// <returns>A read-only dictionary of mapped answers.</returns>
    IReadOnlyDictionary<string, WebRtcInvitationAnswer> GetAnswers(string? documentId = null);

    /// <summary>
    /// Pushes a new WebRTC SDP offer into the distributed CRDT mesh aggressively.
    /// </summary>
    /// <param name="offer">The generated WebRTC offer.</param>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SetOfferAsync(WebRtcInvitationOffer offer, string? documentId = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes an existing WebRTC SDP offer from the distributed CRDT mesh globally.
    /// </summary>
    /// <param name="connectionId">The target connection identifier.</param>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RemoveOfferAsync(Guid connectionId, string? documentId = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Pushes a new WebRTC SDP answer into the distributed CRDT mesh aggressively.
    /// </summary>
    /// <param name="answer">The generated WebRTC answer.</param>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SetAnswerAsync(WebRtcInvitationAnswer answer, string? documentId = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes an existing WebRTC SDP answer from the distributed CRDT mesh globally.
    /// </summary>
    /// <param name="connectionId">The target connection identifier.</param>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RemoveAnswerAsync(Guid connectionId, string? documentId = null, CancellationToken cancellationToken = default);
}