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
    /// Retrieves the current map of active node Join Intents signifying presence inside the topology bounds.
    /// </summary>
    /// <param name="documentId">The target document ID.</param>
    /// <returns>A read-only dictionary mapped by Peer IDs.</returns>
    IReadOnlyDictionary<string, long> GetJoinIntents(string? documentId = null);

    /// <summary>
    /// Pushes a new presence join intent directly asserting the Peer's status inside the topology bounds.
    /// </summary>
    /// <param name="peerId">The specific remote or local unique node ID.</param>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SetJoinIntentAsync(Guid peerId, string? documentId = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a targeted peer intent signifying an offline or departed state removing logic connections.
    /// </summary>
    /// <param name="peerId">The target peer node ID.</param>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RemoveJoinIntentAsync(Guid peerId, string? documentId = null, CancellationToken cancellationToken = default);

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
    /// Pushes a new targeted WebRTC SDP offer into the distributed CRDT mesh aggressively.
    /// </summary>
    /// <param name="routingKey">The custom deterministic targeted mapping identifier ({TargetPeerId}:{OffererPeerId}).</param>
    /// <param name="offer">The generated WebRTC offer.</param>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SetOfferAsync(string routingKey, WebRtcInvitationOffer offer, string? documentId = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes an existing WebRTC SDP offer from the distributed CRDT mesh globally.
    /// </summary>
    /// <param name="routingKey">The matched deterministic mapped routing identifier.</param>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RemoveOfferAsync(string routingKey, string? documentId = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Pushes a new targeted WebRTC SDP answer into the distributed CRDT mesh aggressively.
    /// </summary>
    /// <param name="routingKey">The custom deterministic targeted mapping identifier ({OffererPeerId}:{AnswererPeerId}).</param>
    /// <param name="answer">The generated WebRTC answer.</param>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SetAnswerAsync(string routingKey, WebRtcInvitationAnswer answer, string? documentId = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes an existing WebRTC SDP answer from the distributed CRDT mesh globally.
    /// </summary>
    /// <param name="routingKey">The matched deterministic mapped routing identifier.</param>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RemoveAnswerAsync(string routingKey, string? documentId = null, CancellationToken cancellationToken = default);
}