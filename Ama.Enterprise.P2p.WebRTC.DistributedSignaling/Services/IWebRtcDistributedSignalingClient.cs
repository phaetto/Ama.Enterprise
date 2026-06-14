namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.WebRTC.Models;

/// <summary>
/// Defines a client for invoking WebRTC out-of-band signaling API endpoints managed by the decentralized topology HTTP hooks.
/// </summary>
public interface IWebRtcDistributedSignalingClient
{
    /// <summary>
    /// Retrieves the current map of available node Join Intents signifying presence inside the remote topology bounds.
    /// </summary>
    /// <param name="replicaId">The specific isolated local CRDT replica identifier executing explicit boundaries.</param>
    /// <param name="documentId">The target document ID isolating the signaling boundaries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A dictionary of remotely mapped Join Intents tracking timestamp bounds.</returns>
    Task<IReadOnlyDictionary<string, long>> GetJoinIntentsAsync(string replicaId, string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Pushes a new presence join intent directly asserting the remote mapped Peer's status inside the topology bounds.
    /// </summary>
    /// <param name="replicaId">The specific isolated local CRDT replica identifier executing explicit boundaries.</param>
    /// <param name="peerId">The specific remote unique node ID.</param>
    /// <param name="documentId">The target document ID isolating the signaling boundaries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous remote operation.</returns>
    Task SetJoinIntentAsync(string replicaId, Guid peerId, string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a targeted peer intent signifying an offline or departed state.
    /// </summary>
    /// <param name="replicaId">The specific isolated local CRDT replica identifier executing explicit boundaries.</param>
    /// <param name="peerId">The target peer node ID.</param>
    /// <param name="documentId">The target document ID isolating the signaling boundaries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous remote operation.</returns>
    Task RemoveJoinIntentAsync(string replicaId, Guid peerId, string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the current map of available WebRTC SDP offers from the targeted remote endpoint.
    /// </summary>
    /// <param name="replicaId">The specific isolated local CRDT replica identifier executing explicit boundaries.</param>
    /// <param name="documentId">The target document ID isolating the signaling boundaries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A dictionary of remotely mapped offers tracking SDP state bounds.</returns>
    Task<IReadOnlyDictionary<string, WebRtcInvitationOffer>> GetOffersAsync(string replicaId, string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the current map of available WebRTC SDP answers from the targeted remote endpoint.
    /// </summary>
    /// <param name="replicaId">The specific isolated local CRDT replica identifier executing explicit boundaries.</param>
    /// <param name="documentId">The target document ID isolating the signaling boundaries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A dictionary of remotely mapped answers tracking SDP state bounds.</returns>
    Task<IReadOnlyDictionary<string, WebRtcInvitationAnswer>> GetAnswersAsync(string replicaId, string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Pushes a new WebRTC SDP offer into the distributed CRDT mesh aggressively through the HTTP interface.
    /// </summary>
    /// <param name="replicaId">The specific isolated local CRDT replica identifier executing explicit boundaries.</param>
    /// <param name="routingKey">The matched deterministic targeted routing identifier.</param>
    /// <param name="offer">The generated WebRTC offer.</param>
    /// <param name="documentId">The target document ID isolating the signaling boundaries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous remote operation.</returns>
    Task SetOfferAsync(string replicaId, string routingKey, WebRtcInvitationOffer offer, string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes an existing WebRTC SDP offer from the distributed CRDT mesh globally over the HTTP interface.
    /// </summary>
    /// <param name="replicaId">The specific isolated local CRDT replica identifier executing explicit boundaries.</param>
    /// <param name="routingKey">The custom matched deterministic mapping identifier.</param>
    /// <param name="documentId">The target document ID isolating the signaling boundaries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous remote operation.</returns>
    Task RemoveOfferAsync(string replicaId, string routingKey, string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Pushes a new WebRTC SDP answer into the distributed CRDT mesh aggressively over the HTTP interface.
    /// </summary>
    /// <param name="replicaId">The specific isolated local CRDT replica identifier executing explicit boundaries.</param>
    /// <param name="routingKey">The custom matched deterministic mapping identifier.</param>
    /// <param name="answer">The generated WebRTC answer.</param>
    /// <param name="documentId">The target document ID isolating the signaling boundaries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous remote operation.</returns>
    Task SetAnswerAsync(string replicaId, string routingKey, WebRtcInvitationAnswer answer, string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes an existing WebRTC SDP answer from the distributed CRDT mesh globally over the HTTP interface.
    /// </summary>
    /// <param name="replicaId">The specific isolated local CRDT replica identifier executing explicit boundaries.</param>
    /// <param name="routingKey">The custom matched deterministic mapping identifier.</param>
    /// <param name="documentId">The target document ID isolating the signaling boundaries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous remote operation.</returns>
    Task RemoveAnswerAsync(string replicaId, string routingKey, string documentId, CancellationToken cancellationToken = default);
}