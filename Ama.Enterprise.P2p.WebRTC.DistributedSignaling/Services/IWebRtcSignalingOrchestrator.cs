namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Services;

using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// High-level orchestrator client that manages WebRTC signaling connections and acts as a room coordinator.
/// Abstracts underlying CRDT HTTP signaling requests dynamically targeting peers locally executing SDP negotiations based on mesh presence logic.
/// </summary>
public interface IWebRtcSignalingOrchestrator
{
    /// <summary>
    /// Publishes the local node's Peer ID as a Join Intent signifying physical presence establishing dynamic SDP offer capabilities across the room bounds.
    /// </summary>
    /// <param name="meshId">The overarching generic mesh identifier.</param>
    /// <param name="documentId">The target document ID isolating the signaling boundaries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous remote operation.</returns>
    Task PublishJoinIntentAsync(string meshId, string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Polls the overarching CRDT document retrieving active peer presence intents, and specifically targets unique generated SDP offers towards matching peers executing logical deterministic order constraints.
    /// </summary>
    /// <param name="meshId">The overarching generic mesh identifier.</param>
    /// <param name="documentId">The target document ID isolating the signaling boundaries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous remote operation.</returns>
    Task ProcessJoinIntentsAsync(string meshId, string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Polls the signaling document specifically for SDP offers intentionally mapped toward the local peer identifier executing matching targeted SDP answers routing them identically.
    /// </summary>
    /// <param name="meshId">The overarching generic mesh identifier.</param>
    /// <param name="documentId">The target document ID isolating the signaling boundaries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous remote operation.</returns>
    Task ProcessPendingOffersAsync(string meshId, string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Polls the signaling document specifically for active answers directed back toward the local identifier originating localized specific SDP constraints finalizing the native connections natively.
    /// </summary>
    /// <param name="meshId">The overarching generic mesh identifier.</param>
    /// <param name="documentId">The target document ID isolating the signaling boundaries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous remote operation.</returns>
    Task ProcessPendingAnswersAsync(string meshId, string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Clears internal tracked state matching processed offers and answers structurally freeing dictionary bounds.
    /// </summary>
    void ClearLocalState();
}