using Ama.Enterprise.P2p.Models.Core;

namespace Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// Manages the local node's routing table and awareness of other peers.
/// </summary>
public interface IPeerRegistry
{
    /// <summary>
    /// Adds a new peer or updates the status and endpoint of an existing peer.
    /// </summary>
    /// <param name="node">The peer node details.</param>
    /// <param name="status">The current status of the peer.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous update operation.</returns>
    Task AddOrUpdatePeerAsync(PeerNode node, PeerStatus status, CancellationToken cancellationToken);

    /// <summary>
    /// Removes a peer from the registry entirely.
    /// </summary>
    /// <param name="peerId">The unique identifier of the peer to remove.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous removal operation.</returns>
    Task RemovePeerAsync(PeerId peerId, CancellationToken cancellationToken);

    /// <summary>
    /// Retrieves all peers currently tracked by the registry regardless of status.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains an enumerable of peer nodes.</returns>
    Task<IEnumerable<PeerNode>> GetAllPeersAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Retrieves a subset of peers with the specified status.
    /// </summary>
    /// <param name="status">The status to filter by.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains an enumerable of peer nodes matching the status.</returns>
    Task<IEnumerable<PeerNode>> GetPeersByStatusAsync(PeerStatus status, CancellationToken cancellationToken);
}