namespace Ama.Enterprise.P2p.Services.Core;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines an observer pattern for services that need to react to changes in the P2P network topology.
/// Useful for updating UI, triggering CRDT full-syncs, or adjusting load balancers.
/// </summary>
public interface IPeerTopologyObserver
{
    /// <summary>
    /// Invoked when a new peer is successfully discovered, authenticated, and added to a specific mesh registry.
    /// </summary>
    /// <param name="meshId">The explicit mesh context identifier.</param>
    /// <param name="node">The newly joined peer.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task OnPeerJoinedAsync(string meshId, PeerNode node, CancellationToken cancellationToken);

    /// <summary>
    /// Invoked when a peer is declared dead by the failure detector and removed from a specific mesh registry.
    /// </summary>
    /// <param name="meshId">The explicit mesh context identifier.</param>
    /// <param name="peerId">The identifier of the departed peer.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task OnPeerDepartedAsync(string meshId, PeerId peerId, CancellationToken cancellationToken);

    /// <summary>
    /// Invoked when a peer's status changes (e.g., from Active to Suspect) within a specific mesh.
    /// </summary>
    /// <param name="meshId">The explicit mesh context identifier.</param>
    /// <param name="peerId">The identifier of the peer.</param>
    /// <param name="newStatus">The updated status.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task OnPeerStatusChangedAsync(string meshId, PeerId peerId, PeerStatus newStatus, CancellationToken cancellationToken);
}