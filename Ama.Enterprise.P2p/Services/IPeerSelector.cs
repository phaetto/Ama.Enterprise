using Ama.Enterprise.P2p.Models;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// Defines the strategy for selecting a subset of peers to gossip with during a protocol tick.
/// </summary>
public interface IPeerSelector
{
    /// <summary>
    /// Selects a specified number of optimal peers from the routing table for the next gossip transmission.
    /// </summary>
    /// <param name="fanout">The maximum number of peers to select.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous selection operation. The task result contains an enumerable of selected peers.</returns>
    Task<IEnumerable<PeerNode>> GetPeersForGossipAsync(int fanout, CancellationToken cancellationToken);
}