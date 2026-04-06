namespace Ama.Enterprise.P2p.Services.Core;

using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines the strategy for selecting a subset of peers from the routing table for communication.
/// </summary>
public interface IPeerSelector
{
    /// <summary>
    /// Selects a specified number of optimal peers from the routing table.
    /// </summary>
    /// <param name="count">The maximum number of peers to select.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous selection operation. The task result contains an enumerable of selected peers.</returns>
    Task<IEnumerable<PeerNode>> GetPeersAsync(int count, CancellationToken cancellationToken);
}