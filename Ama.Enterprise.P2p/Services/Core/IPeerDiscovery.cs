namespace Ama.Enterprise.P2p.Services.Core;

using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines mechanisms for discovering other peers within the network.
/// </summary>
public interface IPeerDiscovery
{
    /// <summary>
    /// Executes a discovery process to find available peers on the network.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains an enumerable of discovered peers.</returns>
    Task<IEnumerable<PeerNode>> DiscoverPeersAsync(CancellationToken cancellationToken);
}