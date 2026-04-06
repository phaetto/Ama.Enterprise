using Ama.Enterprise.P2p.Models;
using Microsoft.Extensions.Logging;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// Selects a randomized subset of active peers for gossip distribution.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="RandomPeerSelector"/> class.
/// </remarks>
public sealed class RandomPeerSelector(
    IPeerRegistry peerRegistry,
    ILogger<RandomPeerSelector> logger) : IPeerSelector
{
    private readonly IPeerRegistry peerRegistry = peerRegistry ?? throw new ArgumentNullException(nameof(peerRegistry));
    private readonly ILogger<RandomPeerSelector> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task<IEnumerable<PeerNode>> GetPeersForGossipAsync(int fanout, CancellationToken cancellationToken)
    {
        if (fanout <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(fanout), "Fanout must be greater than zero.");
        }

        var activePeers = await this.peerRegistry.GetPeersByStatusAsync(PeerStatus.Active, cancellationToken).ConfigureAwait(false);
        var peerList = activePeers.ToList();

        if (peerList.Count == 0)
        {
            this.logger.LogDebug("No active peers available for gossip selection.");
            return Enumerable.Empty<PeerNode>();
        }

        // Shuffle the list and take the requested fanout
        this.Shuffle(peerList);

        var selectedPeers = peerList.Take(fanout).ToList();
        
        this.logger.LogTrace("Selected {Count} peers out of {Total} active peers for gossip.", selectedPeers.Count, peerList.Count);
        
        return selectedPeers;
    }

    private void Shuffle(IList<PeerNode> list)
    {
        int count = list.Count;
        while (count > 1)
        {
            count--;
            int k = Random.Shared.Next(count + 1);
            (list[k], list[count]) = (list[count], list[k]);
        }
    }
}