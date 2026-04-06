namespace Ama.Enterprise.P2p.Services.Core;

using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Selects a randomized subset of active peers, primarily suited for Epidemic/Gossip distributions.
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
    public async Task<IEnumerable<PeerNode>> GetPeersAsync(int count, CancellationToken cancellationToken)
    {
        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "Count must be greater than zero.");
        }

        var activePeers = await this.peerRegistry.GetPeersByStatusAsync(PeerStatus.Active, cancellationToken).ConfigureAwait(false);
        var peerList = activePeers.ToList();

        if (peerList.Count == 0)
        {
            this.logger.LogDebug("No active peers available for selection.");
            return Enumerable.Empty<PeerNode>();
        }

        // Shuffle the list and take the requested amount
        this.Shuffle(peerList);

        var selectedPeers = peerList.Take(count).ToList();
        
        this.logger.LogTrace("Selected {Count} peers out of {Total} active peers.", selectedPeers.Count, peerList.Count);
        
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