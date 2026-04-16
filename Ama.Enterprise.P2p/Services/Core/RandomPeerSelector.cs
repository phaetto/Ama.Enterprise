namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Selects a randomized subset of active peers from a specific mesh, primarily suited for Epidemic/Gossip distributions.
/// </summary>
public sealed class RandomPeerSelector(
    string meshId,
    IPeerRegistry peerRegistry,
    ILogger<RandomPeerSelector> logger) : IPeerSelector
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IPeerRegistry peerRegistry = peerRegistry ?? throw new ArgumentNullException(nameof(peerRegistry));
    private readonly ILogger<RandomPeerSelector> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task<IEnumerable<PeerNode>> GetPeersAsync(int count, CancellationToken cancellationToken)
    {
        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "Count must be greater than zero.");
        }

        var activePeers = await peerRegistry.GetPeersByStatusAsync(meshId, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
        var peerList = activePeers.ToList();

        if (peerList.Count == 0)
        {
            logger.LogDebug("[{MeshId}] No active peers available for selection.", meshId);
            return Enumerable.Empty<PeerNode>();
        }

        // Shuffle the list and take the requested amount
        Shuffle(peerList);

        var selectedPeers = peerList.Take(count).ToList();
        
        logger.LogTrace("[{MeshId}] Selected {Count} peers out of {Total} active peers.", meshId, selectedPeers.Count, peerList.Count);
        
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