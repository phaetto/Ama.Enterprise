namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Selects a randomized subset of active peers from a specific mesh, primarily suited for Epidemic/Gossip distributions.
/// </summary>
public sealed class RandomPeerSelector : IPeerSelector, IDisposable
{
    private readonly string meshId;
    private readonly IPeerRegistry peerRegistry;
    private readonly ILogger<RandomPeerSelector> logger;

    private readonly Meter meter;
    private readonly Counter<long> requestsCounter;
    private readonly Histogram<long> selectedPeersHistogram;

    public RandomPeerSelector(
        string meshId,
        IPeerRegistry peerRegistry,
        ILogger<RandomPeerSelector> logger,
        IMeterFactory? meterFactory = null)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.peerRegistry = peerRegistry ?? throw new ArgumentNullException(nameof(peerRegistry));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.RandomPeerSelector") ?? new Meter("Ama.Enterprise.P2p.RandomPeerSelector");
        this.requestsCounter = this.meter.CreateCounter<long>("p2p.selector.random.requests", "requests", "Total random peer selection requests");
        this.selectedPeersHistogram = this.meter.CreateHistogram<long>("p2p.selector.random.selected_peers", "peers", "Number of peers selected and returned");
    }

    /// <inheritdoc />
    public async Task<IEnumerable<PeerNode>> GetPeersAsync(int count, CancellationToken cancellationToken)
    {
        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "Count must be greater than zero.");
        }

        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
        requestsCounter.Add(1, tags);

        var activePeers = await peerRegistry.GetPeersByStatusAsync(meshId, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
        var peerList = activePeers.ToList();

        if (peerList.Count == 0)
        {
            selectedPeersHistogram.Record(0, tags);
            return Enumerable.Empty<PeerNode>();
        }

        // Shuffle the list and take the requested amount
        Shuffle(peerList);

        var selectedPeers = peerList.Take(count).ToList();
        
        logger.LogTrace("[{MeshId}] Selected {Count} peers out of {Total} active peers.", meshId, selectedPeers.Count, peerList.Count);
        
        selectedPeersHistogram.Record(selectedPeers.Count, tags);
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

    /// <inheritdoc />
    public void Dispose()
    {
        meter.Dispose();
    }
}