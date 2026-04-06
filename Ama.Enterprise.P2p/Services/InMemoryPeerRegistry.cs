using System.Collections.Concurrent;
using Ama.Enterprise.P2p.Models;
using Microsoft.Extensions.Logging;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// Implements an in-memory thread-safe registry for managing known peers and their statuses.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="InMemoryPeerRegistry"/> class.
/// </remarks>
public sealed class InMemoryPeerRegistry(
    IEnumerable<IPeerTopologyObserver> topologyObservers,
    ILogger<InMemoryPeerRegistry> logger) : IPeerRegistry
{
    private readonly IEnumerable<IPeerTopologyObserver> topologyObservers = topologyObservers ?? throw new ArgumentNullException(nameof(topologyObservers));
    private readonly ILogger<InMemoryPeerRegistry> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    private readonly ConcurrentDictionary<PeerId, PeerEntry> peers = new ConcurrentDictionary<PeerId, PeerEntry>();

    /// <inheritdoc />
    public async Task AddOrUpdatePeerAsync(PeerNode node, PeerStatus status, CancellationToken cancellationToken)
    {
        if (node.Id.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID cannot be empty.", nameof(node));
        }

        var isNew = false;
        PeerStatus? oldStatus = null;
        var entry = new PeerEntry(node, status);

        this.peers.AddOrUpdate(
            node.Id,
            _ =>
            {
                isNew = true;
                return entry;
            },
            (_, existing) =>
            {
                oldStatus = existing.Status;
                return entry;
            });

        if (isNew)
        {
            this.logger.LogInformation("New peer joined the registry: {PeerId} at {Endpoint}", node.Id.Value, node.Endpoint.Host);
            await this.NotifyObserversAsync(observer => observer.OnPeerJoinedAsync(node, cancellationToken)).ConfigureAwait(false);
        }
        else if (oldStatus.HasValue && oldStatus.Value != status)
        {
            this.logger.LogInformation("Peer {PeerId} status changed from {OldStatus} to {NewStatus}", node.Id.Value, oldStatus.Value, status);
            await this.NotifyObserversAsync(observer => observer.OnPeerStatusChangedAsync(node.Id, status, cancellationToken)).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemovePeerAsync(PeerId peerId, CancellationToken cancellationToken)
    {
        if (peerId.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID cannot be empty.", nameof(peerId));
        }

        if (this.peers.TryRemove(peerId, out _))
        {
            this.logger.LogInformation("Peer {PeerId} was removed from the registry.", peerId.Value);
            await this.NotifyObserversAsync(observer => observer.OnPeerDepartedAsync(peerId, cancellationToken)).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public Task<IEnumerable<PeerNode>> GetAllPeersAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var allPeers = this.peers.Values.Select(entry => entry.Node).ToList();
        return Task.FromResult<IEnumerable<PeerNode>>(allPeers);
    }

    /// <inheritdoc />
    public Task<IEnumerable<PeerNode>> GetPeersByStatusAsync(PeerStatus status, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var filteredPeers = this.peers.Values
            .Where(entry => entry.Status == status)
            .Select(entry => entry.Node)
            .ToList();

        return Task.FromResult<IEnumerable<PeerNode>>(filteredPeers);
    }

    private async Task NotifyObserversAsync(Func<IPeerTopologyObserver, Task> action)
    {
        foreach (var observer in this.topologyObservers)
        {
            try
            {
                await action(observer).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "An error occurred while notifying topology observer {ObserverType}.", observer.GetType().Name);
            }
        }
    }

    private readonly record struct PeerEntry(PeerNode Node, PeerStatus Status);
}