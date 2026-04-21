namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implements an in-memory thread-safe registry tracking peering topology globally across configured multiplexed networks.
/// </summary>
public sealed class InMemoryPeerRegistry(
    IEnumerable<IPeerTopologyObserver> topologyObservers,
    ILogger<InMemoryPeerRegistry> logger) : IPeerRegistry
{
    private readonly IEnumerable<IPeerTopologyObserver> topologyObservers = topologyObservers ?? throw new ArgumentNullException(nameof(topologyObservers));
    private readonly ILogger<InMemoryPeerRegistry> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    private readonly ConcurrentDictionary<PeerRegistryKey, PeerEntry> peers = new();

    /// <inheritdoc />
    public async Task AddOrUpdatePeerAsync(string meshId, PeerNode node, PeerStatus status, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(meshId))
        {
            throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));
        }

        if (node.Id.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID cannot be empty.", nameof(node));
        }

        var key = new PeerRegistryKey(meshId, node.Id);
        var entry = new PeerEntry(meshId, node, status);
        var isNew = false;
        PeerStatus? oldStatus = null;

        peers.AddOrUpdate(
            key,
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
            logger.LogInformation("[{MeshId}] New peer joined the registry: {PeerId}", meshId, node.Id.Value);
            await NotifyObserversAsync(observer => observer.OnPeerJoinedAsync(meshId, node, cancellationToken)).ConfigureAwait(false);
        }
        else if (oldStatus.HasValue && oldStatus.Value != status)
        {
            logger.LogInformation("[{MeshId}] Peer {PeerId} status changed from {OldStatus} to {NewStatus}", meshId, node.Id.Value, oldStatus.Value, status);
            await NotifyObserversAsync(observer => observer.OnPeerStatusChangedAsync(meshId, node.Id, status, cancellationToken)).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemovePeerAsync(string meshId, PeerId peerId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(meshId))
        {
            throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));
        }

        if (peerId.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID cannot be empty.", nameof(peerId));
        }

        var key = new PeerRegistryKey(meshId, peerId);
        if (peers.TryRemove(key, out _))
        {
            logger.LogInformation("[{MeshId}] Peer {PeerId} was removed from the registry.", meshId, peerId.Value);
            await NotifyObserversAsync(observer => observer.OnPeerDepartedAsync(meshId, peerId, cancellationToken)).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public Task<IEnumerable<PeerNode>> GetAllPeersAsync(string meshId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(meshId))
        {
            throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));
        }

        cancellationToken.ThrowIfCancellationRequested();
        
        var result = peers.Values
            .Where(entry => string.Equals(entry.MeshId, meshId, StringComparison.Ordinal))
            .Select(entry => entry.Node)
            .ToList();

        return Task.FromResult<IEnumerable<PeerNode>>(result);
    }

    /// <inheritdoc />
    public Task<IEnumerable<PeerNode>> GetPeersByStatusAsync(string meshId, PeerStatus status, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(meshId))
        {
            throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));
        }

        cancellationToken.ThrowIfCancellationRequested();
        
        var result = peers.Values
            .Where(entry => string.Equals(entry.MeshId, meshId, StringComparison.Ordinal) && entry.Status == status)
            .Select(entry => entry.Node)
            .ToList();

        return Task.FromResult<IEnumerable<PeerNode>>(result);
    }

    private async Task NotifyObserversAsync(Func<IPeerTopologyObserver, Task> action)
    {
        foreach (var observer in topologyObservers)
        {
            try
            {
                await action(observer).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred while notifying topology observer {ObserverType}.", observer.GetType().Name);
            }
        }
    }

    private readonly record struct PeerRegistryKey(string MeshId, PeerId PeerId);

    private readonly record struct PeerEntry(string MeshId, PeerNode Node, PeerStatus Status);
}