namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Implements a scalable, thread-safe memory registry explicitly preserving decoupled multi-mesh peer sessions natively.
/// </summary>
public sealed class InMemoryPeerSessionRegistry : IPeerSessionRegistry
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<PeerId, SessionContext>> meshSessions = new();

    /// <inheritdoc />
    public Task AddOrUpdateSessionAsync(string meshId, PeerId peerId, SessionContext session, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);

        if (peerId.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer identifier cannot be explicitly empty.", nameof(peerId));
        }

        var registry = meshSessions.GetOrAdd(meshId, _ => new ConcurrentDictionary<PeerId, SessionContext>());
        registry.AddOrUpdate(peerId, session, (_, _) => session);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<SessionContext?> GetSessionAsync(string meshId, PeerId peerId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);

        if (meshSessions.TryGetValue(meshId, out var registry) && registry.TryGetValue(peerId, out var session))
        {
            if (session.ExpiresAt > DateTimeOffset.UtcNow)
            {
                return Task.FromResult<SessionContext?>(session);
            }

            registry.TryRemove(peerId, out _);
        }

        return Task.FromResult<SessionContext?>(null);
    }

    /// <inheritdoc />
    public Task RemoveSessionAsync(string meshId, PeerId peerId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);

        if (meshSessions.TryGetValue(meshId, out var registry))
        {
            registry.TryRemove(peerId, out _);
        }

        return Task.CompletedTask;
    }
}