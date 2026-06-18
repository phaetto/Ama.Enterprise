namespace Ama.Enterprise.P2p.Services.Core;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Centralized boundary explicitly tracking multi-node session bounds preserving extracted identities mapped dynamically.
/// </summary>
public interface IPeerSessionRegistry
{
    /// <summary>
    /// Overwrites or establishes a session context for a specific peer natively.
    /// </summary>
    /// <param name="meshId">The target mesh boundary identifier.</param>
    /// <param name="peerId">The tracking peer identifier.</param>
    /// <param name="session">The validated session context limits.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    Task AddOrUpdateSessionAsync(string meshId, PeerId peerId, SessionContext session, CancellationToken cancellationToken);

    /// <summary>
    /// Retrieves a mapped session context if registered natively.
    /// </summary>
    /// <param name="meshId">The target mesh boundary identifier.</param>
    /// <param name="peerId">The targeted peer identifier.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>The session context, or null if unmapped explicitly.</returns>
    Task<SessionContext?> GetSessionAsync(string meshId, PeerId peerId, CancellationToken cancellationToken);

    /// <summary>
    /// Gracefully clears a tracked peer session structurally.
    /// </summary>
    /// <param name="meshId">The target mesh boundary identifier.</param>
    /// <param name="peerId">The target peer identifier to evict.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    Task RemoveSessionAsync(string meshId, PeerId peerId, CancellationToken cancellationToken);
}