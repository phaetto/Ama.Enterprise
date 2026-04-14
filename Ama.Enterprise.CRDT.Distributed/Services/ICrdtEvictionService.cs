namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Service responsible for handling replica evictions and identity re-bootstrapping.
/// </summary>
public interface ICrdtEvictionService
{
    /// <summary>
    /// Evicts the specified replica IDs across all active documents and removes them from global tracking matrices.
    /// </summary>
    /// <param name="replicaIds">The list of replica identifiers to evict.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task EvictPeersAsync(IReadOnlyList<string> replicaIds, CancellationToken cancellationToken = default);

    /// <summary>
    /// Forcibly re-bootstraps the local replica identity.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RebootLocalIdentityAsync(CancellationToken cancellationToken = default);
}