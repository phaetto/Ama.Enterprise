namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Defines a service that orchestrates replica eviction and local identity re-bootstrapping across all CRDT documents.
/// </summary>
/// <example>
/// <code>
/// await evictionService.EvictPeersAsync(["replica-1"], IntentDeliveryMode.Acknowledged, cancellationToken);
/// </code>
/// </example>
public interface ICrdtEvictionService
{
    /// <summary>
    /// Evicts the state and metadata of the specified peer replicas from all active documents and version vectors.
    /// </summary>
    /// <param name="replicaIds">The list of replica identifiers to evict.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task EvictPeersAsync(IReadOnlyList<string> replicaIds, CancellationToken cancellationToken = default);

    /// <summary>
    /// Evicts the state and metadata of the specified peer replicas with the specified delivery mode.
    /// </summary>
    /// <param name="replicaIds">The list of replica identifiers to evict.</param>
    /// <param name="deliveryMode">The delivery mode specifying whether to wait for completion or enqueue as fire-and-forget.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task EvictPeersAsync(IReadOnlyList<string> replicaIds, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Re-bootstraps the local replica identity and resets all local document states after being tombstoned by the cluster.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RebootLocalIdentityAsync(CancellationToken cancellationToken = default);
}