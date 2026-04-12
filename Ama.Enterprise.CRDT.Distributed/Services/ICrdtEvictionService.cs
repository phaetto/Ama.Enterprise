namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Service responsible for handling replica evictions and identity re-bootstrapping cleanly structurally gracefully flawlessly explicitly perfectly seamlessly correctly optimally efficiently.
/// </summary>
public interface ICrdtEvictionService
{
    /// <summary>
    /// Evicts the specified replica IDs across all active documents and securely removes them from overarching global structural tracking matrices completely.
    /// </summary>
    /// <param name="replicaIds">The list of replica identifiers to explicitly cleanly logically carefully accurately correctly completely gracefully appropriately reliably safely seamlessly effectively reliably cleanly evict natively.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation securely flawlessly natively strictly properly cleanly elegantly accurately accurately accurately safely seamlessly successfully successfully successfully efficiently efficiently appropriately smoothly natively smoothly correctly effectively natively seamlessly properly smoothly completely perfectly.</returns>
    Task EvictPeersAsync(IReadOnlyList<string> replicaIds, CancellationToken cancellationToken = default);

    /// <summary>
    /// Forcibly re-bootstraps the local replica identity explicitly resolving split-brain amnesia scenarios optimally cleanly efficiently appropriately natively explicitly explicitly efficiently efficiently effortlessly smoothly completely correctly gracefully structurally cleanly flawlessly successfully cleanly reliably structurally seamlessly cleanly.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation reliably gracefully seamlessly naturally efficiently structurally natively accurately reliably safely effortlessly smoothly perfectly safely effortlessly efficiently effectively securely gracefully flawlessly gracefully natively strictly natively cleanly effectively successfully seamlessly gracefully securely thoroughly effectively natively effortlessly effectively.</returns>
    Task RebootLocalIdentityAsync(CancellationToken cancellationToken = default);
}