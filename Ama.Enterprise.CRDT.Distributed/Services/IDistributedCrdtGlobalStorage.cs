namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;

/// <summary>
/// Defines an optional persistence mechanism for the global distributed CRDT replica state.
/// Implement this interface to load and save the global Dotted Version Vector mapping exactly to the node's tracking history.
/// </summary>
public interface IDistributedCrdtGlobalStorage
{
    /// <summary>
    /// Loads the globally tracked version vector representing the entire replica context state.
    /// </summary>
    /// <param name="replicaId">The active node replica ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The stored DottedVersionVector, or null if it doesn't exist.</returns>
    Task<DottedVersionVector?> LoadGlobalVersionVectorAsync(string replicaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Persists the global replica version vector strictly after operations map appropriately across the bounds.
    /// </summary>
    /// <param name="replicaId">The active node replica ID.</param>
    /// <param name="globalVersionVector">The newly updated complete DottedVersionVector.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous save operation.</returns>
    Task SaveGlobalVersionVectorAsync(string replicaId, DottedVersionVector globalVersionVector, CancellationToken cancellationToken = default);
}