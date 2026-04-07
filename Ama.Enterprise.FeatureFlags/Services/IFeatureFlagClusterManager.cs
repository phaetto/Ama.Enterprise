namespace Ama.Enterprise.FeatureFlags.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.Enterprise.FeatureFlags.Models;

/// <summary>
/// Manages the local feature flag state and synchronizes with the cluster using DVV.
/// </summary>
public interface IFeatureFlagClusterManager
{
    /// <summary>
    /// Event triggered when the state of the feature flags has been updated, either locally or remotely.
    /// </summary>
    event EventHandler? StateChanged;

    /// <summary>
    /// Gets the current read-only state of feature flags.
    /// </summary>
    IReadOnlyDictionary<string, FeatureFlag> GetFlags();

    /// <summary>
    /// Updates or adds a feature flag.
    /// </summary>
    Task SetFlagAsync(string name, bool isEnabled, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a feature flag.
    /// </summary>
    Task RemoveFlagAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the current local Dotted Version Vector representing the state of this replica.
    /// </summary>
    DottedVersionVector GetLocalState();

    /// <summary>
    /// Calculates what operations are needed by a remote replica to catch up with this replica.
    /// </summary>
    Task<IReadOnlyList<CrdtOperation>> GetMissingOperationsAsync(string remoteReplicaId, DottedVersionVector remoteState, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies operations received from another replica to the local feature flags state.
    /// </summary>
    Task ApplyOperationsAsync(IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default);
}