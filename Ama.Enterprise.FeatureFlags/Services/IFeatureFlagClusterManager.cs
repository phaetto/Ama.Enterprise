namespace Ama.Enterprise.FeatureFlags.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.FeatureFlags.Models;

/// <summary>
/// Domain wrapper interfacing explicitly with generic distributed state elements to govern application feature toggles.
/// </summary>
public interface IFeatureFlagClusterManager
{
    /// <summary>
    /// Event triggered when the state of the feature flags has been updated, either locally or remotely.
    /// </summary>
    event EventHandler? StateChanged;

    /// <summary>
    /// Gets the current read-only state map dictionary populated from replicated topology.
    /// </summary>
    IReadOnlyDictionary<string, FeatureFlag> GetFlags();

    /// <summary>
    /// Upserts a distributed feature toggle into the replicated log tracking state convergence.
    /// </summary>
    Task SetFlagAsync(
        string name, 
        bool isEnabled, 
        string? modifiedBy = null,
        FeatureFlagMetadata? metadata = null,
        FeatureFlagOwnership? ownership = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a feature toggle and marks tombstoned intentions across anti-entropy exchanges.
    /// </summary>
    Task RemoveFlagAsync(string name, CancellationToken cancellationToken = default);
}