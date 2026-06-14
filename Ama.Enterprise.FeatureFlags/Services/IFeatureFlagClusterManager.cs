namespace Ama.Enterprise.FeatureFlags.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.FeatureFlags.Models;

/// <summary>
/// Interface for managing distributed feature toggles.
/// </summary>
public interface IFeatureFlagClusterManager
{
    /// <summary>
    /// Event triggered when the state of the feature flags has been updated.
    /// </summary>
    event EventHandler? StateChanged;

    /// <summary>
    /// Gets the current read-only map of feature flags.
    /// </summary>
    IReadOnlyDictionary<string, FeatureFlag> GetFlags();

    /// <summary>
    /// Upserts a feature flag.
    /// </summary>
    Task SetFlagAsync(
        string name, 
        bool isEnabled, 
        string? modifiedBy = null,
        FeatureFlagMetadata? metadata = null,
        FeatureFlagOwnership? ownership = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a feature flag.
    /// </summary>
    Task RemoveFlagAsync(string name, CancellationToken cancellationToken = default);
}