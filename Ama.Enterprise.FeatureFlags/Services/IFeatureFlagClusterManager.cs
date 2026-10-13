namespace Ama.Enterprise.FeatureFlags.Services;

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
    /// Gets the current read-only map of feature flags asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    ValueTask<IReadOnlyDictionary<string, FeatureFlag>> GetFlagsAsync(CancellationToken cancellationToken = default);

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