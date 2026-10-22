namespace Ama.Enterprise.FeatureFlags.Services;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.FeatureFlags.Models;

/// <summary>
/// Interface for managing distributed feature toggles.
/// </summary>
/// <example>
/// <code>
/// await featureFlagManager.SetFlagAsync("new-checkout", true, deliveryMode: IntentDeliveryMode.Acknowledged, cancellationToken: cancellationToken);
/// var flags = await featureFlagManager.GetFlagsAsync(cancellationToken);
/// </code>
/// </example>
public interface IFeatureFlagCrdtManager
{
    /// <summary>
    /// Gets the current read-only map of feature flags asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A read-only dictionary of feature flags keyed by flag name.</returns>
    ValueTask<IReadOnlyDictionary<string, FeatureFlag>> GetFlagsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Upserts a feature flag.
    /// </summary>
    /// <param name="name">The unique name of the feature flag.</param>
    /// <param name="isEnabled">Indicates whether the feature flag is enabled.</param>
    /// <param name="modifiedBy">The identifier of the user or system modifying the flag.</param>
    /// <param name="metadata">Optional metadata for the feature flag.</param>
    /// <param name="ownership">Optional ownership details for the feature flag.</param>
    /// <param name="deliveryMode">The delivery mode for the operation intent.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SetFlagAsync(
        string name, 
        bool isEnabled, 
        string? modifiedBy = null,
        FeatureFlagMetadata? metadata = null,
        FeatureFlagOwnership? ownership = null,
        IntentDeliveryMode deliveryMode = IntentDeliveryMode.Acknowledged,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a feature flag using the default acknowledged delivery mode.
    /// </summary>
    /// <param name="name">The unique name of the feature flag to remove.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RemoveFlagAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a feature flag with the specified delivery mode.
    /// </summary>
    /// <param name="name">The unique name of the feature flag to remove.</param>
    /// <param name="deliveryMode">The delivery mode for the operation intent.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RemoveFlagAsync(string name, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default);
}