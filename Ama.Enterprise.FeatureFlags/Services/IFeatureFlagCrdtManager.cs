namespace Ama.Enterprise.FeatureFlags.Services;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.FeatureFlags.Models;

/// <summary>
/// Interface for managing distributed feature toggles sharded per product and tenant document.
/// </summary>
/// <example>
/// <code>
/// await featureFlagManager.SetFlagAsync("billing", "acme", "new-checkout", true, deliveryMode: IntentDeliveryMode.Acknowledged, cancellationToken: cancellationToken);
/// var flags = await featureFlagManager.GetFlagsAsync("billing", "acme", cancellationToken);
/// </code>
/// </example>
public interface IFeatureFlagCrdtManager
{
    /// <summary>
    /// Gets the read-only map of all feature flags across all product and tenant documents in the cluster asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A read-only dictionary of feature flags across the cluster.</returns>
    ValueTask<IReadOnlyDictionary<string, FeatureFlag>> GetAllFlagsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the read-only map of feature flags for a specific product and tenant document shard asynchronously.
    /// </summary>
    /// <param name="productId">The product identifier.</param>
    /// <param name="tenantId">The tenant identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A read-only dictionary of feature flags keyed by flag name for that product and tenant.</returns>
    ValueTask<IReadOnlyDictionary<string, FeatureFlag>> GetFlagsAsync(string productId, string tenantId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Upserts a feature flag into the document shard for the specified product and tenant.
    /// </summary>
    /// <param name="productId">The product identifier.</param>
    /// <param name="tenantId">The tenant identifier.</param>
    /// <param name="name">The unique name of the feature flag within the product and tenant scope.</param>
    /// <param name="isEnabled">Indicates whether the feature flag is enabled.</param>
    /// <param name="modifiedBy">The identifier of the user or system modifying the flag.</param>
    /// <param name="ownership">Optional ownership details for the feature flag.</param>
    /// <param name="deliveryMode">The delivery mode for the operation intent.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SetFlagAsync(
        string productId,
        string tenantId,
        string name, 
        bool isEnabled, 
        string? modifiedBy = null,
        FeatureFlagOwnership? ownership = null,
        IntentDeliveryMode deliveryMode = IntentDeliveryMode.Acknowledged,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a feature flag from a specific product and tenant document shard.
    /// </summary>
    /// <param name="productId">The product identifier of the document shard.</param>
    /// <param name="tenantId">The tenant identifier of the document shard.</param>
    /// <param name="name">The unique name of the feature flag to remove.</param>
    /// <param name="deliveryMode">The delivery mode for the operation intent.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RemoveFlagAsync(
        string productId,
        string tenantId,
        string name,
        IntentDeliveryMode deliveryMode = IntentDeliveryMode.Acknowledged,
        CancellationToken cancellationToken = default);
}