namespace Ama.Enterprise.FeatureFlags;

using System;

/// <summary>
/// Global constants for the Feature Flags module.
/// </summary>
public static class Constants
{
    /// <summary>
    /// The registered document type alias for feature flags.
    /// </summary>
    public const string FeatureFlagDocumentType = "feature-flag";

    /// <summary>
    /// The wire alias for the feature flag cluster query.
    /// </summary>
    public const string FeatureFlagClusterQueryAlias = "feature-flag-cluster-query";

    /// <summary>
    /// Builds a deterministic document identifier for a specific product and tenant combination.
    /// </summary>
    /// <param name="productId">The product identifier.</param>
    /// <param name="tenantId">The tenant identifier.</param>
    /// <returns>The document identifier for the given product and tenant scope.</returns>
    public static string GetDocumentId(string productId, string tenantId)
    {
        if (string.IsNullOrWhiteSpace(productId))
        {
            throw new ArgumentException("Product ID cannot be null or whitespace.", nameof(productId));
        }

        if (string.IsNullOrWhiteSpace(tenantId))
        {
            throw new ArgumentException("Tenant ID cannot be null or whitespace.", nameof(tenantId));
        }

        return $"{FeatureFlagDocumentType}:{productId.Trim()}:{tenantId.Trim()}";
    }
}