namespace Ama.Enterprise.FeatureFlags.Models;

/// <summary>
/// Metadata structure encapsulating enterprise multi-tenancy and product domain data.
/// </summary>
public readonly record struct FeatureFlagMetadata(
    string? ProductId,
    string? TenantId);