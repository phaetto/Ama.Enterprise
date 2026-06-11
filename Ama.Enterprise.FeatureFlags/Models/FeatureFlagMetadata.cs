namespace Ama.Enterprise.FeatureFlags.Models;
/// <summary>
/// Custom metadata structure encapsulating enterprise multi-tenancy and product domain boundaries.
/// </summary>
public readonly record struct FeatureFlagMetadata(
    string? ProductId,
    string? TenantId);