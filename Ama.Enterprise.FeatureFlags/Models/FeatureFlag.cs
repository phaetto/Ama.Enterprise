namespace Ama.Enterprise.FeatureFlags.Models;

/// <summary>
/// Represents a single enterprise feature flag in the system incorporating audit, ownership, and metadata structures.
/// </summary>
public readonly record struct FeatureFlag(
    string Name,
    bool IsEnabled,
    FeatureFlagMetadata Metadata,
    FeatureFlagAudit Audit,
    FeatureFlagOwnership Ownership);