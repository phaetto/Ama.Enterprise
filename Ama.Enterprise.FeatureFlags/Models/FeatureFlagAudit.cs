namespace Ama.Enterprise.FeatureFlags.Models;

using System;

/// <summary>
/// Audit trace structure tracking temporal modifications for a specific feature flag.
/// </summary>
public readonly record struct FeatureFlagAudit(
    string? LastModifiedBy,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);