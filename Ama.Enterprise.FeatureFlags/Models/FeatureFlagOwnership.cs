namespace Ama.Enterprise.FeatureFlags.Models;

/// <summary>
/// Ownership details specifying team contact routing for a feature flag.
/// </summary>
public readonly record struct FeatureFlagOwnership(
    string? Owner,
    string? ContactInfo);