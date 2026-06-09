namespace Ama.Enterprise.FeatureFlags.Models;

using System;

/// <summary>
/// Ownership details bounding structural domain responsibility and internal team contact routing.
/// </summary>
public readonly record struct FeatureFlagOwnership(
    string? Owner,
    string? ContactInfo);