namespace Ama.Enterprise.FeatureFlags.Models;

using System;

/// <summary>
/// Represents a single feature flag in the system.
/// </summary>
public readonly record struct FeatureFlag(string Name, bool IsEnabled);