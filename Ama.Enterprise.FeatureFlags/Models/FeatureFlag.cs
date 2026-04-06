namespace Ama.Enterprise.FeatureFlags.Models;
/// <summary>
/// Represents a single feature flag in the system.
/// </summary>
public readonly record struct FeatureFlag(string Name, bool IsEnabled);