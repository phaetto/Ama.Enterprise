namespace Ama.Enterprise.FeatureFlags;

/// <summary>
/// Global constants for the Feature Flags module.
/// </summary>
public static class Constants
{
    /// <summary>
    /// The default document identifier for the singleton feature flags state.
    /// </summary>
    public const string GlobalDocumentId = "ama-enterprise-feature-flags-singleton";

    /// <summary>
    /// The registered document type alias for feature flags.
    /// </summary>
    public const string FeatureFlagDocumentType = "feature-flag";
}