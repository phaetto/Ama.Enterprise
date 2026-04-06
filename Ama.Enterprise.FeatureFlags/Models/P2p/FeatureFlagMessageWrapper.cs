namespace Ama.Enterprise.FeatureFlags.Models.P2p;

/// <summary>
/// Envelope wrapper for feature flag messages sent over the generic P2P gossip network.
/// </summary>
public readonly record struct FeatureFlagMessageWrapper(string? MessageType, byte[]? Payload);