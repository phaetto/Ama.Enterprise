namespace Ama.Enterprise.FeatureFlags.Models.P2p;

using Ama.CRDT.Models;

/// <summary>
/// Message containing the local state vector to synchronize with remote peers.
/// </summary>
public readonly record struct FeatureFlagStateSyncMessage(string? ReplicaId, DottedVersionVector? State);