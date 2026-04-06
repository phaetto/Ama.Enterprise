namespace Ama.Enterprise.FeatureFlags.Models.P2p;

using Ama.CRDT.Models;

/// <summary>
/// Message containing the missing CRDT operations targeted for a remote peer.
/// </summary>
public readonly record struct FeatureFlagOperationsMessage(string? ReplicaId, CrdtOperation[]? Operations);