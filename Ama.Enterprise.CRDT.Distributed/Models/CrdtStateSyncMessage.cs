namespace Ama.Enterprise.CRDT.Distributed.Models;

using Ama.CRDT.Models;

/// <summary>
/// Message containing the full causal state (DottedVersionVector) of a replica across all active shards, used for Anti-Entropy exchanges.
/// </summary>
public readonly record struct CrdtStateSyncMessage(
    string ReplicaId,
    ShardVersionVectorMap State,
    bool IsTopologyAnnouncementOnly = false,
    bool IsPartialShardSelection = false);