namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Specifies how the orchestrator selects target replicas for each shard during a distributed Map/Reduce job.
/// </summary>
public enum CrdtMapReduceRoutingMode
{
    /// <summary>
    /// Evaluates each shard on one active replica, preferring the local replica if it hosts the shard,
    /// and batching remote shards onto the minimum number of remote peers.
    /// </summary>
    MinimalShardCover = 0,

    /// <summary>
    /// Evaluates each shard on its primary owning replica on the consistent hash ring,
    /// falling back to secondary replicas only if the primary is unreachable.
    /// </summary>
    PrimaryReplicaOnly = 1,

    /// <summary>
    /// Evaluates each shard across all active replicas currently hosting a copy of the shard.
    /// </summary>
    AllReplicas = 2
}