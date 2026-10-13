namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Collections.Generic;

/// <summary>
/// Defines the contract for dynamically mapping shards to specific network replicas utilizing consistent hashing.
/// </summary>
public interface IShardDistributionManager
{
    /// <summary>
    /// Determines whether the specified shard should be hosted by the local replica dynamically.
    /// </summary>
    /// <param name="shardId">The shard identifier to evaluate against the distribution ring.</param>
    /// <returns><c>true</c> if the local node is responsible for the shard natively; otherwise, <c>false</c>.</returns>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var isLocal = distributionManager.IsShardLocal("shard-tenant-123");
    /// if (isLocal)
    /// {
    ///     // Process the shard initialization or operations locally explicitly
    /// }
    /// ]]>
    /// </code>
    /// </example>
    bool IsShardLocal(string shardId);

    /// <summary>
    /// Retrieves the target replica identifiers responsible for hosting the specified shard bounded naturally.
    /// </summary>
    /// <param name="shardId">The shard identifier to evaluate against the distribution ring.</param>
    /// <returns>A read-only list of target replica identifiers responsible for the shard bounds.</returns>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var replicas = distributionManager.GetTargetReplicas("shard-tenant-123");
    /// foreach(var targetId in replicas)
    /// {
    ///     // Forward metrics, payload or direct messaging towards active mapped replica safely
    /// }
    /// ]]>
    /// </code>
    /// </example>
    IReadOnlyList<string> GetTargetReplicas(string shardId);
}