namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Defines the contract for dynamically mapping shards to specific network replicas utilizing consistent hashing.
/// </summary>
public interface IShardDistributionManager
{
    /// <summary>
    /// Determines whether the specified shard should be hosted by the local replica dynamically evaluating live network peers.
    /// </summary>
    /// <param name="shardId">The shard identifier to evaluate against the distribution ring.</param>
    /// <param name="cancellationToken">Token to monitor for cancellation requests.</param>
    /// <returns><c>true</c> if the local node is responsible for the shard natively; otherwise, <c>false</c>.</returns>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var isLocal = await distributionManager.IsShardLocalAsync("shard-tenant-123", cancellationToken);
    /// if (isLocal)
    /// {
    ///     // Process the shard initialization or operations locally explicitly
    /// }
    /// ]]>
    /// </code>
    /// </example>
    ValueTask<bool> IsShardLocalAsync(string shardId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the target replica identifiers responsible for hosting the specified shard bounded naturally evaluating active peering bounds.
    /// </summary>
    /// <param name="shardId">The shard identifier to evaluate against the distribution ring.</param>
    /// <param name="cancellationToken">Token to monitor for cancellation requests.</param>
    /// <returns>A read-only list of target replica identifiers responsible for the shard bounds.</returns>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var replicas = await distributionManager.GetTargetNodesForShardAsync("shard-tenant-123", cancellationToken);
    /// foreach(var targetId in replicas)
    /// {
    ///     // Forward metrics, payload or direct messaging towards active mapped replica safely
    /// }
    /// ]]>
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetTargetNodesForShardAsync(string shardId, CancellationToken cancellationToken = default);
}