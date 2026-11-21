namespace Ama.Enterprise.CRDT.Distributed.Models;

using System.Collections.Generic;

/// <summary>
/// Defines optional targeting filters and routing behavior for a distributed Map/Reduce or Cluster Query job.
/// </summary>
/// <param name="TargetShardIds">Optional list of shard identifiers to restrict the operation to.</param>
/// <param name="TargetDocumentIds">Optional list of document identifiers to restrict the operation to.</param>
/// <param name="RoutingMode">The replica routing mode to use for shard selection.</param>
/// <param name="AllowPartialResultsOnPeerFailure">Indicates whether to return partial results when all replicas for a shard are unreachable.</param>
public readonly record struct CrdtMapReduceScopeDto(
    IReadOnlyList<string>? TargetShardIds = null,
    IReadOnlyList<string>? TargetDocumentIds = null,
    CrdtMapReduceRoutingMode RoutingMode = CrdtMapReduceRoutingMode.MinimalShardCover,
    bool AllowPartialResultsOnPeerFailure = false);