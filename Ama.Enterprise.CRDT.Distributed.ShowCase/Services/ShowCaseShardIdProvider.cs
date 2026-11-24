namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Providers;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

/// <summary>
/// Custom shard ID provider evaluating multi-document boundaries across a fixed number of logical shards.
/// Maps documents deterministically into 10 logical shards so the distributed hash ring
/// distributes and migrates shard buckets across active nodes in the cluster.
/// </summary>
public sealed class ShowCaseShardIdProvider : IShardIdProvider
{
    private const int LogicalShardCount = 100;

    /// <inheritdoc />
    public string GetShardId<T>(CrdtDocument<T> document) where T : class
    {
        ArgumentNullException.ThrowIfNull(document);

        if (document.Data is TaskListState taskListState)
        {
            return ComputeLogicalShardId("task", taskListState.Id);
        }

        if (document.Data is FleetState fleetState)
        {
            return ComputeLogicalShardId("fleet", fleetState.Id);
        }

        return "default";
    }

    /// <inheritdoc />
    public string GetShardId<T>(IComparable documentId) where T : class
    {
        ArgumentNullException.ThrowIfNull(documentId);
        return GetShardId(documentId, typeof(T));
    }

    /// <inheritdoc />
    public string GetShardId(IComparable documentId, Type documentType)
    {
        ArgumentNullException.ThrowIfNull(documentId);
        ArgumentNullException.ThrowIfNull(documentType);

        var idString = documentId.ToString() ?? string.Empty;

        if (documentType == typeof(TaskListState))
        {
            return ComputeLogicalShardId("task", idString);
        }

        if (documentType == typeof(FleetState))
        {
            return ComputeLogicalShardId("fleet", idString);
        }

        return "default";
    }

    private static string ComputeLogicalShardId(string domainPrefix, string documentId)
    {
        // Standard FNV-1a 32-bit hash for deterministic cross-process shard assignment without extra package dependencies.
        uint hash = 2166136261u;

        foreach (var ch in domainPrefix)
        {
            hash ^= ch;
            hash *= 16777619u;
        }

        hash ^= ':';
        hash *= 16777619u;

        foreach (var ch in documentId)
        {
            hash ^= ch;
            hash *= 16777619u;
        }

        var shardIndex = hash % LogicalShardCount;
        return $"shard-{shardIndex}";
    }
}