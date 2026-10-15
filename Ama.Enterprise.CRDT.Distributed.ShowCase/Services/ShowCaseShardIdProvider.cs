namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Providers;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

/// <summary>
/// Custom shard ID provider evaluating strict multi-document boundaries.
/// Isolates causal history tracking for independent business domains like Tasks and Fleets.
/// By appending the document ID, the distributed hash ring will automatically 
/// distribute and migrate documents evenly across all active nodes in the cluster.
/// </summary>
public sealed class ShowCaseShardIdProvider : IShardIdProvider
{
    /// <inheritdoc />
    public string GetShardId<T>(CrdtDocument<T> document) where T : class
    {
        ArgumentNullException.ThrowIfNull(document);

        if (document.Data is TaskListState taskListState)
        {
            return $"task-shard-{taskListState.Id}";
        }

        if (document.Data is FleetState fleetState)
        {
            return $"fleet-shard-{fleetState.Id}";
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

        if (documentType == typeof(TaskListState))
        {
            return $"task-shard-{documentId}";
        }

        if (documentType == typeof(FleetState))
        {
            return $"fleet-shard-{documentId}";
        }

        return "default";
    }
}