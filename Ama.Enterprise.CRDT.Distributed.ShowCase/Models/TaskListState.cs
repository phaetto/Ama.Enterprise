namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using Ama.CRDT.Attributes.Strategies;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Root CRDT document model representing a task list.
/// </summary>
public sealed class TaskListState : IEquatable<TaskListState>, IDistributedCrdtState
{
    /// <inheritdoc />
    public string Id { get; init; } = Constants.TaskListDocumentId;

    /// <summary>
    /// Gets or sets the tasks mapped by their identifier.
    /// </summary>
    [CrdtOrMapStrategy]
    public IDictionary<string, TaskItem> Tasks { get; set; } = new Dictionary<string, TaskItem>();

    /// <inheritdoc />
    public bool Equals(TaskListState? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        if (Id != other.Id) return false;
        if (Tasks.Count != other.Tasks.Count) return false;

        foreach (var kvp in Tasks)
        {
            if (!other.Tasks.TryGetValue(kvp.Key, out var otherVal)) return false;
            if (!kvp.Value.Equals(otherVal)) return false;
        }

        return true;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as TaskListState);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Id);
        
        foreach (var kvp in Tasks.OrderBy(k => k.Key))
        {
            hash.Add(kvp.Key);
            hash.Add(kvp.Value);
        }
        
        return hash.ToHashCode();
    }
}