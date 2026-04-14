namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

using System.Collections.Generic;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Root CRDT document model representing a task list.
/// </summary>
public sealed class TaskListState : IDistributedCrdtState
{
    /// <inheritdoc />
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the mapped sequence tracking tasks.
    /// </summary>
    public Dictionary<string, TaskItem> Tasks { get; set; } = new(System.StringComparer.Ordinal);
}