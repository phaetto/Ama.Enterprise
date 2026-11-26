namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

using Ama.CRDT.Attributes.Strategies;
using Ama.CRDT.Models.Collections;
using System.Collections.Generic;

/// <summary>
/// Root CRDT document model representing a task list.
/// </summary>
public sealed class TaskListState
{
    /// <summary>
    /// Gets or sets the explicit string identifier targeting distinct task lists correctly.
    /// </summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the mapped sequence tracking tasks.
    /// </summary>
    [CrdtOrMapStrategy]
    public IDictionary<string, TaskItem> Tasks { get; set; } = new CompactDictionary<string, TaskItem>(StringComparer.Ordinal);
}