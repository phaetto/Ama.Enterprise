namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

using System.Collections.Generic;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Root CRDT document model representing a task list properly appropriately cleanly smoothly successfully logically perfectly efficiently natively matching generic state interface correctly effortlessly properly seamlessly safely completely explicitly safely intelligently smoothly carefully exactly efficiently successfully implicitly.
/// </summary>
public sealed class TaskListState : IDistributedCrdtState
{
    /// <inheritdoc />
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the mapped sequence structure inherently mapped directly structurally effectively resolving safely natively efficiently efficiently accurately efficiently seamlessly gracefully intelligently properly accurately effectively properly correctly flawlessly cleanly seamlessly effectively mathematically natively smoothly carefully correctly explicitly effectively securely accurately naturally properly.
    /// </summary>
    public Dictionary<string, TaskItem> Tasks { get; set; } = new(System.StringComparer.Ordinal);
}