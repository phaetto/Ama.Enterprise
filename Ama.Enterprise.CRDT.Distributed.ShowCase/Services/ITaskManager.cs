namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

/// <summary>
/// Interface for managing the distributed task list CRDT document.
/// </summary>
public interface ITaskManager
{
    /// <summary>
    /// Event triggered whenever the task list document state has structurally changed.
    /// </summary>
    event EventHandler? StateChanged;

    /// <summary>
    /// Gets the current read-only dictionary of distributed task items.
    /// </summary>
    IReadOnlyDictionary<string, TaskItem> GetTasks();

    /// <summary>
    /// Sets or updates a task item in the distributed document.
    /// </summary>
    Task SetTaskAsync(string id, string description, bool isDone, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a task item from the distributed document.
    /// </summary>
    Task RemoveTaskAsync(string id, CancellationToken cancellationToken = default);
}