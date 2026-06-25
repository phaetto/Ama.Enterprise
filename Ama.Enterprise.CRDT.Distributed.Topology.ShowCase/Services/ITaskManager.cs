namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Models;

/// <summary>
/// Interface for managing the distributed task list CRDT document.
/// </summary>
public interface ITaskManager
{
    /// <summary>
    /// Event triggered when the task list state changes.
    /// </summary>
    event EventHandler? StateChanged;

    /// <summary>
    /// Retrieves the read-only dictionary of active tasks for the specified document.
    /// </summary>
    IReadOnlyDictionary<string, TaskItem> GetTasks(string documentId);

    /// <summary>
    /// Adds or updates a task in the list.
    /// </summary>
    Task SetTaskAsync(string documentId, string id, string description, bool isDone, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a task from the list.
    /// </summary>
    Task RemoveTaskAsync(string documentId, string id, CancellationToken cancellationToken = default);
}