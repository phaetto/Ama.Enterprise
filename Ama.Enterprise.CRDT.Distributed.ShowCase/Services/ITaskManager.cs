namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

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
    /// Retrieves the read-only dictionary of active tasks for the specified document.
    /// </summary>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    ValueTask<IReadOnlyDictionary<string, TaskItem>> GetTasksAsync(string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds or updates a task in the list.
    /// </summary>
    Task SetTaskAsync(string documentId, string id, string description, bool isDone, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a task from the list.
    /// </summary>
    Task RemoveTaskAsync(string documentId, string id, CancellationToken cancellationToken = default);
}