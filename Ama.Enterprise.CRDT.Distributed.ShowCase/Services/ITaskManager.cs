namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

/// <summary>
/// Interface for managing the distributed task list CRDT document.
/// </summary>
/// <example>
/// <code>
/// await taskManager.SetTaskAsync("tasks-1", "task-01", "Deploy update", false, IntentDeliveryMode.Acknowledged, cancellationToken);
/// var tasks = await taskManager.GetTasksAsync("tasks-1", cancellationToken);
/// </code>
/// </example>
public interface ITaskManager
{
    /// <summary>
    /// Retrieves the read-only dictionary of active tasks for the specified document.
    /// </summary>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A read-only dictionary of task items keyed by task ID.</returns>
    ValueTask<IReadOnlyDictionary<string, TaskItem>> GetTasksAsync(string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds or updates a task in the list using the default acknowledged delivery mode.
    /// </summary>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="id">The unique task ID.</param>
    /// <param name="description">The task description.</param>
    /// <param name="isDone">Indicates whether the task is completed.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SetTaskAsync(string documentId, string id, string description, bool isDone, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds or updates a task in the list with the specified delivery mode.
    /// </summary>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="id">The unique task ID.</param>
    /// <param name="description">The task description.</param>
    /// <param name="isDone">Indicates whether the task is completed.</param>
    /// <param name="deliveryMode">The delivery mode for the operation intent.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SetTaskAsync(string documentId, string id, string description, bool isDone, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a task from the list using the default acknowledged delivery mode.
    /// </summary>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="id">The unique task ID to remove.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RemoveTaskAsync(string documentId, string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a task from the list with the specified delivery mode.
    /// </summary>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="id">The unique task ID to remove.</param>
    /// <param name="deliveryMode">The delivery mode for the operation intent.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RemoveTaskAsync(string documentId, string id, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default);
}