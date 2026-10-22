namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models.Intents;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

/// <summary>
/// Implementation handling intentions and queries for the task list document routing via the orchestrator gateway.
/// </summary>
public sealed class TaskManager : ITaskManager
{
    private readonly ICrdtDocumentOrchestrator orchestrator;

    public TaskManager(ICrdtDocumentOrchestrator orchestrator)
    {
        this.orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, TaskItem>> GetTasksAsync(string documentId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));

        if (!await orchestrator.DocumentExistsAsync<TaskListState>(documentId, cancellationToken).ConfigureAwait(false))
        {
            return new ReadOnlyDictionary<string, TaskItem>(new Dictionary<string, TaskItem>());
        }

        var crdtDoc = await orchestrator.GetDocumentStateAsync<TaskListState>(documentId, cancellationToken).ConfigureAwait(false);
        if (crdtDoc.Data != null && crdtDoc.Data.Tasks != null)
        {
            return new ReadOnlyDictionary<string, TaskItem>(crdtDoc.Data.Tasks);
        }
        
        return new ReadOnlyDictionary<string, TaskItem>(new Dictionary<string, TaskItem>());
    }

    /// <inheritdoc />
    public Task SetTaskAsync(string documentId, string id, string description, bool isDone, CancellationToken cancellationToken = default)
    {
        return SetTaskAsync(documentId, id, description, isDone, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task SetTaskAsync(string documentId, string id, string description, bool isDone, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var intent = new MapSetIntent(id, new TaskItem(id, description, isDone));
        await orchestrator.ApplyIntentAsync<TaskListState>(documentId, intent, "Tasks", deliveryMode, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task RemoveTaskAsync(string documentId, string id, CancellationToken cancellationToken = default)
    {
        return RemoveTaskAsync(documentId, id, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task RemoveTaskAsync(string documentId, string id, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var intent = new MapRemoveIntent(id);
        await orchestrator.ApplyIntentAsync<TaskListState>(documentId, intent, "Tasks", deliveryMode, cancellationToken).ConfigureAwait(false);
    }
}