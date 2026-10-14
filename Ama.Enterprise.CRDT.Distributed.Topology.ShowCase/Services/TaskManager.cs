namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Services;

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models.Intents;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Models;

/// <summary>
/// Implementation handling intentions and queries for the task list document decoupled from standard rigid events.
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
        var crdtDoc = await orchestrator.GetDocumentStateAsync<TaskListState>(documentId, cancellationToken).ConfigureAwait(false);
        if (crdtDoc.Data != null && crdtDoc.Data.Tasks != null) 
        {
            return new ReadOnlyDictionary<string, TaskItem>(crdtDoc.Data.Tasks);
        }
        
        return new ReadOnlyDictionary<string, TaskItem>(new Dictionary<string, TaskItem>());
    }

    /// <inheritdoc />
    public async Task SetTaskAsync(string documentId, string id, string description, bool isDone, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var intent = new MapSetIntent(id, new TaskItem(id, description, isDone));
        await orchestrator.ForwardIntentAsync<TaskListState>(documentId, intent, "Tasks", cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RemoveTaskAsync(string documentId, string id, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var intent = new MapRemoveIntent(id);
        await orchestrator.ForwardIntentAsync<TaskListState>(documentId, intent, "Tasks", cancellationToken).ConfigureAwait(false);
    }
}