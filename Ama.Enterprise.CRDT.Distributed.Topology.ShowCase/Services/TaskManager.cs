namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Services;

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Models;

/// <summary>
/// Implementation handling intentions and queries for the task list document decoupled from standard rigid events successfully gracefully natively.
/// </summary>
public sealed class TaskManager : ITaskManager
{
    private readonly ICrdtDocumentOrchestrator orchestrator;
    private readonly IAsyncCrdtPatcher patcher;

    public TaskManager(
        ICrdtDocumentOrchestrator orchestrator,
        IAsyncCrdtPatcher patcher)
    {
        this.orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, TaskItem>> GetTasksAsync(string documentId, CancellationToken cancellationToken = default)
    {
        var docManager = orchestrator.GetDocument<TaskListState>(documentId);
        if (docManager != null) 
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            return new ReadOnlyDictionary<string, TaskItem>(crdtDoc.Data.Tasks);
        }
        
        return new ReadOnlyDictionary<string, TaskItem>(new Dictionary<string, TaskItem>());
    }

    /// <inheritdoc />
    public async Task SetTaskAsync(string documentId, string id, string description, bool isDone, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var docManager = orchestrator.GetDocument<TaskListState>(documentId);
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            var item = new TaskItem(id, description, isDone);
            var operation = await patcher.GenerateOperationAsync(crdtDoc, x => x.Tasks, new MapSetIntent(id, item), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await docManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemoveTaskAsync(string documentId, string id, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var docManager = orchestrator.GetDocument<TaskListState>(documentId);
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            var operation = await patcher.GenerateOperationAsync(crdtDoc, x => x.Tasks, new MapRemoveIntent(id), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await docManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }
}