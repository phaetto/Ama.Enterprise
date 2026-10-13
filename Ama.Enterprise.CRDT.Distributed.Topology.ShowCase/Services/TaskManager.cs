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
    private readonly object syncRoot = new();

    public TaskManager(
        ICrdtDocumentOrchestrator orchestrator,
        IAsyncCrdtPatcher patcher)
    {
        this.orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, TaskItem> GetTasks(string documentId)
    {
        lock (syncRoot)
        {
            var doc = orchestrator.GetDocument<TaskListState>(documentId);
            return doc != null 
                ? new ReadOnlyDictionary<string, TaskItem>(doc.Document.Data.Tasks) 
                : new ReadOnlyDictionary<string, TaskItem>(new Dictionary<string, TaskItem>());
        }
    }

    /// <inheritdoc />
    public async Task SetTaskAsync(string documentId, string id, string description, bool isDone, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var doc = orchestrator.GetDocument<TaskListState>(documentId);
        if (doc != null)
        {
            var item = new TaskItem(id, description, isDone);
            var operation = await patcher.GenerateOperationAsync(doc.Document, x => x.Tasks, new MapSetIntent(id, item), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await doc.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemoveTaskAsync(string documentId, string id, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var doc = orchestrator.GetDocument<TaskListState>(documentId);
        if (doc != null)
        {
            var operation = await patcher.GenerateOperationAsync(doc.Document, x => x.Tasks, new MapRemoveIntent(id), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await doc.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }
}