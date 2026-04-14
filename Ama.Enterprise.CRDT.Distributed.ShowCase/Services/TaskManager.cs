namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

/// <summary>
/// Implementation handling intentions and queries for the task list document.
/// </summary>
public sealed class TaskManager : ITaskManager, IDisposable
{
    private readonly ICrdtDocumentOrchestrator orchestrator;
    private readonly ICrdtPatcher patcher;
    private readonly object syncRoot = new();

    /// <inheritdoc />
    public event EventHandler? StateChanged;

    public TaskManager(
        ICrdtDocumentOrchestrator orchestrator,
        ICrdtPatcher patcher)
    {
        this.orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));

        this.orchestrator.DocumentsChanged += OnOrchestratorChanged;
        SubscribeToActiveDocuments();
    }

    private void OnOrchestratorChanged(object? sender, EventArgs e)
    {
        SubscribeToActiveDocuments();
        StateChanged?.Invoke(this, EventArgs.Empty);
    }

    private void SubscribeToActiveDocuments()
    {
        lock (syncRoot)
        {
            var docs = orchestrator.GetActiveDocuments();
            foreach (var d in docs)
            {
                if (d is IDistributedCrdtDocument<TaskListState> typedDoc)
                {
                    // To avoid multiple subscriptions
                    typedDoc.StateChanged -= OnDocumentStateChanged;
                    typedDoc.StateChanged += OnDocumentStateChanged;
                }
            }
        }
    }

    private void OnDocumentStateChanged(object? sender, EventArgs e)
    {
        StateChanged?.Invoke(this, EventArgs.Empty);
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
            var operation = patcher.GenerateOperation(doc.Document, x => x.Tasks, new MapSetIntent(id, item));
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
            var operation = patcher.GenerateOperation(doc.Document, x => x.Tasks, new MapRemoveIntent(id));
            var patch = new CrdtPatch(new[] { operation });

            await doc.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        orchestrator.DocumentsChanged -= OnOrchestratorChanged;
        var docs = orchestrator.GetActiveDocuments();
        foreach (var d in docs)
        {
            if (d is IDistributedCrdtDocument<TaskListState> typedDoc)
            {
                typedDoc.StateChanged -= OnDocumentStateChanged;
            }
        }
    }
}