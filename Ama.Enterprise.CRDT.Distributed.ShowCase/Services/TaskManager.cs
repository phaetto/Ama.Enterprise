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
public sealed class TaskManager : ITaskManager
{
    private readonly IDistributedCrdtDocument<TaskListState> documentManager;
    private readonly ICrdtPatcher patcher;
    private readonly object syncRoot = new();

    /// <inheritdoc />
    public event EventHandler? StateChanged;

    public TaskManager(
        IDistributedCrdtDocument<TaskListState> documentManager,
        ICrdtPatcher patcher)
    {
        this.documentManager = documentManager ?? throw new ArgumentNullException(nameof(documentManager));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));

        this.documentManager.StateChanged += (sender, args) => StateChanged?.Invoke(this, args);
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, TaskItem> GetTasks()
    {
        lock (syncRoot)
        {
            return new ReadOnlyDictionary<string, TaskItem>(documentManager.Document.Data.Tasks);
        }
    }

    /// <inheritdoc />
    public async Task SetTaskAsync(string id, string description, bool isDone, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var item = new TaskItem(id, description, isDone);
        var operation = patcher.GenerateOperation(documentManager.Document, x => x.Tasks, new MapSetIntent(id, item));
        var patch = new CrdtPatch(new[] { operation });

        await documentManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RemoveTaskAsync(string id, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var operation = patcher.GenerateOperation(documentManager.Document, x => x.Tasks, new MapRemoveIntent(id));
        var patch = new CrdtPatch(new[] { operation });

        await documentManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
    }
}