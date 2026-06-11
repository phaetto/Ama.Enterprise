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
/// Implementation handling intentions and queries for the Fleet document.
/// </summary>
public sealed class FleetManager : IFleetManager, IDisposable
{
    private readonly ICrdtDocumentOrchestrator orchestrator;
    private readonly IAsyncCrdtPatcher patcher;
    private readonly object syncRoot = new();

    /// <inheritdoc />
    public event EventHandler? StateChanged;

    public FleetManager(
        ICrdtDocumentOrchestrator orchestrator,
        IAsyncCrdtPatcher patcher)
    {
        this.orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));

        this.orchestrator.DocumentsChanged += OnOrchestratorChanged;
        SubscribeToActiveDocuments();
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, DeviceStatus> GetDevices(string documentId)
    {
        lock (syncRoot)
        {
            var doc = orchestrator.GetDocument<FleetState>(documentId);
            return doc != null 
                ? new ReadOnlyDictionary<string, DeviceStatus>(doc.Document.Data.Devices) 
                : new ReadOnlyDictionary<string, DeviceStatus>(new Dictionary<string, DeviceStatus>());
        }
    }

    /// <inheritdoc />
    public async Task SetDeviceAsync(string documentId, string id, bool isOnline, int batteryLevel, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var doc = orchestrator.GetDocument<FleetState>(documentId);
        if (doc != null)
        {
            var item = new DeviceStatus(id, isOnline, batteryLevel);
            var operation = await patcher.GenerateOperationAsync(doc.Document, x => x.Devices, new MapSetIntent(id, item), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await doc.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemoveDeviceAsync(string documentId, string id, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var doc = orchestrator.GetDocument<FleetState>(documentId);
        if (doc != null)
        {
            var operation = await patcher.GenerateOperationAsync(doc.Document, x => x.Devices, new MapRemoveIntent(id), cancellationToken).ConfigureAwait(false);
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
            if (d is IDistributedCrdtDocument<FleetState> typedDoc)
            {
                typedDoc.StateChanged -= OnDocumentStateChanged;
            }
        }
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
                if (d is IDistributedCrdtDocument<FleetState> typedDoc)
                {
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
}