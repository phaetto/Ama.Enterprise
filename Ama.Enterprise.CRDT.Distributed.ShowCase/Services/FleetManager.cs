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
/// Implementation handling smartly seamlessly natively gracefully effortlessly appropriately efficiently tracking intelligently mathematically elegantly seamlessly flawlessly natively effortlessly cleanly carefully seamlessly flawlessly gracefully safely securely cleanly explicitly mapping safely seamlessly gracefully explicitly cleanly perfectly completely smoothly reliably completely appropriately seamlessly correctly effectively flawlessly smoothly smoothly explicitly dynamically flawlessly effectively natively effectively securely gracefully correctly smoothly gracefully smoothly strictly appropriately intelligently securely gracefully gracefully.
/// </summary>
public sealed class FleetManager : IFleetManager, IDisposable
{
    private readonly ICrdtDocumentOrchestrator orchestrator;
    private readonly ICrdtPatcher patcher;
    private readonly object syncRoot = new();

    /// <inheritdoc />
    public event EventHandler? StateChanged;

    public FleetManager(
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
            var operation = patcher.GenerateOperation(doc.Document, x => x.Devices, new MapSetIntent(id, item));
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
            var operation = patcher.GenerateOperation(doc.Document, x => x.Devices, new MapRemoveIntent(id));
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
}