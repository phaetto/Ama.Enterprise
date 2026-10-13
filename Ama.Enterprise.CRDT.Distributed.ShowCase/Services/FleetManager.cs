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
/// Implementation handling intentions and queries for the Fleet document decoupled from UI triggers natively.
/// </summary>
public sealed class FleetManager : IFleetManager
{
    private readonly ICrdtDocumentOrchestrator orchestrator;
    private readonly IAsyncCrdtPatcher patcher;

    public FleetManager(
        ICrdtDocumentOrchestrator orchestrator,
        IAsyncCrdtPatcher patcher)
    {
        this.orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, DeviceStatus>> GetDevicesAsync(string documentId, CancellationToken cancellationToken = default)
    {
        var docManager = orchestrator.GetDocument<FleetState>(documentId);
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            return new ReadOnlyDictionary<string, DeviceStatus>(crdtDoc.Data.Devices);
        }
        
        return new ReadOnlyDictionary<string, DeviceStatus>(new Dictionary<string, DeviceStatus>());
    }

    /// <inheritdoc />
    public async Task SetDeviceAsync(string documentId, string id, bool isOnline, int batteryLevel, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var docManager = orchestrator.GetDocument<FleetState>(documentId);
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            var item = new DeviceStatus(id, isOnline, batteryLevel);
            var operation = await patcher.GenerateOperationAsync(crdtDoc, x => x.Devices, new MapSetIntent(id, item), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await docManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemoveDeviceAsync(string documentId, string id, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var docManager = orchestrator.GetDocument<FleetState>(documentId);
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            var operation = await patcher.GenerateOperationAsync(crdtDoc, x => x.Devices, new MapRemoveIntent(id), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await docManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }
}