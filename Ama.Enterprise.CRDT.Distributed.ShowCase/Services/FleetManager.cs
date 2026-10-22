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
/// Implementation handling intentions and queries for the Fleet document routing via the orchestrator gateway.
/// </summary>
public sealed class FleetManager : IFleetManager
{
    private readonly ICrdtDocumentOrchestrator orchestrator;

    public FleetManager(ICrdtDocumentOrchestrator orchestrator)
    {
        this.orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, DeviceStatus>> GetDevicesAsync(string documentId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));

        if (!await orchestrator.DocumentExistsAsync<FleetState>(documentId, cancellationToken).ConfigureAwait(false))
        {
            return new ReadOnlyDictionary<string, DeviceStatus>(new Dictionary<string, DeviceStatus>());
        }

        var crdtDoc = await orchestrator.GetDocumentStateAsync<FleetState>(documentId, cancellationToken).ConfigureAwait(false);
        if (crdtDoc.Data != null && crdtDoc.Data.Devices != null)
        {
            return new ReadOnlyDictionary<string, DeviceStatus>(crdtDoc.Data.Devices);
        }
        
        return new ReadOnlyDictionary<string, DeviceStatus>(new Dictionary<string, DeviceStatus>());
    }

    /// <inheritdoc />
    public Task SetDeviceAsync(string documentId, string id, bool isOnline, int batteryLevel, CancellationToken cancellationToken = default)
    {
        return SetDeviceAsync(documentId, id, isOnline, batteryLevel, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task SetDeviceAsync(string documentId, string id, bool isOnline, int batteryLevel, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var intent = new MapSetIntent(id, new DeviceStatus(id, isOnline, batteryLevel));
        await orchestrator.ApplyIntentAsync<FleetState>(documentId, intent, "Devices", deliveryMode, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task RemoveDeviceAsync(string documentId, string id, CancellationToken cancellationToken = default)
    {
        return RemoveDeviceAsync(documentId, id, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task RemoveDeviceAsync(string documentId, string id, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document Id cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var intent = new MapRemoveIntent(id);
        await orchestrator.ApplyIntentAsync<FleetState>(documentId, intent, "Devices", deliveryMode, cancellationToken).ConfigureAwait(false);
    }
}