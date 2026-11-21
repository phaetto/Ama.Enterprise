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
public sealed class FleetManager :
    IFleetManager,
    ICrdtClusterQuery<FleetState, FleetBatterySearchQuery, FleetBatterySearchResultDto>
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

    /// <inheritdoc />
    public Task<CrdtMapReduceResultDto<FleetBatterySearchResultDto>> SearchLowBatteryDevicesAsync(
        int maxBatteryThreshold,
        bool onlineOnly,
        CancellationToken cancellationToken = default)
    {
        var query = new FleetBatterySearchQuery(maxBatteryThreshold, onlineOnly);
        return orchestrator.QueryClusterAsync<FleetState, FleetBatterySearchQuery, FleetBatterySearchResultDto>(
            query,
            default,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<CrdtMapReduceResultDto<FleetBatteryHealthReportDto>> GetClusterBatteryStatsAsync(
        bool onlineOnly,
        CancellationToken cancellationToken = default)
    {
        var input = new FleetBatteryStatsInput(onlineOnly);
        return orchestrator.ExecuteMapReduceAsync<FleetState, FleetBatteryStatsInput, FleetBatteryStatsAccumulatorDto, FleetBatteryHealthReportDto>(
            input,
            default,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<CrdtMapReduceResultDto<PurgeOfflineDevicesSummaryDto>> PurgeOfflineDevicesAsync(
        int maxBatteryThreshold,
        CancellationToken cancellationToken = default)
    {
        var command = new PurgeOfflineDevicesCommand(maxBatteryThreshold);
        var scope = new CrdtMapReduceScopeDto(RoutingMode: CrdtMapReduceRoutingMode.PrimaryReplicaOnly);
        return orchestrator.QueryClusterAsync<FleetState, PurgeOfflineDevicesCommand, PurgeOfflineDevicesSummaryDto>(
            command,
            scope,
            cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<FleetBatterySearchResultDto> MapDocumentAsync(
        CrdtMapContext<FleetState> context,
        FleetBatterySearchQuery query,
        CancellationToken cancellationToken = default)
    {
        var matches = new List<LowBatteryDeviceMatchDto>();
        var devices = context.Document.Data?.Devices;

        if (devices is not null)
        {
            foreach (var kvp in devices)
            {
                var device = kvp.Value;
                if (device.BatteryLevel <= query.MaxBatteryThreshold &&
                    (!query.OnlineOnly || device.IsOnline))
                {
                    matches.Add(new LowBatteryDeviceMatchDto(
                        context.DocumentId,
                        device.DeviceId,
                        device.BatteryLevel,
                        device.IsOnline));
                }
            }
        }

        return new ValueTask<FleetBatterySearchResultDto>(new FleetBatterySearchResultDto(matches));
    }

    /// <inheritdoc />
    public FleetBatterySearchResultDto Combine(
        FleetBatterySearchQuery query,
        IReadOnlyList<FleetBatterySearchResultDto> accumulators)
    {
        ArgumentNullException.ThrowIfNull(accumulators);

        if (accumulators.Count == 0)
        {
            return new FleetBatterySearchResultDto(Array.Empty<LowBatteryDeviceMatchDto>());
        }

        if (accumulators.Count == 1)
        {
            return accumulators[0];
        }

        var combined = new List<LowBatteryDeviceMatchDto>();
        for (var i = 0; i < accumulators.Count; i++)
        {
            var list = accumulators[i].Matches;
            if (list is not null)
            {
                for (var j = 0; j < list.Count; j++)
                {
                    combined.Add(list[j]);
                }
            }
        }

        return new FleetBatterySearchResultDto(combined);
    }
}