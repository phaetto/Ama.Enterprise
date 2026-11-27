namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models.Intents;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

/// <summary>
/// Read-write distributed Map/Reduce handler that scans sharded Fleet documents in-place on their primary replicas
/// and removes offline devices below a specified battery threshold.
/// </summary>
public sealed class PurgeOfflineDevicesMapReduce
    : ICrdtClusterQuery<FleetState, PurgeOfflineDevicesCommand, PurgeOfflineDevicesSummaryDto>
{
    /// <inheritdoc />
    public async ValueTask<PurgeOfflineDevicesSummaryDto> MapDocumentAsync(
        CrdtMapContext<FleetState> context,
        PurgeOfflineDevicesCommand command,
        CancellationToken cancellationToken = default)
    {
        if (!context.CanMutateInPlace || context.LiveDocumentHandle is null)
        {
            return new PurgeOfflineDevicesSummaryDto(0, 0);
        }

        var devices = context.Data?.Devices;
        if (devices is null || devices.Count == 0)
        {
            return new PurgeOfflineDevicesSummaryDto(0, 0);
        }

        var keysToRemove = new List<string>();
        foreach (var kvp in devices)
        {
            if (!kvp.Value.IsOnline && kvp.Value.BatteryLevel <= command.MaxBatteryThreshold)
            {
                keysToRemove.Add(kvp.Key);
            }
        }

        if (keysToRemove.Count == 0)
        {
            return new PurgeOfflineDevicesSummaryDto(0, 0);
        }

        var removeTasks = new Task[keysToRemove.Count];
        for (var i = 0; i < keysToRemove.Count; i++)
        {
            var key = keysToRemove[i];
            removeTasks[i] = context.LiveDocumentHandle.ApplyIntentAsync(
                new MapRemoveIntent(key),
                nameof(FleetState.Devices),
                IntentDeliveryMode.Acknowledged,
                cancellationToken);
        }

        await Task.WhenAll(removeTasks).ConfigureAwait(false);

        return new PurgeOfflineDevicesSummaryDto(keysToRemove.Count, 1);
    }

    /// <inheritdoc />
    public PurgeOfflineDevicesSummaryDto Combine(
        PurgeOfflineDevicesCommand command,
        IReadOnlyList<PurgeOfflineDevicesSummaryDto> accumulators)
    {
        ArgumentNullException.ThrowIfNull(accumulators);

        var totalPurged = 0;
        var totalDocs = 0;
        for (var i = 0; i < accumulators.Count; i++)
        {
            totalPurged += accumulators[i].PurgedDeviceCount;
            totalDocs += accumulators[i].ModifiedDocumentCount;
        }

        return new PurgeOfflineDevicesSummaryDto(totalPurged, totalDocs);
    }
}