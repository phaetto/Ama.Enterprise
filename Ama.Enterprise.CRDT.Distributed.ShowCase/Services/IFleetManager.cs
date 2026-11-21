namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

/// <summary>
/// Interface for managing the distributed fleet status CRDT document.
/// </summary>
/// <example>
/// <code>
/// await fleetManager.SetDeviceAsync("fleet-1", "dev-01", true, 95, IntentDeliveryMode.Acknowledged, cancellationToken);
/// var devices = await fleetManager.GetDevicesAsync("fleet-1", cancellationToken);
/// </code>
/// </example>
public interface IFleetManager
{
    /// <summary>
    /// Retrieves the read-only dictionary of active devices for the specified document.
    /// </summary>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A read-only dictionary of device statuses keyed by device ID.</returns>
    ValueTask<IReadOnlyDictionary<string, DeviceStatus>> GetDevicesAsync(string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds or updates a device in the fleet state using the default acknowledged delivery mode.
    /// </summary>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="id">The unique device ID.</param>
    /// <param name="isOnline">Indicates whether the device is online.</param>
    /// <param name="batteryLevel">The battery level percentage of the device.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SetDeviceAsync(string documentId, string id, bool isOnline, int batteryLevel, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds or updates a device in the fleet state with the specified delivery mode.
    /// </summary>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="id">The unique device ID.</param>
    /// <param name="isOnline">Indicates whether the device is online.</param>
    /// <param name="batteryLevel">The battery level percentage of the device.</param>
    /// <param name="deliveryMode">The delivery mode for the operation intent.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SetDeviceAsync(string documentId, string id, bool isOnline, int batteryLevel, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a device from the fleet state using the default acknowledged delivery mode.
    /// </summary>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="id">The unique device ID to remove.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RemoveDeviceAsync(string documentId, string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a device from the fleet state with the specified delivery mode.
    /// </summary>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="id">The unique device ID to remove.</param>
    /// <param name="deliveryMode">The delivery mode for the operation intent.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RemoveDeviceAsync(string documentId, string id, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a cluster-wide search across all sharded fleet documents for devices with a battery level at or below the threshold.
    /// </summary>
    /// <param name="maxBatteryThreshold">The maximum battery percentage to match.</param>
    /// <param name="onlineOnly">Whether to only match devices that are currently online.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The search results along with distributed execution diagnostics.</returns>
    Task<CrdtMapReduceResultDto<FleetBatterySearchResultDto>> SearchLowBatteryDevicesAsync(
        int maxBatteryThreshold,
        bool onlineOnly,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a multi-stage statistical Map/Reduce job across all fleet shards to compute global battery health metrics.
    /// </summary>
    /// <param name="onlineOnly">Whether to only include online devices in the statistical calculation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The global battery health report along with distributed execution diagnostics.</returns>
    Task<CrdtMapReduceResultDto<FleetBatteryHealthReportDto>> GetClusterBatteryStatsAsync(
        bool onlineOnly,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes an in-place distributed bulk purge across all primary fleet shards, removing offline devices at or below the battery threshold.
    /// </summary>
    /// <param name="maxBatteryThreshold">The maximum battery percentage for offline devices to be purged.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The purge summary along with distributed execution diagnostics.</returns>
    Task<CrdtMapReduceResultDto<PurgeOfflineDevicesSummaryDto>> PurgeOfflineDevicesAsync(
        int maxBatteryThreshold,
        CancellationToken cancellationToken = default);
}