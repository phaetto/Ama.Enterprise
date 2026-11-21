namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

using System.Text.Json.Serialization;
using Ama.CRDT.Models;

/// <summary>
/// AOT JSON serialization context for the showcase multi-CRDT models.
/// </summary>
[JsonSerializable(typeof(TaskListState))]
[JsonSerializable(typeof(TaskItem))]
[JsonSerializable(typeof(FleetState))]
[JsonSerializable(typeof(DeviceStatus))]
[JsonSerializable(typeof(CrdtDocument<TaskListState>))]
[JsonSerializable(typeof(CrdtDocument<FleetState>))]
[JsonSerializable(typeof(FleetBatterySearchQuery))]
[JsonSerializable(typeof(LowBatteryDeviceMatchDto))]
[JsonSerializable(typeof(FleetBatterySearchResultDto))]
[JsonSerializable(typeof(FleetBatteryStatsInput))]
[JsonSerializable(typeof(FleetBatteryStatsAccumulatorDto))]
[JsonSerializable(typeof(PurgeOfflineDevicesCommand))]
[JsonSerializable(typeof(PurgeOfflineDevicesSummaryDto))]
public sealed partial class ShowCaseJsonContext : JsonSerializerContext
{
}