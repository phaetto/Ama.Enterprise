namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

using System.Text.Json.Serialization;

/// <summary>
/// AOT JSON serialization context for the showcase multi-CRDT models.
/// </summary>
[JsonSerializable(typeof(TaskListState))]
[JsonSerializable(typeof(TaskItem))]
[JsonSerializable(typeof(FleetState))]
[JsonSerializable(typeof(DeviceStatus))]
public sealed partial class ShowCaseJsonContext : JsonSerializerContext
{
}