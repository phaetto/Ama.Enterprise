namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

using System.Collections.Generic;
using System.Text.Json.Serialization;
using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Models;

/// <summary>
/// AOT JSON serialization context for the showcase multi-CRDT models.
/// </summary>
[JsonSerializable(typeof(TaskListState))]
[JsonSerializable(typeof(TaskItem))]
[JsonSerializable(typeof(FleetState))]
[JsonSerializable(typeof(DeviceStatus))]
[JsonSerializable(typeof(CrdtDocument<TaskListState>))]
[JsonSerializable(typeof(CrdtDocument<FleetState>))]
public sealed partial class ShowCaseJsonContext : JsonSerializerContext
{
}