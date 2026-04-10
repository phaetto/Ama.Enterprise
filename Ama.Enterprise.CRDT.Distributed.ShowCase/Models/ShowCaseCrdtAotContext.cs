namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

using System.Collections.Generic;
using Ama.CRDT.Attributes;
using Ama.CRDT.Models.Aot;

/// <summary>
/// AOT reflection context for the Multi-CRDT Showcase models.
/// </summary>
[CrdtAotType(typeof(TaskItem))]
[CrdtAotType(typeof(TaskListState))]
[CrdtAotType(typeof(IDictionary<string, TaskItem>))]
[CrdtAotType(typeof(Dictionary<string, TaskItem>))]
[CrdtAotType(typeof(DeviceStatus))]
[CrdtAotType(typeof(FleetState))]
[CrdtAotType(typeof(IDictionary<string, DeviceStatus>))]
[CrdtAotType(typeof(Dictionary<string, DeviceStatus>))]
public sealed partial class ShowCaseCrdtAotContext : CrdtAotContext
{
}