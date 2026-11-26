namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

using Ama.CRDT.Attributes;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Models.Collections;
using System.Collections.Generic;

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
[CrdtAotType(typeof(CompactDictionary<string, TaskItem>))]
[CrdtAotType(typeof(CompactDictionary<string, DeviceStatus>))]
[CrdtAotType(typeof(ICollection<TaskItem>))]
[CrdtAotType(typeof(ICollection<DeviceStatus>))]
public sealed partial class ShowCaseCrdtAotContext : CrdtAotContext
{
}