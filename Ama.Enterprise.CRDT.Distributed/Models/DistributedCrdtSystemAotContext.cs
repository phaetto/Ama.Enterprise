namespace Ama.Enterprise.CRDT.Distributed.Models;

using System.Collections.Generic;
using Ama.CRDT.Attributes;
using Ama.CRDT.Models.Aot;

/// <summary>
/// AOT contextual reflection mapping for internal orchestrator registry CRDT scopes.
/// </summary>
[CrdtAotType(typeof(CrdtRegistryEntry))]
[CrdtAotType(typeof(CrdtRegistryState))]
[CrdtAotType(typeof(Dictionary<string, CrdtRegistryEntry>))]
[CrdtAotType(typeof(IDictionary<string, CrdtRegistryEntry>))]
public sealed partial class DistributedCrdtSystemAotContext : CrdtAotContext
{
}