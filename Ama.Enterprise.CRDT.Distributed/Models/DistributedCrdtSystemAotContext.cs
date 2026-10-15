namespace Ama.Enterprise.CRDT.Distributed.Models;

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json;
using Ama.CRDT.Attributes;
using Ama.CRDT.Models.Aot;

/// <summary>
/// AOT contextual reflection mapping for internal orchestrator registry CRDT scopes.
/// </summary>
[CrdtAotType(typeof(CrdtRegistryState))]
[CrdtAotType(typeof(CrdtRegistryEntry))]
[CrdtAotType(typeof(ConcurrentDictionary<string, CrdtRegistryEntry>))]
[CrdtAotType(typeof(Dictionary<string, CrdtRegistryEntry>))]
[CrdtAotType(typeof(IDictionary<string, JsonElement>))]
[CrdtAotType(typeof(IList<ReadOnlyMemory<byte>>))]
public sealed partial class DistributedCrdtSystemAotContext : CrdtAotContext
{
}