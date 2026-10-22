namespace Ama.Enterprise.CRDT.Distributed.Models;

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using Ama.CRDT.Models;

/// <summary>
/// Source-generated AOT JSON serialization context for internal orchestrator and cluster state models.
/// </summary>
[JsonSerializable(typeof(CrdtRegistryState))]
[JsonSerializable(typeof(CrdtRegistryEntry))]
[JsonSerializable(typeof(ConcurrentDictionary<string, CrdtRegistryEntry>))]
[JsonSerializable(typeof(Dictionary<string, CrdtRegistryEntry>))]
[JsonSerializable(typeof(CrdtDocument<CrdtRegistryState>))]
[JsonSerializable(typeof(ClusterStateSnapshotDto))]
[JsonSerializable(typeof(ShardVersionVectorMap))]
[JsonSerializable(typeof(DottedVersionVector))]
[JsonSerializable(typeof(JournaledOperation))]
[JsonSerializable(typeof(CrdtOperation))]
public sealed partial class DistributedCrdtSystemJsonContext : JsonSerializerContext
{
}