namespace Ama.Enterprise.CRDT.Distributed.Models;

using System.Collections.Generic;
using System.Text.Json.Serialization;
using Ama.CRDT.Models;

/// <summary>
/// JSON serialization context guaranteeing AOT compatibility for internal orchestrator registry CRDT scopes.
/// </summary>
[JsonSerializable(typeof(CrdtRegistryEntry))]
[JsonSerializable(typeof(CrdtRegistryState))]
[JsonSerializable(typeof(Dictionary<string, CrdtRegistryEntry>))]
[JsonSerializable(typeof(CrdtDocument<CrdtRegistryState>))]
[JsonSerializable(typeof(ClusterStateSnapshotDto))]
[JsonSerializable(typeof(ClusterPeerStateDto))]
public sealed partial class DistributedCrdtSystemJsonContext : JsonSerializerContext
{
}