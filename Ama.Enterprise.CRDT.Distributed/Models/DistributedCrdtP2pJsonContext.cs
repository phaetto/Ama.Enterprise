namespace Ama.Enterprise.CRDT.Distributed.Models;

using System.Text.Json.Serialization;
using Ama.CRDT.Models;

/// <summary>
/// JSON serialization context for the generic Distributed CRDT P2P models.
/// </summary>
[JsonSerializable(typeof(CrdtMessageWrapper))]
[JsonSerializable(typeof(CrdtStateSyncMessage))]
[JsonSerializable(typeof(CrdtOperationsMessage))]
[JsonSerializable(typeof(CrdtPatchMessage))]
[JsonSerializable(typeof(DottedVersionVector))]
[JsonSerializable(typeof(CrdtSnapshotMessage))]
[JsonSerializable(typeof(CrdtEvictionRejectionMessage))]
[JsonSerializable(typeof(CrdtReadRequestDto))]
[JsonSerializable(typeof(CrdtReadResponseDto))]
[JsonSerializable(typeof(CrdtIntentForwardRequestDto))]
[JsonSerializable(typeof(CrdtIntentForwardResponseDto))]
public sealed partial class DistributedCrdtP2pJsonContext : JsonSerializerContext
{
}