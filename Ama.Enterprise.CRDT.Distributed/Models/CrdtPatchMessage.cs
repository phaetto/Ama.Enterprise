namespace Ama.Enterprise.CRDT.Distributed.Models;

using Ama.CRDT.Models;

/// <summary>
/// Message containing a broadcasted CRDT patch.
/// </summary>
public readonly record struct CrdtPatchMessage(string? ReplicaId, CrdtPatch Patch);