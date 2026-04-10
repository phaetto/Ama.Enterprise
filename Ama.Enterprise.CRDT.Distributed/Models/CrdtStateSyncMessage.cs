namespace Ama.Enterprise.CRDT.Distributed.Models;

using Ama.CRDT.Models;

/// <summary>
/// Message containing the local state vector of a CRDT document to synchronize with remote peers.
/// </summary>
public readonly record struct CrdtStateSyncMessage(string? ReplicaId, DottedVersionVector? State);