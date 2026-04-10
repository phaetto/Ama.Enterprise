namespace Ama.Enterprise.CRDT.Distributed.Models;

using Ama.CRDT.Models;

/// <summary>
/// Message containing the missing CRDT operations targeted for a remote peer.
/// </summary>
public readonly record struct CrdtOperationsMessage(string? ReplicaId, CrdtOperation[]? Operations);