namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Configuration payload to natively map and pre-initialize isolated local CRDT replicas on startup natively avoiding explicit generic boundaries.
/// </summary>
public sealed record DistributedCrdtReplicaRegistration(string ReplicaId);