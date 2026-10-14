namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Configuration payload to explicitly map a P2P mesh to its corresponding local CRDT replica for targeted topological evaluations natively avoiding implicit generic boundaries.
/// </summary>
public sealed record DistributedCrdtP2pRegistration(string MeshId, string ReplicaId);