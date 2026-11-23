namespace Ama.Enterprise.CRDT.Distributed.Services;

/// <summary>
/// Factory for creating isolated CRDT replica scopes.
/// </summary>
/// <example>
/// <code>
/// var scope = scopeFactory.CreateScope("eu-user", "node-8100");
/// </code>
/// </example>
public interface IDistributedCrdtScopeFactory
{
    /// <summary>
    /// Constructs a distributed CRDT scope identified by the given replica or scope identifier.
    /// </summary>
    /// <param name="replicaId">The replica or scope identifier.</param>
    /// <returns>The created distributed CRDT scope.</returns>
    IDistributedCrdtScope CreateScope(string replicaId);

    /// <summary>
    /// Constructs a distributed CRDT scope for a logical scope identifier hosted on a physical host replica.
    /// </summary>
    /// <param name="scopeId">The logical scope identifier.</param>
    /// <param name="hostReplicaId">The physical host replica identifier bound to P2P meshes.</param>
    /// <returns>The created distributed CRDT scope.</returns>
    IDistributedCrdtScope CreateScope(string scopeId, string hostReplicaId);
}