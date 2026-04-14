namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Text.Json.Serialization;

/// <summary>
/// Message broadcasted to reject and re-bootstrap nodes that have been tombstoned by the cluster.
/// </summary>
public readonly record struct CrdtEvictionRejectionMessage : IEquatable<CrdtEvictionRejectionMessage>
{
    /// <summary>
    /// Gets the globally unique identifier of the replica that has been explicitly tombstoned.
    /// </summary>
    public string EvictedReplicaId { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="CrdtEvictionRejectionMessage"/> struct.
    /// </summary>
    /// <param name="evictedReplicaId">The identifier of the evicted replica.</param>
    [JsonConstructor]
    public CrdtEvictionRejectionMessage(string evictedReplicaId)
    {
        EvictedReplicaId = evictedReplicaId ?? throw new ArgumentNullException(nameof(evictedReplicaId));
    }

    /// <inheritdoc />
    public bool Equals(CrdtEvictionRejectionMessage other)
    {
        return EvictedReplicaId == other.EvictedReplicaId;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return EvictedReplicaId.GetHashCode();
    }
}