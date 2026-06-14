namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Message broadcasted to reject and re-bootstrap nodes that have been tombstoned by the cluster.
/// </summary>
public sealed record CrdtEvictionRejectionMessage : IEquatable<CrdtEvictionRejectionMessage>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets the globally unique identifier of the replica that has been explicitly tombstoned.
    /// </summary>
    public string EvictedReplicaId { get; }

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="CrdtEvictionRejectionMessage"/> class.
    /// </summary>
    /// <param name="evictedReplicaId">The identifier of the evicted replica.</param>
    [JsonConstructor]
    public CrdtEvictionRejectionMessage(string evictedReplicaId)
    {
        EvictedReplicaId = evictedReplicaId ?? throw new ArgumentNullException(nameof(evictedReplicaId));
    }

    /// <inheritdoc />
    public bool Equals(CrdtEvictionRejectionMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return EvictedReplicaId == other.EvictedReplicaId;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return EvictedReplicaId.GetHashCode();
    }
}