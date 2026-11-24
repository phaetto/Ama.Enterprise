namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.CRDT.Models;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Message containing the full causal state (DottedVersionVector) of a replica across all active shards, used for Anti-Entropy exchanges.
/// </summary>
public sealed record CrdtStateSyncMessage(
    string ReplicaId,
    ShardVersionVectorMap State,
    bool IsTopologyAnnouncementOnly = false,
    bool IsPartialShardSelection = false) : IEquatable<CrdtStateSyncMessage>, IExtensibleDistributedPayload
{
    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(CrdtStateSyncMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return string.Equals(ReplicaId, other.ReplicaId, StringComparison.Ordinal) &&
               Equals(State, other.State) &&
               IsTopologyAnnouncementOnly == other.IsTopologyAnnouncementOnly &&
               IsPartialShardSelection == other.IsPartialShardSelection;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(ReplicaId, State, IsTopologyAnnouncementOnly, IsPartialShardSelection);
    }
}