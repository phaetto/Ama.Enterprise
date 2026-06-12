namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using Ama.CRDT.Models;

/// <summary>
/// AOT friendly data transfer object capturing the complete in-memory matrix of cluster states, tombstones, and topologies.
/// This model safely enables persistence, bypassing split-brain restart amnesia edge cases natively.
/// </summary>
public sealed record ClusterPeerStateDto : IEquatable<ClusterPeerStateDto>
{
    public DottedVersionVector State { get; init; } = new(new Dictionary<string, long>(), new Dictionary<string, ISet<long>>());
    
    public DateTime LastSeen { get; init; }

    public bool Equals(ClusterPeerStateDto? other)
    {
        if (other is null) return false;
        return LastSeen == other.LastSeen && Equals(State, other.State);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(State, LastSeen);
    }
}

/// <summary>
/// AOT friendly root data transfer object storing the explicitly mapped cluster tracking parameters bridging multi-node network meshes safely natively.
/// </summary>
public sealed record ClusterStateSnapshotDto : IEquatable<ClusterStateSnapshotDto>
{
    public Dictionary<string, string> NetworkIdToReplicaId { get; init; } = new();
    
    public Dictionary<string, ClusterPeerStateDto> PeerStates { get; init; } = new();
    
    public HashSet<string> TombstonedReplicas { get; init; } = new();

    public bool Equals(ClusterStateSnapshotDto? other)
    {
        if (ReferenceEquals(this, other)) return true;
        if (other is null) return false;

        if (NetworkIdToReplicaId.Count != other.NetworkIdToReplicaId.Count ||
            PeerStates.Count != other.PeerStates.Count ||
            TombstonedReplicas.Count != other.TombstonedReplicas.Count)
        {
            return false;
        }

        foreach (var kvp in NetworkIdToReplicaId)
        {
            if (!other.NetworkIdToReplicaId.TryGetValue(kvp.Key, out var otherVal) || kvp.Value != otherVal)
            {
                return false;
            }
        }

        foreach (var kvp in PeerStates)
        {
            if (!other.PeerStates.TryGetValue(kvp.Key, out var otherVal) || !Equals(kvp.Value, otherVal))
            {
                return false;
            }
        }

        if (!TombstonedReplicas.SetEquals(other.TombstonedReplicas))
        {
            return false;
        }

        return true;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(NetworkIdToReplicaId.Count, PeerStates.Count, TombstonedReplicas.Count);
    }
}