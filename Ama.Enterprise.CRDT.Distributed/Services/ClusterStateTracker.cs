namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using Ama.CRDT.Models;

/// <summary>
/// Singleton thread-safe implementation strictly capturing localized maps representing exact overarching remote state matrix limits.
/// </summary>
public sealed class ClusterStateTracker : IClusterStateTracker
{
    private readonly Dictionary<string, PeerStateEntry> peerStates = new();
    private readonly Dictionary<string, string> networkIdToReplicaId = new();
    private readonly HashSet<string> tombstonedReplicas = new();
    private readonly object syncRoot = new();

    /// <inheritdoc />
    public void UpdatePeerState(string peerReplicaId, string peerId, DottedVersionVector globalState)
    {
        if (string.IsNullOrWhiteSpace(peerReplicaId)) throw new ArgumentException("Peer Replica ID cannot be null or empty.", nameof(peerReplicaId));
        if (string.IsNullOrWhiteSpace(peerId)) throw new ArgumentException("Peer ID cannot be null or empty.", nameof(peerId));
        if (globalState == null) throw new ArgumentNullException(nameof(globalState));

        lock (syncRoot)
        {
            if (networkIdToReplicaId.TryGetValue(peerId, out var oldReplicaId) && oldReplicaId != peerReplicaId)
            {
                peerStates.Remove(oldReplicaId);
            }
            
            peerStates[peerReplicaId] = new PeerStateEntry(globalState, DateTime.UtcNow);
            networkIdToReplicaId[peerId] = peerReplicaId;
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<DottedVersionVector> GetClusterStates()
    {
        lock (syncRoot)
        {
            return peerStates.Values.Select(v => v.State).ToList();
        }
    }

    /// <inheritdoc />
    public void RemovePeerByNetworkId(string peerId)
    {
        if (string.IsNullOrWhiteSpace(peerId)) throw new ArgumentException("Peer ID cannot be null or empty.", nameof(peerId));

        lock (syncRoot)
        {
            // ONLY unmap the network routing. We intentionally DO NOT remove the replica from `peerStates` here!
            // If we remove the CRDT state early, the GMVV will trim the journal and cause amnesia for offline peers.
            // The state remains safely bounded until `GetAndTombstoneExpiredPeers` handles the TTL timeout inherently.
            networkIdToReplicaId.Remove(peerId);
        }
    }

    /// <inheritdoc />
    public void TombstoneReplica(string replicaId)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) return;
        
        lock (syncRoot)
        {
            tombstonedReplicas.Add(replicaId);
            peerStates.Remove(replicaId);
            
            var keysToRemove = networkIdToReplicaId
                .Where(x => x.Value == replicaId)
                .Select(x => x.Key)
                .ToList();
                
            foreach (var key in keysToRemove)
            {
                networkIdToReplicaId.Remove(key);
            }
        }
    }

    /// <inheritdoc />
    public string? TombstonePeerByNetworkId(string peerId)
    {
        if (string.IsNullOrWhiteSpace(peerId)) return null;

        lock (syncRoot)
        {
            if (networkIdToReplicaId.TryGetValue(peerId, out var replicaId))
            {
                TombstoneReplica(replicaId);
                return replicaId;
            }
        }

        return null;
    }

    /// <inheritdoc />
    public bool IsReplicaTombstoned(string replicaId)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) return false;

        lock (syncRoot)
        {
            return tombstonedReplicas.Contains(replicaId);
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<string> GetAndTombstoneExpiredPeers(TimeSpan ttl)
    {
        var now = DateTime.UtcNow;
        var expiredReplicas = new List<string>();

        lock (syncRoot)
        {
            foreach (var kvp in peerStates)
            {
                if (now - kvp.Value.LastSeen > ttl)
                {
                    expiredReplicas.Add(kvp.Key);
                }
            }

            foreach (var replicaId in expiredReplicas)
            {
                TombstoneReplica(replicaId);
            }
        }

        return expiredReplicas;
    }

    private readonly record struct PeerStateEntry : IEquatable<PeerStateEntry>
    {
        public DottedVersionVector State { get; }
        public DateTime LastSeen { get; }

        public PeerStateEntry(DottedVersionVector state, DateTime lastSeen)
        {
            State = state ?? throw new ArgumentNullException(nameof(state));
            LastSeen = lastSeen;
        }

        public bool Equals(PeerStateEntry other)
        {
            return LastSeen == other.LastSeen && Equals(State, other.State);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(State, LastSeen);
        }
    }
}