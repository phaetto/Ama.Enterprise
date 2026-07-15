namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Singleton thread-safe implementation capturing localized maps representing overarching remote state matrix limits.
/// </summary>
public sealed class ClusterStateTracker : IClusterStateTracker, IDisposable
{
    private readonly Dictionary<string, PeerStateEntry> peerStates = new();
    private readonly Dictionary<string, string> networkIdToReplicaId = new();
    private readonly Dictionary<string, DateTime> tombstonedReplicas = new();
    private readonly object syncRoot = new();

    private readonly Meter meter;
    private readonly Counter<long> stateUpdatesCounter;
    private readonly Counter<long> tombstonedPeersCounter;
    private readonly Counter<long> peerRemovalsCounter;
    private readonly Counter<long> expiredTombstonesCounter;

    public ClusterStateTracker(IMeterFactory? meterFactory = null)
    {
        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.ClusterStateTracker") ?? new Meter("Ama.Enterprise.CRDT.Distributed.ClusterStateTracker");
        this.stateUpdatesCounter = this.meter.CreateCounter<long>("crdt.cluster.state_updates", "updates", "Number of cluster state updates");
        this.tombstonedPeersCounter = this.meter.CreateCounter<long>("crdt.cluster.tombstoned_peers", "peers", "Number of explicitly tombstoned replicas");
        this.peerRemovalsCounter = this.meter.CreateCounter<long>("crdt.cluster.peer_removals", "peers", "Number of network peers intentionally disconnected preserving states");
        this.expiredTombstonesCounter = this.meter.CreateCounter<long>("crdt.cluster.expired_tombstones", "peers", "Number of explicitly tombstoned replicas purged entirely after evaluating cooldown limits");
    }

    /// <inheritdoc />
    public void UpdatePeerState(string peerReplicaId, string peerId, DottedVersionVector globalState)
    {
        if (string.IsNullOrWhiteSpace(peerReplicaId)) throw new ArgumentException("Peer Replica ID cannot be null or empty.", nameof(peerReplicaId));
        if (string.IsNullOrWhiteSpace(peerId)) throw new ArgumentException("Peer ID cannot be null or empty.", nameof(peerId));
        ArgumentNullException.ThrowIfNull(globalState);

        lock (syncRoot)
        {
            if (networkIdToReplicaId.TryGetValue(peerId, out var oldReplicaId) && oldReplicaId != peerReplicaId)
            {
                peerStates.Remove(oldReplicaId);
            }
            
            peerStates[peerReplicaId] = new PeerStateEntry(globalState, DateTime.UtcNow);
            networkIdToReplicaId[peerId] = peerReplicaId;
        }

        stateUpdatesCounter.Add(1, new KeyValuePair<string, object?>("replica_id", peerReplicaId));
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
            if (networkIdToReplicaId.Remove(peerId))
            {
                peerRemovalsCounter.Add(1);
            }
        }
    }

    /// <inheritdoc />
    public void TombstoneReplica(string replicaId)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) return;
        
        lock (syncRoot)
        {
            if (tombstonedReplicas.TryAdd(replicaId, DateTime.UtcNow))
            {
                tombstonedPeersCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaId));
            }
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
            return tombstonedReplicas.ContainsKey(replicaId);
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

    /// <inheritdoc />
    public void CleanupExpiredTombstones(TimeSpan cooldown)
    {
        var now = DateTime.UtcNow;

        lock (syncRoot)
        {
            var keysToRemove = new List<string>();
            foreach (var kvp in tombstonedReplicas)
            {
                if (now - kvp.Value > cooldown)
                {
                    keysToRemove.Add(kvp.Key);
                }
            }

            foreach (var key in keysToRemove)
            {
                tombstonedReplicas.Remove(key);
                expiredTombstonesCounter.Add(1, new KeyValuePair<string, object?>("replica_id", key));
            }
        }
    }

    /// <inheritdoc />
    public ClusterStateSnapshotDto ExportState()
    {
        lock (syncRoot)
        {
            var dto = new ClusterStateSnapshotDto();

            foreach (var kvp in networkIdToReplicaId)
            {
                dto.NetworkIdToReplicaId[kvp.Key] = kvp.Value;
            }

            foreach (var kvp in tombstonedReplicas)
            {
                dto.TombstonedReplicas.Add(kvp.Key, kvp.Value);
            }

            foreach (var kvp in peerStates)
            {
                var safeStateVersions = kvp.Value.State.Versions.ToDictionary(v => v.Key, v => v.Value);
                var safeStateDots = kvp.Value.State.Dots?.ToDictionary(d => d.Key, d => (ISet<long>)new HashSet<long>(d.Value)) 
                                    ?? new Dictionary<string, ISet<long>>();

                dto.PeerStates[kvp.Key] = new ClusterPeerStateDto
                {
                    State = new DottedVersionVector(safeStateVersions, safeStateDots),
                    LastSeen = kvp.Value.LastSeen
                };
            }

            return dto;
        }
    }

    /// <inheritdoc />
    public void ImportState(ClusterStateSnapshotDto state, TimeSpan cooldown)
    {
        ArgumentNullException.ThrowIfNull(state);

        lock (syncRoot)
        {
            networkIdToReplicaId.Clear();
            foreach (var kvp in state.NetworkIdToReplicaId)
            {
                networkIdToReplicaId[kvp.Key] = kvp.Value;
            }

            tombstonedReplicas.Clear();
            foreach (var kvp in state.TombstonedReplicas)
            {
                tombstonedReplicas[kvp.Key] = kvp.Value;
            }

            peerStates.Clear();
            foreach (var kvp in state.PeerStates)
            {
                var safeStateVersions = kvp.Value.State.Versions.ToDictionary(v => v.Key, v => v.Value);
                var safeStateDots = kvp.Value.State.Dots?.ToDictionary(d => d.Key, d => (ISet<long>)new HashSet<long>(d.Value));

                peerStates[kvp.Key] = new PeerStateEntry(
                    new DottedVersionVector(safeStateVersions, safeStateDots),
                    kvp.Value.LastSeen);
            }
        }
        
        // Immediately purge tombstones that expired while offline evaluating limits.
        CleanupExpiredTombstones(cooldown);
    }

    public void Dispose()
    {
        meter.Dispose();
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