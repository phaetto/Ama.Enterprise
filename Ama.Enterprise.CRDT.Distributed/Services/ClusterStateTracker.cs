namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Pooling;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Microsoft.Extensions.Logging;

/// <summary>
/// Singleton thread-safe implementation capturing localized maps representing overarching remote state matrix limits.
/// </summary>
public sealed class ClusterStateTracker : IClusterStateTracker, IDisposable
{
    private readonly IEnumerable<IClusterStateObserver> observers;
    private readonly ILogger<ClusterStateTracker> logger;
    private readonly ICrdtStringPool stringPool;
    
    private readonly Dictionary<string, PeerStateEntry> peerStates = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> networkIdToReplicaId = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DateTime> tombstonedReplicas = new(StringComparer.Ordinal);
    private readonly object syncRoot = new();

    private readonly Meter meter;
    private readonly Counter<long> stateUpdatesCounter;
    private readonly Counter<long> tombstonedPeersCounter;
    private readonly Counter<long> peerRemovalsCounter;
    private readonly Counter<long> expiredTombstonesCounter;

    public ClusterStateTracker(
        IEnumerable<IClusterStateObserver> observers,
        ILogger<ClusterStateTracker> logger,
        IMeterFactory? meterFactory = null,
        ICrdtStringPool? stringPool = null)
    {
        this.observers = observers ?? throw new ArgumentNullException(nameof(observers));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.stringPool = stringPool ?? CrdtStringPool.Shared;
        
        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.ClusterStateTracker") ?? new Meter("Ama.Enterprise.CRDT.Distributed.ClusterStateTracker");
        this.stateUpdatesCounter = this.meter.CreateCounter<long>("crdt.cluster.state_updates", "updates", "Number of cluster state updates");
        this.tombstonedPeersCounter = this.meter.CreateCounter<long>("crdt.cluster.tombstoned_peers", "peers", "Number of explicitly tombstoned replicas");
        this.peerRemovalsCounter = this.meter.CreateCounter<long>("crdt.cluster.peer_removals", "peers", "Number of network peers intentionally disconnected preserving states");
        this.expiredTombstonesCounter = this.meter.CreateCounter<long>("crdt.cluster.expired_tombstones", "peers", "Number of explicitly tombstoned replicas purged entirely after evaluating cooldown limits");
    }

    /// <inheritdoc />
    public void UpdatePeerState(string peerReplicaId, string peerId, ShardVersionVectorMap globalState)
    {
        if (string.IsNullOrWhiteSpace(peerReplicaId)) throw new ArgumentException("Peer Replica ID cannot be null or empty.", nameof(peerReplicaId));
        if (string.IsNullOrWhiteSpace(peerId)) throw new ArgumentException("Peer ID cannot be null or empty.", nameof(peerId));
        ArgumentNullException.ThrowIfNull(globalState);

        peerReplicaId = stringPool.GetOrAdd(peerReplicaId);
        peerId = stringPool.GetOrAdd(peerId);

        bool topologyChanged = false;

        lock (syncRoot)
        {
            // 1. Remove any old network mappings for this specific replica (enforce 1:1 relationship)
            var stalePeerIds = networkIdToReplicaId
                .Where(x => x.Value == peerReplicaId && x.Key != peerId)
                .Select(x => x.Key)
                .ToList();
                
            foreach (var stalePeerId in stalePeerIds)
            {
                networkIdToReplicaId.Remove(stalePeerId);
                topologyChanged = true;
            }

            // 2. Handle if the current network PeerId was previously mapped to a DIFFERENT replica
            if (!networkIdToReplicaId.TryGetValue(peerId, out var oldReplicaId) || oldReplicaId != peerReplicaId)
            {
                if (oldReplicaId != null)
                {
                    peerStates.Remove(oldReplicaId);
                }
                topologyChanged = true;
            }
            
            peerStates[peerReplicaId] = new PeerStateEntry(globalState, DateTime.UtcNow);
            networkIdToReplicaId[peerId] = peerReplicaId;
        }

        stateUpdatesCounter.Add(1, new KeyValuePair<string, object?>("replica_id", peerReplicaId));
        
        if (topologyChanged)
        {
            NotifyObservers();
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<ShardVersionVectorMap> GetClusterStates()
    {
        lock (syncRoot)
        {
            return peerStates.Values.Select(v => v.State).ToList();
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<string> GetActiveReplicaIds()
    {
        lock (syncRoot)
        {
            return networkIdToReplicaId.Values.Distinct().ToList();
        }
    }

    /// <inheritdoc />
    public string? GetPeerIdByReplicaId(string replicaId)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) return null;

        lock (syncRoot)
        {
            foreach (var kvp in networkIdToReplicaId)
            {
                if (kvp.Value == replicaId)
                {
                    return kvp.Key;
                }
            }
        }
        return null;
    }

    /// <inheritdoc />
    public string? GetReplicaIdByPeerId(string peerId)
    {
        if (string.IsNullOrWhiteSpace(peerId)) return null;

        lock (syncRoot)
        {
            if (networkIdToReplicaId.TryGetValue(peerId, out var replicaId))
            {
                return replicaId;
            }
        }
        return null;
    }

    /// <inheritdoc />
    public void RemovePeerByNetworkId(string peerId)
    {
        if (string.IsNullOrWhiteSpace(peerId)) throw new ArgumentException("Peer ID cannot be null or empty.", nameof(peerId));

        peerId = stringPool.GetOrAdd(peerId);
        bool removed = false;

        lock (syncRoot)
        {
            // ONLY unmap the network routing. We intentionally DO NOT remove the replica from `peerStates` here!
            // If we remove the CRDT state early, the GMVV will trim the journal and cause amnesia for offline peers.
            // The state remains safely bounded until `GetAndTombstoneExpiredPeers` handles the TTL timeout inherently.
            removed = networkIdToReplicaId.Remove(peerId);
        }
        
        if (removed)
        {
            peerRemovalsCounter.Add(1);
            NotifyObservers();
        }
    }

    /// <inheritdoc />
    public void TombstoneReplica(string replicaId)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) return;

        replicaId = stringPool.GetOrAdd(replicaId);
        bool topologyChanged = false;

        lock (syncRoot)
        {
            if (tombstonedReplicas.TryAdd(replicaId, DateTime.UtcNow))
            {
                tombstonedPeersCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaId));
                topologyChanged = true;
            }
            
            if (peerStates.Remove(replicaId))
            {
                topologyChanged = true;
            }
            
            var keysToRemove = networkIdToReplicaId
                .Where(x => x.Value == replicaId)
                .Select(x => x.Key)
                .ToList();
                
            foreach (var key in keysToRemove)
            {
                networkIdToReplicaId.Remove(key);
                topologyChanged = true;
            }
        }
        
        if (topologyChanged)
        {
            NotifyObservers();
        }
    }

    /// <inheritdoc />
    public string? TombstonePeerByNetworkId(string peerId)
    {
        if (string.IsNullOrWhiteSpace(peerId)) return null;

        string? replicaIdToTombstone = null;

        lock (syncRoot)
        {
            if (networkIdToReplicaId.TryGetValue(peerId, out var replicaId))
            {
                replicaIdToTombstone = replicaId;
            }
        }

        if (replicaIdToTombstone != null)
        {
            TombstoneReplica(replicaIdToTombstone);
            return replicaIdToTombstone;
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
        }

        foreach (var replicaId in expiredReplicas)
        {
            TombstoneReplica(replicaId);
        }

        return expiredReplicas;
    }

    /// <inheritdoc />
    public void CleanupExpiredTombstones(TimeSpan cooldown)
    {
        if (cooldown <= TimeSpan.Zero)
        {
            return;
        }

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

            // Deliberately skipping networkIdToReplicaId export, as Peer IDs are ephemeral session concepts
            // and should never survive across node reboots or long-term storage boundaries.

            foreach (var kvp in tombstonedReplicas)
            {
                dto.TombstonedReplicas.Add(kvp.Key, kvp.Value);
            }

            foreach (var kvp in peerStates)
            {
                var safeMap = new ShardVersionVectorMap();
                
                foreach (var shardKvp in kvp.Value.State)
                {
                    var pooledShardKey = stringPool.GetOrAdd(shardKvp.Key);
                    var safeStateVersions = shardKvp.Value.Versions.ToDictionary(v => stringPool.GetOrAdd(v.Key), v => v.Value, StringComparer.Ordinal);
                    var safeStateDots = shardKvp.Value.Dots?.ToDictionary(d => stringPool.GetOrAdd(d.Key), d => (ISet<long>)new HashSet<long>(d.Value), StringComparer.Ordinal) 
                                        ?? new Dictionary<string, ISet<long>>(StringComparer.Ordinal);

                    safeMap[pooledShardKey] = new DottedVersionVector(safeStateVersions, safeStateDots);
                }

                dto.PeerStates[stringPool.GetOrAdd(kvp.Key)] = new ClusterPeerStateDto
                {
                    State = safeMap,
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
            // Deliberately skipping networkIdToReplicaId import to prevent poisoning live routing
            // with dead ephemeral session identifiers loaded from storage.

            tombstonedReplicas.Clear();
            foreach (var kvp in state.TombstonedReplicas)
            {
                tombstonedReplicas[stringPool.GetOrAdd(kvp.Key)] = kvp.Value;
            }

            peerStates.Clear();
            foreach (var kvp in state.PeerStates)
            {
                var safeMap = new ShardVersionVectorMap();
                
                foreach (var shardKvp in kvp.Value.State)
                {
                    var pooledShardKey = stringPool.GetOrAdd(shardKvp.Key);
                    var safeStateVersions = shardKvp.Value.Versions.ToDictionary(v => stringPool.GetOrAdd(v.Key), v => v.Value, StringComparer.Ordinal);
                    var safeStateDots = shardKvp.Value.Dots?.ToDictionary(d => stringPool.GetOrAdd(d.Key), d => (ISet<long>)new HashSet<long>(d.Value), StringComparer.Ordinal);

                    safeMap[pooledShardKey] = new DottedVersionVector(safeStateVersions, safeStateDots);
                }

                peerStates[stringPool.GetOrAdd(kvp.Key)] = new PeerStateEntry(
                    safeMap,
                    kvp.Value.LastSeen);
            }
        }
        
        NotifyObservers();
        
        // Immediately purge tombstones that expired while offline evaluating limits.
        if (cooldown > TimeSpan.Zero)
        {
            CleanupExpiredTombstones(cooldown);
        }
    }

    public void Dispose()
    {
        meter.Dispose();
    }
    
    private void NotifyObservers()
    {
        foreach (var observer in observers)
        {
            try
            {
                observer.OnClusterTopologyChanged();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred executing cluster state topology observer explicitly mapped bounding limits cleanly.");
            }
        }
    }

    private readonly record struct PeerStateEntry : IEquatable<PeerStateEntry>
    {
        public ShardVersionVectorMap State { get; }
        public DateTime LastSeen { get; }

        public PeerStateEntry(ShardVersionVectorMap state, DateTime lastSeen)
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