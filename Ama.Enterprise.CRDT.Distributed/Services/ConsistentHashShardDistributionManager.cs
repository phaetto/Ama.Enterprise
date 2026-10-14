namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements consistent hashing to dynamically distribute CRDT shards across active cluster replicas securely and predictably.
/// Evaluates standard network liveliness ensuring targeted requests seamlessly resolve to mapped connected peers natively.
/// </summary>
public sealed class ConsistentHashShardDistributionManager : IShardDistributionManager
{
    private readonly IClusterStateTracker clusterTracker;
    private readonly string localReplicaId;
    private readonly IServiceProvider serviceProvider;
    private readonly IEnumerable<DistributedCrdtP2pRegistration> p2pRegistrations;
    private readonly int replicationFactor;
    private readonly int virtualNodes;

    private readonly object syncRoot = new();
    private HashSet<string> lastKnownReplicas = new(StringComparer.Ordinal);
    
    // Fast arrays used to execute Array.BinarySearch preventing list alloc bounds natively.
    private uint[] ringKeys = [];
    private string[] ringValues = [];

    public ConsistentHashShardDistributionManager(
        IClusterStateTracker clusterTracker,
        DistributedCrdtReplicaRegistration replicaRegistration,
        IServiceProvider serviceProvider,
        IEnumerable<DistributedCrdtP2pRegistration> p2pRegistrations,
        IOptions<DistributedCrdtOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        
        this.clusterTracker = clusterTracker ?? throw new ArgumentNullException(nameof(clusterTracker));
        this.localReplicaId = replicaRegistration?.ReplicaId ?? throw new ArgumentNullException(nameof(replicaRegistration));
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.p2pRegistrations = p2pRegistrations ?? throw new ArgumentNullException(nameof(p2pRegistrations));
        
        this.replicationFactor = Math.Max(1, options.Value.ShardReplicationFactor);
        this.virtualNodes = Math.Max(1, options.Value.VirtualNodesPerReplica);
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsShardLocalAsync(string shardId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(shardId)) return false;

        var targets = await GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
        
        for (int i = 0; i < targets.Count; i++)
        {
            if (string.Equals(targets[i], localReplicaId, StringComparison.Ordinal))
            {
                return true;
            }
        }
        
        return false;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<string>> GetTargetNodesForShardAsync(string shardId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(shardId)) return [];

        await EnsureRingUpdatedAsync(cancellationToken).ConfigureAwait(false);

        uint[] currentKeys;
        string[] currentValues;

        lock (syncRoot)
        {
            if (ringKeys.Length == 0)
            {
                return [localReplicaId];
            }
            currentKeys = ringKeys;
            currentValues = ringValues;
        }

        var hash = ComputeHash(shardId);
        var targets = new List<string>(replicationFactor);
        
        int index = Array.BinarySearch(currentKeys, hash);
        if (index < 0)
        {
            // Bitwise complement maps towards the index of the first larger element successfully
            index = ~index;
        }
        
        if (index >= currentKeys.Length)
        {
            // Wrap completely around towards the beginning of the ring explicitly
            index = 0;
        }

        // Collect specific unique replicas bounded up to the required replication factor natively
        for (int i = 0; i < currentKeys.Length && targets.Count < replicationFactor; i++)
        {
            var replica = currentValues[(index + i) % currentKeys.Length];
            
            bool alreadyAdded = false;
            for (int j = 0; j < targets.Count; j++)
            {
                if (string.Equals(targets[j], replica, StringComparison.Ordinal))
                {
                    alreadyAdded = true;
                    break;
                }
            }

            if (!alreadyAdded)
            {
                targets.Add(replica);
            }
        }

        return targets;
    }

    private async ValueTask EnsureRingUpdatedAsync(CancellationToken cancellationToken)
    {
        var activeReplicaIds = new HashSet<string>(StringComparer.Ordinal);
        
        // Resolve the registry through the service provider dynamically bridging capabilities 
        // to prevent DI validation failures if P2P meshes are not explicitly registered or to break circular graphs.
        var peerRegistry = serviceProvider.GetService<IPeerRegistry>();

        if (peerRegistry != null)
        {
            // Dynamically map solely connected live network peers explicitly bypassing offline tracked tombstone cooldowns
            foreach (var registration in p2pRegistrations)
            {
                if (!string.Equals(registration.ReplicaId, localReplicaId, StringComparison.Ordinal))
                {
                    continue;
                }

                var activePeers = await peerRegistry.GetPeersByStatusAsync(registration.MeshId, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
                foreach (var peer in activePeers)
                {
                    var peerIdStr = peer.Id.Value.ToString();
                    var replicaId = clusterTracker.GetReplicaIdByPeerId(peerIdStr);
                    
                    if (!string.IsNullOrWhiteSpace(replicaId))
                    {
                        activeReplicaIds.Add(replicaId);
                    }
                }
            }
        }
        
        lock (syncRoot)
        {
            activeReplicaIds.Add(localReplicaId); // Consistently evaluate local instance inside topological calculations

            if (activeReplicaIds.SetEquals(lastKnownReplicas))
            {
                return; // Topologies mirror effectively, return dynamically mapped arrays directly
            }

            var newRing = new SortedDictionary<uint, string>();
            
            // Fix determinism by ordering the set execution structurally explicitly guaranteeing the exact identical collision resolution across all cluster nodes
            var sortedReplicas = activeReplicaIds.OrderBy(r => r, StringComparer.Ordinal).ToList();
            
            foreach (var replica in sortedReplicas)
            {
                for (int i = 0; i < virtualNodes; i++)
                {
                    var vnodeKey = $"{replica}:vn:{i}";
                    var hash = ComputeHash(vnodeKey);
                    
                    // Resolves mathematically improbable edge hash collisions securely resolving next available gap
                    while (newRing.ContainsKey(hash))
                    {
                        hash++;
                    }
                    
                    newRing[hash] = replica;
                }
            }

            ringKeys = new uint[newRing.Count];
            ringValues = new string[newRing.Count];
            
            int idx = 0;
            foreach (var kvp in newRing)
            {
                ringKeys[idx] = kvp.Key;
                ringValues[idx] = kvp.Value;
                idx++;
            }

            lastKnownReplicas = activeReplicaIds;
        }
    }

    private static uint ComputeHash(string input)
    {
        // Encapsulating explicit SHA256 truncating mapped exactly into integer bounds securely naturally
        var bytes = Encoding.UTF8.GetBytes(input);
        var hashBytes = SHA256.HashData(bytes);
        return BitConverter.ToUInt32(hashBytes, 0);
    }
}