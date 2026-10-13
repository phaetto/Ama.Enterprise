namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements consistent hashing to dynamically distribute CRDT shards across active cluster replicas securely and predictably.
/// </summary>
public sealed class ConsistentHashShardDistributionManager : IShardDistributionManager
{
    private readonly IClusterStateTracker clusterTracker;
    private readonly string localReplicaId;
    private readonly int replicationFactor;
    private readonly int virtualNodes;

    private readonly object syncRoot = new();
    private HashSet<string> lastKnownReplicas = new(StringComparer.Ordinal);
    
    // Fast arrays used to execute Array.BinarySearch preventing list alloc bounds natively.
    private uint[] ringKeys = [];
    private string[] ringValues = [];

    public ConsistentHashShardDistributionManager(
        IClusterStateTracker clusterTracker,
        ReplicaContext replicaContext,
        IOptions<DistributedCrdtOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        
        this.clusterTracker = clusterTracker ?? throw new ArgumentNullException(nameof(clusterTracker));
        this.localReplicaId = replicaContext?.ReplicaId ?? throw new ArgumentNullException(nameof(replicaContext));
        
        this.replicationFactor = Math.Max(1, options.Value.ShardReplicationFactor);
        this.virtualNodes = Math.Max(1, options.Value.VirtualNodesPerReplica);
    }

    /// <inheritdoc />
    public bool IsShardLocal(string shardId)
    {
        var targets = GetTargetReplicas(shardId);
        
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
    public IReadOnlyList<string> GetTargetReplicas(string shardId)
    {
        if (string.IsNullOrWhiteSpace(shardId)) return [];

        EnsureRingUpdated();

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

    private void EnsureRingUpdated()
    {
        var activeRemotes = clusterTracker.GetActiveReplicaIds();
        
        lock (syncRoot)
        {
            var currentReplicas = new HashSet<string>(activeRemotes, StringComparer.Ordinal);
            currentReplicas.Add(localReplicaId); // Consistently evaluate local instance inside topological calculations

            if (currentReplicas.SetEquals(lastKnownReplicas))
            {
                return; // Topologies mirror effectively, return dynamically mapped arrays directly
            }

            var newRing = new SortedDictionary<uint, string>();
            foreach (var replica in currentReplicas)
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

            lastKnownReplicas = currentReplicas;
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