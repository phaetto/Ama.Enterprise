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
    private readonly Dictionary<string, DottedVersionVector> peerStates = new();
    private readonly Dictionary<string, string> networkIdToReplicaId = new();
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
            
            peerStates[peerReplicaId] = globalState;
            networkIdToReplicaId[peerId] = peerReplicaId;
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<DottedVersionVector> GetClusterStates()
    {
        lock (syncRoot)
        {
            return peerStates.Values.ToList();
        }
    }

    /// <inheritdoc />
    public void RemovePeerByNetworkId(string peerId)
    {
        if (string.IsNullOrWhiteSpace(peerId)) throw new ArgumentException("Peer ID cannot be null or empty.", nameof(peerId));

        lock (syncRoot)
        {
            if (networkIdToReplicaId.TryGetValue(peerId, out var replicaId))
            {
                peerStates.Remove(replicaId);
                networkIdToReplicaId.Remove(peerId);
            }
        }
    }
}