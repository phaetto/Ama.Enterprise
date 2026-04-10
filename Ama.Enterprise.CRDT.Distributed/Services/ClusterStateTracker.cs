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
    private readonly object syncRoot = new();

    /// <inheritdoc />
    public void UpdatePeerState(string peerReplicaId, DottedVersionVector globalState)
    {
        if (string.IsNullOrWhiteSpace(peerReplicaId)) throw new ArgumentException("Peer Replica ID cannot be null or empty.", nameof(peerReplicaId));
        if (globalState == null) throw new ArgumentNullException(nameof(globalState));

        lock (syncRoot)
        {
            peerStates[peerReplicaId] = globalState;
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
    public void RemovePeerState(string peerReplicaId)
    {
        if (string.IsNullOrWhiteSpace(peerReplicaId)) throw new ArgumentException("Peer Replica ID cannot be null or empty.", nameof(peerReplicaId));

        lock (syncRoot)
        {
            peerStates.Remove(peerReplicaId);
        }
    }
}