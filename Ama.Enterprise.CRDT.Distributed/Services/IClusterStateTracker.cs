namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using Ama.CRDT.Models;

/// <summary>
/// Tracks the last known synchronization bounds (Global Dotted Version Vectors) for all connected peers.
/// Used specifically to aggregate metrics for safe mathematical journal trimming limits asynchronously.
/// </summary>
public interface IClusterStateTracker
{
    /// <summary>
    /// Updates the tracked boundaries matching the extracted global synchronizations.
    /// </summary>
    /// <param name="peerReplicaId">The remote peer node replica identifier.</param>
    /// <param name="peerId">The remote network peer identifier.</param>
    /// <param name="globalState">The most recent bounds broadcasted matching underlying global structural maps natively.</param>
    void UpdatePeerState(string peerReplicaId, string peerId, DottedVersionVector globalState);

    /// <summary>
    /// Retrieves a complete snapshot containing every currently connected peer.
    /// </summary>
    IReadOnlyList<DottedVersionVector> GetClusterStates();

    /// <summary>
    /// Safely unmaps a disconnected network peer ID without causing CRDT amnesia. 
    /// The CRDT state remains preserved until the TTL expires allowing for safe offline recovery.
    /// </summary>
    /// <param name="peerId">The remote network peer identifier.</param>
    void RemovePeerByNetworkId(string peerId);

    /// <summary>
    /// Permanently tombstones a replica, tracking its ID so any future incoming messages from it are categorically rejected
    /// enforcing strict cluster continuity and preventing split-brain amnesia anomalies natively.
    /// </summary>
    /// <param name="replicaId">The identifier of the replica to tombstone.</param>
    void TombstoneReplica(string replicaId);

    /// <summary>
    /// Instantly tombstones a peer based on its network ID, typically used during graceful P2P departures.
    /// </summary>
    /// <param name="peerId">The remote network peer identifier.</param>
    /// <returns>The CRDT Replica ID if a mapping existed; otherwise, null.</returns>
    string? TombstonePeerByNetworkId(string peerId);

    /// <summary>
    /// Checks whether the specified replica identifier has been explicitly tombstoned by the cluster.
    /// </summary>
    /// <param name="replicaId">The replica identifier to check.</param>
    /// <returns><c>true</c> if tombstoned; otherwise, <c>false</c>.</returns>
    bool IsReplicaTombstoned(string replicaId);

    /// <summary>
    /// Identifies and tombstones peers that have not updated their state within the specified Time-To-Live (TTL) limit natively avoiding network amnesia edge cases.
    /// </summary>
    /// <param name="ttl">The time-to-live duration determining expiration.</param>
    /// <returns>A list of replica identifiers that were actively tombstoned.</returns>
    IReadOnlyList<string> GetAndTombstoneExpiredPeers(TimeSpan ttl);
}