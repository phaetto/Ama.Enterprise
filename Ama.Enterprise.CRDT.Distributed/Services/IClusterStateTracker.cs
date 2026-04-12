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
    /// Updates the tracked boundaries securely matching the explicitly extracted explicit global synchronizations seamlessly.
    /// </summary>
    /// <param name="peerReplicaId">The remote peer node replica identifier.</param>
    /// <param name="peerId">The remote network peer identifier.</param>
    /// <param name="globalState">The most recent bounds broadcasted matching underlying global structural maps natively.</param>
    void UpdatePeerState(string peerReplicaId, string peerId, DottedVersionVector globalState);

    /// <summary>
    /// Retrieves a complete snapshot containing every currently connected peer mapped implicitly alongside native state parameters actively safely.
    /// </summary>
    IReadOnlyList<DottedVersionVector> GetClusterStates();

    /// <summary>
    /// Removes a disconnected peer from the explicitly mapped underlying sequence boundaries explicitly securely using its network ID.
    /// </summary>
    /// <param name="peerId">The remote network peer identifier.</param>
    void RemovePeerByNetworkId(string peerId);

    /// <summary>
    /// Identifies and removes peers that have not updated their state within the specified Time-To-Live (TTL) limit.
    /// </summary>
    /// <param name="ttl">The time-to-live duration determining expiration.</param>
    /// <returns>A list of replica identifiers that were evicted.</returns>
    IReadOnlyList<string> GetAndRemoveExpiredPeers(TimeSpan ttl);
}