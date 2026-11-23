namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;

/// <summary>
/// Represents the authorization context for evaluating whether a remote peer can access or replicate a specific CRDT scope.
/// </summary>
/// <param name="ScopeId">The opaque identifier of the CRDT scope or causal compartment.</param>
/// <param name="PeerNetworkId">The network identifier of the remote peer.</param>
/// <param name="MeshId">The optional identifier of the P2P mesh over which the message travels.</param>
/// <param name="MessageType">The optional CRDT wire message type being evaluated.</param>
public readonly record struct ScopeAccessContext(
    string ScopeId,
    string PeerNetworkId,
    string? MeshId = null,
    string? MessageType = null) : IEquatable<ScopeAccessContext>;