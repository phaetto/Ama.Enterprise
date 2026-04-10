namespace Ama.Enterprise.P2p.WebRTC.Models;

using System;

/// <summary>
/// DTO sent immediately upon data channel opening to identify the remote peer within the network topology.
/// </summary>
public sealed record WebRtcHandshakeMessage
{
    /// <summary>
    /// Gets the globally unique identifier of the connecting peer.
    /// </summary>
    public Guid PeerId { get; init; }
}