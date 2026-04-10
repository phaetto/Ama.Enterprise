namespace Ama.Enterprise.P2p.WebRTC.Models;

using System;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Represents a WebRTC data channel connection endpoint.
/// </summary>
public sealed record WebRtcPeerEndpoint : PeerEndpoint, IEquatable<WebRtcPeerEndpoint>
{
    /// <summary>
    /// Gets the unique identifier for the locally tracked WebRTC connection.
    /// </summary>
    public Guid ConnectionId { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="WebRtcPeerEndpoint"/> struct.
    /// </summary>
    /// <param name="connectionId">The WebRTC connection ID.</param>
    public WebRtcPeerEndpoint(Guid connectionId)
    {
        ConnectionId = connectionId;
    }

    /// <inheritdoc />
    public bool Equals(WebRtcPeerEndpoint? other)
    {
        if (other is null) return false;
        return ConnectionId.Equals(other.ConnectionId);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return ConnectionId.GetHashCode();
    }
}