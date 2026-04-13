namespace Ama.Enterprise.P2p.WebRTC.Models;

using System;

/// <summary>
/// DTO representing a WebRTC invitation offer.
/// </summary>
public readonly record struct WebRtcInvitationOffer : IEquatable<WebRtcInvitationOffer>
{
    /// <summary>
    /// Gets the connection identifier.
    /// </summary>
    public Guid ConnectionId { get; init; }

    /// <summary>
    /// Gets the SDP offer string.
    /// </summary>
    public string SdpOffer { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="WebRtcInvitationOffer"/> struct.
    /// </summary>
    /// <param name="connectionId">The assigned connection ID.</param>
    /// <param name="sdpOffer">The generated SDP offer string.</param>
    public WebRtcInvitationOffer(Guid connectionId, string sdpOffer)
    {
        ConnectionId = connectionId;
        SdpOffer = sdpOffer ?? string.Empty;
    }

    /// <inheritdoc />
    public bool Equals(WebRtcInvitationOffer other) =>
        ConnectionId.Equals(other.ConnectionId) &&
        string.Equals(SdpOffer, other.SdpOffer, StringComparison.Ordinal);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(ConnectionId, SdpOffer);
}