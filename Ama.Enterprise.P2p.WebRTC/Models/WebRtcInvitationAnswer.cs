namespace Ama.Enterprise.P2p.WebRTC.Models;

using System;

/// <summary>
/// DTO representing a WebRTC invitation answer.
/// </summary>
public readonly record struct WebRtcInvitationAnswer : IEquatable<WebRtcInvitationAnswer>
{
    /// <summary>
    /// Gets the connection identifier.
    /// </summary>
    public Guid ConnectionId { get; init; }

    /// <summary>
    /// Gets the SDP answer string.
    /// </summary>
    public string SdpAnswer { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="WebRtcInvitationAnswer"/> struct.
    /// </summary>
    /// <param name="connectionId">The associated connection ID.</param>
    /// <param name="sdpAnswer">The generated SDP answer string.</param>
    public WebRtcInvitationAnswer(Guid connectionId, string sdpAnswer)
    {
        ConnectionId = connectionId;
        SdpAnswer = sdpAnswer ?? string.Empty;
    }

    /// <inheritdoc />
    public bool Equals(WebRtcInvitationAnswer other) =>
        ConnectionId.Equals(other.ConnectionId) &&
        string.Equals(SdpAnswer, other.SdpAnswer, StringComparison.Ordinal);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(ConnectionId, SdpAnswer);
}