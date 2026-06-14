namespace Ama.Enterprise.P2p.WebRTC.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// DTO representing a WebRTC invitation offer.
/// </summary>
public sealed class WebRtcInvitationOffer : IEquatable<WebRtcInvitationOffer>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets or sets the connection identifier.
    /// </summary>
    public Guid ConnectionId { get; set; }

    /// <summary>
    /// Gets or sets the SDP offer string.
    /// </summary>
    public string SdpOffer { get; set; } = string.Empty;

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="WebRtcInvitationOffer"/> class.
    /// </summary>
    public WebRtcInvitationOffer()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="WebRtcInvitationOffer"/> class.
    /// </summary>
    /// <param name="connectionId">The assigned connection ID.</param>
    /// <param name="sdpOffer">The generated SDP offer string.</param>
    public WebRtcInvitationOffer(Guid connectionId, string sdpOffer)
    {
        ConnectionId = connectionId;
        SdpOffer = sdpOffer ?? string.Empty;
    }

    /// <inheritdoc />
    public bool Equals(WebRtcInvitationOffer? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return ConnectionId.Equals(other.ConnectionId) &&
               string.Equals(SdpOffer, other.SdpOffer, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as WebRtcInvitationOffer);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(ConnectionId, SdpOffer);
}