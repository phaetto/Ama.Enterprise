namespace Ama.Enterprise.P2p.WebRTC.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// DTO representing a WebRTC invitation answer.
/// </summary>
public sealed class WebRtcInvitationAnswer : IEquatable<WebRtcInvitationAnswer>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets or sets the connection identifier.
    /// </summary>
    public Guid ConnectionId { get; set; }

    /// <summary>
    /// Gets or sets the SDP answer string.
    /// </summary>
    public string SdpAnswer { get; set; } = string.Empty;

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="WebRtcInvitationAnswer"/> class.
    /// </summary>
    public WebRtcInvitationAnswer()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="WebRtcInvitationAnswer"/> class.
    /// </summary>
    /// <param name="connectionId">The associated connection ID.</param>
    /// <param name="sdpAnswer">The generated SDP answer string.</param>
    public WebRtcInvitationAnswer(Guid connectionId, string sdpAnswer)
    {
        ConnectionId = connectionId;
        SdpAnswer = sdpAnswer ?? string.Empty;
    }

    /// <inheritdoc />
    public bool Equals(WebRtcInvitationAnswer? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return ConnectionId.Equals(other.ConnectionId) &&
               string.Equals(SdpAnswer, other.SdpAnswer, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as WebRtcInvitationAnswer);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(ConnectionId, SdpAnswer);
}