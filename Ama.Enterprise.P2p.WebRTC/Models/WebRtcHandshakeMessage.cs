namespace Ama.Enterprise.P2p.WebRTC.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// DTO sent immediately upon data channel opening to identify the remote peer within the network topology.
/// </summary>
public sealed record WebRtcHandshakeMessage : IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets the globally unique identifier of the connecting peer.
    /// </summary>
    public Guid PeerId { get; init; }

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }
}