namespace Ama.Enterprise.P2p.Mqtt.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Lightweight payload utilized strictly for Phase 1 MQTT active discovery mapping routing bounds.
/// </summary>
public sealed record MqttDiscoveryMessage : IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets the explicit mesh context identifier isolating the targeted topology.
    /// </summary>
    public string MeshId { get; init; } = string.Empty;

    /// <summary>
    /// Gets the globally unique identifier associated with the originating node.
    /// </summary>
    public string ClientId { get; init; } = string.Empty;

    /// <summary>
    /// Gets the IP address associated with the originating node to be used for handshaking.
    /// </summary>
    public string IpAddress { get; init; } = string.Empty;

    /// <summary>
    /// Gets the specific pseudo-port discriminator preventing localized handshake collisions.
    /// </summary>
    public int HandshakePort { get; init; }

    /// <summary>
    /// Gets the specific topic intended to capture Phase 1 return ping traces directly.
    /// </summary>
    public string ReplyToTopic { get; init; } = string.Empty;

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }
}