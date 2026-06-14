namespace Ama.Enterprise.P2p.Mqtt.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Payload utilized for Phase 2 decoupled MQTT network handshakes.
/// </summary>
public sealed record MqttHandshakeMessage : IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets the designated temporary topic intended for direct asynchronous handshake confirmations.
    /// </summary>
    public string ReplyToTopic { get; init; } = string.Empty;

    /// <summary>
    /// Gets the serialized generic PeerNode bytes encapsulating the overarching local node identity.
    /// </summary>
    public byte[] Payload { get; init; } = Array.Empty<byte>();

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }
}