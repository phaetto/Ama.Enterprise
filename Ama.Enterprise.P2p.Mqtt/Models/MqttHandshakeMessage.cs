namespace Ama.Enterprise.P2p.Mqtt.Models;

using System;

/// <summary>
/// Payload utilized for Phase 2 decoupled MQTT network handshakes.
/// </summary>
public sealed record MqttHandshakeMessage
{
    /// <summary>
    /// Gets the designated temporary topic intended for direct asynchronous handshake confirmations.
    /// </summary>
    public string ReplyToTopic { get; init; } = string.Empty;

    /// <summary>
    /// Gets the serialized generic PeerNode bytes encapsulating the overarching local node identity.
    /// </summary>
    public byte[] Payload { get; init; } = Array.Empty<byte>();
}