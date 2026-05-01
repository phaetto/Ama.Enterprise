namespace Ama.Enterprise.P2p.Mqtt.Models;

/// <summary>
/// Lightweight payload utilized strictly for Phase 1 MQTT active discovery mapping routing bounds.
/// </summary>
public sealed record MqttDiscoveryMessage
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
}