namespace Ama.Enterprise.P2p.Mqtt.Models;

using System;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Represents a peer endpoint identified by a unique MQTT Client ID for targeted topic routing.
/// </summary>
public sealed record MqttPeerEndpoint : PeerEndpoint, IEquatable<MqttPeerEndpoint>
{
    /// <summary>
    /// Gets the unique identifier for the targeted MQTT client.
    /// </summary>
    public string ClientId { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="MqttPeerEndpoint"/> struct.
    /// </summary>
    /// <param name="clientId">The associated MQTT client identifier.</param>
    public MqttPeerEndpoint(string clientId)
    {
        if (string.IsNullOrWhiteSpace(clientId))
        {
            throw new ArgumentException("Client ID cannot be null or empty.", nameof(clientId));
        }

        ClientId = clientId;
    }

    /// <inheritdoc />
    public bool Equals(MqttPeerEndpoint? other)
    {
        if (other is null) return false;
        return string.Equals(ClientId, other.ClientId, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return StringComparer.Ordinal.GetHashCode(ClientId);
    }
}