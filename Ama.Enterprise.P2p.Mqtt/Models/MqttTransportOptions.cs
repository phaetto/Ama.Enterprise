namespace Ama.Enterprise.P2p.Mqtt.Models;

using System;

/// <summary>
/// Configuration options for the MQTT transport connection.
/// </summary>
public sealed class MqttTransportOptions : IEquatable<MqttTransportOptions>
{
    /// <summary>
    /// Gets or sets the MQTT broker host address.
    /// </summary>
    public string Host { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the MQTT broker port.
    /// </summary>
    public int Port { get; set; } = 1883;

    /// <summary>
    /// Gets or sets the base topic prefix used for routing messages within the mesh.
    /// </summary>
    public string TopicPrefix { get; set; } = "p2p-mesh";

    /// <summary>
    /// Gets or sets the optional username for broker authentication.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    /// Gets or sets the optional password for broker authentication.
    /// </summary>
    public string? Password { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether to use TLS for the connection.
    /// </summary>
    public bool UseTls { get; set; }

    /// <inheritdoc />
    public bool Equals(MqttTransportOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return string.Equals(Host, other.Host, StringComparison.OrdinalIgnoreCase) &&
               Port == other.Port &&
               string.Equals(TopicPrefix, other.TopicPrefix, StringComparison.Ordinal) &&
               string.Equals(Username, other.Username, StringComparison.Ordinal) &&
               string.Equals(Password, other.Password, StringComparison.Ordinal) &&
               UseTls == other.UseTls;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as MqttTransportOptions);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(Host, Port, TopicPrefix, Username, Password, UseTls);
    }
}