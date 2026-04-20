namespace Ama.Enterprise.P2p.Mqtt.Models;

using System;

/// <summary>
/// Configuration options for the MQTT peer discovery mechanism.
/// </summary>
public sealed class MqttDiscoveryOptions : IEquatable<MqttDiscoveryOptions>
{
    /// <summary>
    /// Gets or sets the MQTT broker host address for discovery broadcasts.
    /// </summary>
    public string Host { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the MQTT broker port for discovery broadcasts.
    /// </summary>
    public int Port { get; set; } = 1883;

    /// <summary>
    /// Gets or sets the base topic prefix used for routing discovery messages within the mesh.
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

    /// <summary>
    /// Gets or sets the time interval between active peer discovery broadcasts.
    /// </summary>
    public TimeSpan DiscoveryInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the duration to wait for discovery responses after a broadcast.
    /// </summary>
    public TimeSpan DiscoveryTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the suffix appended to the base topic prefix for discovery broadcasts.
    /// </summary>
    public string DiscoveryTopicSuffix { get; set; } = "discovery";

    /// <inheritdoc />
    public bool Equals(MqttDiscoveryOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return string.Equals(Host, other.Host, StringComparison.OrdinalIgnoreCase) &&
               Port == other.Port &&
               string.Equals(TopicPrefix, other.TopicPrefix, StringComparison.Ordinal) &&
               string.Equals(Username, other.Username, StringComparison.Ordinal) &&
               string.Equals(Password, other.Password, StringComparison.Ordinal) &&
               UseTls == other.UseTls &&
               DiscoveryInterval.Equals(other.DiscoveryInterval) &&
               DiscoveryTimeout.Equals(other.DiscoveryTimeout) &&
               string.Equals(DiscoveryTopicSuffix, other.DiscoveryTopicSuffix, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as MqttDiscoveryOptions);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Host);
        hash.Add(Port);
        hash.Add(TopicPrefix);
        hash.Add(Username);
        hash.Add(Password);
        hash.Add(UseTls);
        hash.Add(DiscoveryInterval);
        hash.Add(DiscoveryTimeout);
        hash.Add(DiscoveryTopicSuffix);
        return hash.ToHashCode();
    }
}