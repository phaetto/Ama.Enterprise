namespace Ama.Enterprise.P2p.Mqtt.Models;

using System;

/// <summary>
/// Configuration structure isolating localized MQTT targeted handshake connections.
/// </summary>
public sealed class MqttHandshakeOptions : IEquatable<MqttHandshakeOptions>
{
    /// <summary>
    /// Gets or sets the active MQTT broker host.
    /// </summary>
    public string Host { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the port bound to the active MQTT endpoint.
    /// </summary>
    public int Port { get; set; } = 1883;

    /// <summary>
    /// Gets or sets the baseline prefix managing localized topology routes.
    /// </summary>
    public string TopicPrefix { get; set; } = "p2p-mesh";

    /// <summary>
    /// Gets or sets the optional broker username.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    /// Gets or sets the optional broker authentication token.
    /// </summary>
    public string? Password { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether strict TLS parameters are mandated.
    /// </summary>
    public bool UseTls { get; set; }

    /// <summary>
    /// Gets or sets the maximum allowance evaluated prior to timing out targeted probes.
    /// </summary>
    public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the standard suffix identifying Phase 2 topics.
    /// </summary>
    public string HandshakeTopicSuffix { get; set; } = "handshake";

    /// <inheritdoc />
    public bool Equals(MqttHandshakeOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return string.Equals(Host, other.Host, StringComparison.OrdinalIgnoreCase) &&
               Port == other.Port &&
               string.Equals(TopicPrefix, other.TopicPrefix, StringComparison.Ordinal) &&
               string.Equals(Username, other.Username, StringComparison.Ordinal) &&
               string.Equals(Password, other.Password, StringComparison.Ordinal) &&
               UseTls == other.UseTls &&
               HandshakeTimeout.Equals(other.HandshakeTimeout) &&
               string.Equals(HandshakeTopicSuffix, other.HandshakeTopicSuffix, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as MqttHandshakeOptions);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Host, StringComparer.OrdinalIgnoreCase);
        hash.Add(Port);
        hash.Add(TopicPrefix, StringComparer.Ordinal);
        hash.Add(Username, StringComparer.Ordinal);
        hash.Add(Password, StringComparer.Ordinal);
        hash.Add(UseTls);
        hash.Add(HandshakeTimeout);
        hash.Add(HandshakeTopicSuffix, StringComparer.Ordinal);
        return hash.ToHashCode();
    }
}