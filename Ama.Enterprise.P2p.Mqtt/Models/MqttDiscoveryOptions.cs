namespace Ama.Enterprise.P2p.Mqtt.Models;

using System;

/// <summary>
/// Configuration options for the MQTT peer discovery mechanism.
/// </summary>
public sealed class MqttDiscoveryOptions : IEquatable<MqttDiscoveryOptions>
{
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

        return DiscoveryInterval.Equals(other.DiscoveryInterval) &&
               DiscoveryTimeout.Equals(other.DiscoveryTimeout) &&
               string.Equals(DiscoveryTopicSuffix, other.DiscoveryTopicSuffix, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as MqttDiscoveryOptions);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(DiscoveryInterval, DiscoveryTimeout, DiscoveryTopicSuffix);
    }
}