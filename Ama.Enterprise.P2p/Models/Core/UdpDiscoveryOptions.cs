namespace Ama.Enterprise.P2p.Models.Core;

using System;

/// <summary>
/// Configuration options for tuning the behavior of UDP multicast peer discovery.
/// </summary>
public sealed class UdpDiscoveryOptions : IEquatable<UdpDiscoveryOptions>
{
    /// <summary>
    /// Gets or sets the multicast IP address used for sending and listening to discovery requests.
    /// </summary>
    public string MulticastAddress { get; set; } = "239.255.255.250";

    /// <summary>
    /// Gets or sets the network port used for multicast discovery.
    /// </summary>
    public int MulticastPort { get; set; } = 8021;

    /// <summary>
    /// Gets or sets the duration to wait for discovery responses before returning the result.
    /// </summary>
    public TimeSpan DiscoveryTimeout { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the interval at which the background service will actively broadcast discovery requests.
    /// </summary>
    public TimeSpan DiscoveryInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <inheritdoc />
    public bool Equals(UdpDiscoveryOptions? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return MulticastPort == other.MulticastPort &&
               DiscoveryTimeout.Equals(other.DiscoveryTimeout) &&
               DiscoveryInterval.Equals(other.DiscoveryInterval) &&
               string.Equals(MulticastAddress, other.MulticastAddress, StringComparison.OrdinalIgnoreCase);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as UdpDiscoveryOptions);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(
            StringComparer.OrdinalIgnoreCase.GetHashCode(MulticastAddress ?? string.Empty),
            MulticastPort,
            DiscoveryTimeout,
            DiscoveryInterval);
    }
}