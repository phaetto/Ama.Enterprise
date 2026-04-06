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
    public TimeSpan DiscoveryTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Gets or sets the interval at which the background service will actively broadcast discovery requests.
    /// </summary>
    public TimeSpan DiscoveryInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the unique identifier of the local peer.
    /// </summary>
    public Guid LocalPeerId { get; set; } = Guid.NewGuid();

    /// <summary>
    /// Gets or sets the host address where the local peer is listening for standard gossip communication.
    /// </summary>
    public string LocalEndpointHost { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the port where the local peer is listening for standard gossip communication.
    /// </summary>
    public int LocalEndpointPort { get; set; } = 8080;

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
               LocalPeerId.Equals(other.LocalPeerId) &&
               LocalEndpointPort == other.LocalEndpointPort &&
               string.Equals(MulticastAddress, other.MulticastAddress, StringComparison.OrdinalIgnoreCase) &&
               string.Equals(LocalEndpointHost, other.LocalEndpointHost, StringComparison.OrdinalIgnoreCase);
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
            DiscoveryInterval,
            LocalPeerId,
            StringComparer.OrdinalIgnoreCase.GetHashCode(LocalEndpointHost ?? string.Empty),
            LocalEndpointPort);
    }
}