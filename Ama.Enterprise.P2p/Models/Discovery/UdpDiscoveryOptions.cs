namespace Ama.Enterprise.P2p.Models.Discovery;

using System;

/// <summary>
/// Configuration options for UDP multicast peer discovery.
/// </summary>
public sealed record UdpDiscoveryOptions
{
    private TimeSpan discoveryInterval = TimeSpan.FromSeconds(30);
    private TimeSpan discoveryTimeout = TimeSpan.FromSeconds(5);
    private string multicastAddress = "239.0.0.1";

    /// <summary>
    /// Gets or sets the UDP multicast address.
    /// </summary>
    public string MulticastAddress
    {
        get => multicastAddress;
        set => multicastAddress = string.IsNullOrWhiteSpace(value) 
            ? throw new ArgumentException("Multicast address cannot be null or whitespace.", nameof(value)) 
            : value;
    }

    /// <summary>
    /// Gets or sets the UDP multicast port.
    /// </summary>
    public int MulticastPort { get; set; } = 5000;

    /// <summary>
    /// Gets or sets the port number this node advertises to other peers to initiate Phase 2 handshakes.
    /// </summary>
    public int AdvertisedHandshakePort { get; set; }

    /// <summary>
    /// Gets or sets the interval between active discovery multicast broadcasts.
    /// </summary>
    public TimeSpan DiscoveryInterval
    {
        get => discoveryInterval;
        set => discoveryInterval = value > TimeSpan.Zero 
            ? value 
            : throw new ArgumentOutOfRangeException(nameof(value), "Discovery interval must be strictly positive.");
    }

    /// <summary>
    /// Gets or sets the timeout for listening to multicast responses.
    /// </summary>
    public TimeSpan DiscoveryTimeout
    {
        get => discoveryTimeout;
        set => discoveryTimeout = value > TimeSpan.Zero 
            ? value 
            : throw new ArgumentOutOfRangeException(nameof(value), "Discovery timeout must be strictly positive.");
    }
}