namespace Ama.Enterprise.P2p.Models.Discovery;

using System;

/// <summary>
/// Configuration options for DNS-based peer discovery.
/// </summary>
public sealed record DnsDiscoveryOptions
{
    private TimeSpan discoveryInterval = TimeSpan.FromSeconds(30);
    private TimeSpan handshakeTimeout = TimeSpan.FromSeconds(5);
    private string hostname = string.Empty;

    /// <summary>
    /// Gets or sets the DNS hostname to query for peer seeds.
    /// </summary>
    public string Hostname
    {
        get => hostname;
        set => hostname = string.IsNullOrWhiteSpace(value) 
            ? throw new ArgumentException("Hostname cannot be null or whitespace.", nameof(value)) 
            : value;
    }

    /// <summary>
    /// Gets or sets the default port number to associate with resolved IP addresses.
    /// </summary>
    public int Port { get; set; }

    /// <summary>
    /// Gets or sets the interval between active DNS resolutions.
    /// </summary>
    public TimeSpan DiscoveryInterval
    {
        get => discoveryInterval;
        set => discoveryInterval = value > TimeSpan.Zero 
            ? value 
            : throw new ArgumentOutOfRangeException(nameof(value), "Discovery interval must be strictly positive.");
    }

    /// <summary>
    /// Gets or sets the timeout for individual node handshake attempts.
    /// </summary>
    public TimeSpan HandshakeTimeout
    {
        get => handshakeTimeout;
        set => handshakeTimeout = value > TimeSpan.Zero 
            ? value 
            : throw new ArgumentOutOfRangeException(nameof(value), "Handshake timeout must be strictly positive.");
    }
}