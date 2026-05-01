namespace Ama.Enterprise.P2p.Models.Discovery;

using System;

/// <summary>
/// Configuration options for the isolated unicast UDP handshaker.
/// </summary>
public sealed record UdpHandshakeOptions
{
    private TimeSpan handshakeTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the local UDP port to listen for incoming handshake requests.
    /// </summary>
    public int ListenPort { get; set; } = 8081;

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