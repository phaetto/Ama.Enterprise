namespace Ama.Enterprise.P2p.Kestrel.Models;

using System;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Represents an HTTP/IP-based network endpoint for peer communication handled by Kestrel.
/// </summary>
public sealed record KestrelPeerEndpoint : PeerEndpoint, IEquatable<KestrelPeerEndpoint>
{
    /// <summary>
    /// Gets the IP address or hostname of the peer.
    /// </summary>
    public string Host { get; init; }

    /// <summary>
    /// Gets the network port the peer is listening on via Kestrel.
    /// </summary>
    public int Port { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="KestrelPeerEndpoint"/> class.
    /// </summary>
    /// <param name="host">The host address.</param>
    /// <param name="port">The host port.</param>
    public KestrelPeerEndpoint(string host, int port)
    {
        Host = host ?? string.Empty;
        Port = port;
    }

    /// <inheritdoc />
    public bool Equals(KestrelPeerEndpoint? other)
    {
        if (other is null) return false;
        return string.Equals(Host, other.Host, StringComparison.OrdinalIgnoreCase) && Port == other.Port;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(StringComparer.OrdinalIgnoreCase.GetHashCode(Host), Port);
    }
}