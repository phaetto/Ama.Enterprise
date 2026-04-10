namespace Ama.Enterprise.P2p.Models.Transports;

using Ama.Enterprise.P2p.Models.Core;

using System;

/// <summary>
/// Represents an HTTP/IP-based network endpoint for peer communication.
/// </summary>
public sealed record HttpPeerEndpoint : PeerEndpoint, IEquatable<HttpPeerEndpoint>
{
    /// <summary>
    /// Gets the IP address or hostname of the peer.
    /// </summary>
    public string Host { get; init; }

    /// <summary>
    /// Gets the network port the peer is listening on.
    /// </summary>
    public int Port { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="HttpPeerEndpoint"/> struct.
    /// </summary>
    /// <param name="host">The host address.</param>
    /// <param name="port">The host port.</param>
    public HttpPeerEndpoint(string host, int port)
    {
        Host = host ?? string.Empty;
        Port = port;
    }

    /// <inheritdoc />
    public bool Equals(HttpPeerEndpoint? other)
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