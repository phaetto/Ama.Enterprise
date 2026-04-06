namespace Ama.Enterprise.P2p.Models;

/// <summary>
/// Represents the network address and port where a peer can be reached.
/// </summary>
public readonly record struct PeerEndpoint : IEquatable<PeerEndpoint>
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
    /// Initializes a new instance of the <see cref="PeerEndpoint"/> struct.
    /// </summary>
    /// <param name="host">The host address.</param>
    /// <param name="port">The host port.</param>
    public PeerEndpoint(string host, int port)
    {
        Host = host ?? string.Empty;
        Port = port;
    }

    /// <inheritdoc />
    public bool Equals(PeerEndpoint other)
    {
        return string.Equals(Host, other.Host, StringComparison.OrdinalIgnoreCase) && Port == other.Port;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(StringComparer.OrdinalIgnoreCase.GetHashCode(Host), Port);
    }
}