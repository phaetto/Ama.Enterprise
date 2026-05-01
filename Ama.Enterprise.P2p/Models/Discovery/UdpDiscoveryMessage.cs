namespace Ama.Enterprise.P2p.Models.Discovery;

/// <summary>
/// Represents a lightweight Phase 1 multicast payload used to discover available peer IPs and Phase 2 routing ports.
/// </summary>
public sealed record UdpDiscoveryMessage
{
    /// <summary>
    /// Gets or sets the target mesh identifier preventing cross-talk across environments.
    /// </summary>
    public string MeshId { get; init; } = string.Empty;

    /// <summary>
    /// Gets or sets the specific port the originating node is actively listening on for Phase 2 handshakes.
    /// </summary>
    public int HandshakePort { get; init; }
}