namespace Ama.Enterprise.P2p.Models.Discovery;

/// <summary>
/// Represents a lightweight Phase 1 multicast payload used to discover available peer IPs.
/// </summary>
public sealed record UdpDiscoveryMessage
{
    /// <summary>
    /// Gets or sets the target mesh identifier preventing cross-talk across environments.
    /// </summary>
    public string MeshId { get; init; } = string.Empty;

    /// <summary>
    /// Gets or sets the explicitly advertised port where the active handshaker is listening.
    /// </summary>
    public int AdvertisedHandshakePort { get; init; }
}