namespace Ama.Enterprise.P2p.Models.Discovery;

using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Wraps peer node discovery information tightly bound to its target mesh explicitly safely bridging shared UDP topologies accurately.
/// </summary>
/// <param name="MeshId">The explicit mesh identifier context targeted inherently by the payload.</param>
/// <param name="ProtocolVersion">The explicit protocol version validating message compatibility across any transport.</param>
/// <param name="Node">The identified active P2P node data organically discovered gracefully.</param>
public sealed record UdpDiscoveryMessage(string MeshId, string ProtocolVersion, PeerNode Node) : IMeshMessage;