namespace Ama.Enterprise.P2p.Models.Discovery;

using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Wraps peer node discovery information tightly bound to its target mesh.
/// </summary>
/// <param name="MeshId">The explicit mesh identifier context targeted by the payload.</param>
/// <param name="ProtocolVersion">The protocol version validating message compatibility across any transport.</param>
/// <param name="Node">The identified active P2P node data.</param>
public sealed record UdpDiscoveryMessage(string MeshId, string ProtocolVersion, PeerNode Node) : IMeshMessage;