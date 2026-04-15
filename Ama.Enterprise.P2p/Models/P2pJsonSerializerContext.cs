namespace Ama.Enterprise.P2p.Models.Gossip;

using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Discovery;
using Ama.Enterprise.P2p.Models.Transports;

/// <summary>
/// AOT-friendly JSON context for P2P models.
/// </summary>
[JsonSerializable(typeof(IMeshMessage))]
[JsonSerializable(typeof(GossipMessage))]
[JsonSerializable(typeof(UdpDiscoveryMessage))]
[JsonSerializable(typeof(PeerNode))]
[JsonSerializable(typeof(PeerEndpoint))]
[JsonSerializable(typeof(HttpPeerEndpoint))]
public partial class P2pJsonSerializerContext : JsonSerializerContext
{
}