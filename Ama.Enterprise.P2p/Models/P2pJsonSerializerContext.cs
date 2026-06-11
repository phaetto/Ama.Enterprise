namespace Ama.Enterprise.P2p.Models;

using Ama.Enterprise.P2p.Models.Algorithms;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Discovery;
using Ama.Enterprise.P2p.Models.Transports;
using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>
/// AOT-friendly JSON context for P2P models.
/// </summary>
[JsonSerializable(typeof(IMeshMessage))]
[JsonSerializable(typeof(GossipMessage))]
[JsonSerializable(typeof(UdpDiscoveryMessage))]
[JsonSerializable(typeof(PeerNode))]
[JsonSerializable(typeof(PeerEndpoint))]
[JsonSerializable(typeof(TcpPeerEndpoint))]
[JsonSerializable(typeof(UdpPeerEndpoint))]
[JsonSerializable(typeof(TcpTransportOptions))]
[JsonSerializable(typeof(UdpTransportOptions))]
[JsonSerializable(typeof(UdpDiscoveryMessage))]
[JsonSerializable(typeof(IDictionary<string, JsonElement>))]
public partial class P2pJsonSerializerContext : JsonSerializerContext
{
}