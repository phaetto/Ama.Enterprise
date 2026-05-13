namespace Ama.Enterprise.P2p.Services.Discovery;

using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Discovery;

/// <summary>
/// Source-generated JSON serialization context for AOT-friendly serialization of UDP discovery models.
/// </summary>
[JsonSerializable(typeof(PeerNode))]
[JsonSerializable(typeof(PeerEndpoint))]
[JsonSerializable(typeof(UdpDiscoveryMessage))]
internal sealed partial class UdpDiscoveryJsonContext : JsonSerializerContext
{
}