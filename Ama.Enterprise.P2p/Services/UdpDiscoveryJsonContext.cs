namespace Ama.Enterprise.P2p.Services;

using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models;

/// <summary>
/// Source-generated JSON serialization context for AOT-friendly serialization of UDP discovery models.
/// </summary>
[JsonSerializable(typeof(PeerNode))]
internal sealed partial class UdpDiscoveryJsonContext : JsonSerializerContext
{
}