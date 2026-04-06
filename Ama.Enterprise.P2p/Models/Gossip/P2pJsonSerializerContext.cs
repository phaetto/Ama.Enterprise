namespace Ama.Enterprise.P2p.Models.Gossip;

using System.Text.Json.Serialization;

/// <summary>
/// AOT-friendly JSON context for P2P models.
/// </summary>
[JsonSerializable(typeof(GossipMessage))]
public partial class P2pJsonSerializerContext : JsonSerializerContext
{
}