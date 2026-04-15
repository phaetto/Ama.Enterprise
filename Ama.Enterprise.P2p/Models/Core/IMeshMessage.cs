namespace Ama.Enterprise.P2p.Models.Core;

using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Discovery;
using Ama.Enterprise.P2p.Models.Gossip;

/// <summary>
/// Imposes a centralized generic constraint on protocol messages to map their own synchronization mesh identifiers.
/// Configured with AOT-friendly JSON polymorphism to dynamically resolve specific network envelopes at the transport layer.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type", IgnoreUnrecognizedTypeDiscriminators = true, UnknownDerivedTypeHandling = JsonUnknownDerivedTypeHandling.FallBackToBaseType)]
[JsonDerivedType(typeof(GossipMessage), "gossip")]
[JsonDerivedType(typeof(UdpDiscoveryMessage), "udp-discovery")]
public interface IMeshMessage
{
    /// <summary>
    /// Gets the unique identifier of the target P2P mesh network context.
    /// </summary>
    string MeshId { get; }

    /// <summary>
    /// Gets the protocol version of the message ensuring cross-version compatibility across all transports.
    /// </summary>
    string ProtocolVersion { get; }
}