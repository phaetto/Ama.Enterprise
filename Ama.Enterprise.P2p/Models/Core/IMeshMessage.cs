namespace Ama.Enterprise.P2p.Models.Core;

using System;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Algorithms;

/// <summary>
/// Imposes a centralized generic constraint on protocol messages to map their own synchronization mesh identifiers.
/// Configured with AOT-friendly JSON polymorphism to dynamically resolve specific network envelopes at the transport layer.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type", IgnoreUnrecognizedTypeDiscriminators = true, UnknownDerivedTypeHandling = JsonUnknownDerivedTypeHandling.FallBackToBaseType)]
[JsonDerivedType(typeof(GossipMessage), "gossip")]
public interface IMeshMessage : IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets the unique identifier of the target P2P mesh network context.
    /// </summary>
    string MeshId { get; }

    /// <summary>
    /// Gets the protocol version of the message ensuring cross-version compatibility across all transports.
    /// </summary>
    string ProtocolVersion { get; }

    /// <summary>
    /// Gets the unique identifier of the generic message ensuring native global tracking bounds and deduplication.
    /// </summary>
    Guid MessageId => Guid.NewGuid();

    /// <summary>
    /// Gets the identifier of the peer that originally created this message explicitly bounding network origins.
    /// </summary>
    PeerId SenderId { get; }

    /// <summary>
    /// Gets the underlying generically mapped application bytes decoupled from overarching network mechanisms.
    /// </summary>
    ReadOnlyMemory<byte> Payload => ReadOnlyMemory<byte>.Empty;
}