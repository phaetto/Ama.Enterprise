namespace Ama.Enterprise.P2p.Models.Core;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Transports;

/// <summary>
/// Represents the abstract base network address where a peer can be reached.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type", IgnoreUnrecognizedTypeDiscriminators = true, UnknownDerivedTypeHandling = JsonUnknownDerivedTypeHandling.FallBackToBaseType)]
[JsonDerivedType(typeof(TcpPeerEndpoint), "tcp-peer-endpoint")]
[JsonDerivedType(typeof(UdpPeerEndpoint), "udp-peer-endpoint")]
public abstract record PeerEndpoint : IEquatable<PeerEndpoint>, IExtensibleDistributedPayload
{
    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement> JsonExtensionData { get; set; } = new Dictionary<string, JsonElement>();

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>> BinaryExtensionData { get; set; } = new List<ReadOnlyMemory<byte>>();
}