namespace Ama.Enterprise.P2p.Models.Core;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>
/// Represents a known peer node in the gossip network, combining its identity and endpoint.
/// </summary>
public record struct PeerNode : IEquatable<PeerNode>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets the unique identifier of the peer.
    /// </summary>
    public PeerId Id { get; init; }

    /// <summary>
    /// Gets the network endpoint of the peer.
    /// </summary>
    public PeerEndpoint Endpoint { get; init; }

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="PeerNode"/> struct.
    /// </summary>
    /// <param name="id">The peer identifier.</param>
    /// <param name="endpoint">The network endpoint.</param>
    public PeerNode(PeerId id, PeerEndpoint endpoint)
    {
        Id = id;
        Endpoint = endpoint ?? throw new ArgumentNullException(nameof(endpoint));
        JsonExtensionData = null;
        BinaryExtensionData = null;
    }

    /// <inheritdoc />
    public bool Equals(PeerNode other)
    {
        return Id.Equals(other.Id) && EqualityComparer<PeerEndpoint>.Default.Equals(Endpoint, other.Endpoint);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(Id, Endpoint);
    }
}