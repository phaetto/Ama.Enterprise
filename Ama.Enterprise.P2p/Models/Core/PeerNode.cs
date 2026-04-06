namespace Ama.Enterprise.P2p.Models.Core;
/// <summary>
/// Represents a known peer node in the gossip network, combining its identity and endpoint.
/// </summary>
public readonly record struct PeerNode : IEquatable<PeerNode>
{
    /// <summary>
    /// Gets the unique identifier of the peer.
    /// </summary>
    public PeerId Id { get; init; }

    /// <summary>
    /// Gets the network endpoint of the peer.
    /// </summary>
    public PeerEndpoint Endpoint { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="PeerNode"/> struct.
    /// </summary>
    /// <param name="id">The peer identifier.</param>
    /// <param name="endpoint">The network endpoint.</param>
    public PeerNode(PeerId id, PeerEndpoint endpoint)
    {
        Id = id;
        Endpoint = endpoint;
    }

    /// <inheritdoc />
    public bool Equals(PeerNode other)
    {
        return Id.Equals(other.Id) && Endpoint.Equals(other.Endpoint);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(Id, Endpoint);
    }
}