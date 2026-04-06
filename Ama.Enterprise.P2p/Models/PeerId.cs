namespace Ama.Enterprise.P2p.Models;

/// <summary>
/// Represents a unique identifier for a peer in the gossip network.
/// </summary>
public readonly record struct PeerId : IEquatable<PeerId>
{
    /// <summary>
    /// Gets the globally unique identifier of the peer.
    /// </summary>
    public Guid Value { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="PeerId"/> struct.
    /// </summary>
    /// <param name="value">The underlying GUID value.</param>
    public PeerId(Guid value)
    {
        Value = value;
    }

    /// <inheritdoc />
    public bool Equals(PeerId other)
    {
        return Value.Equals(other.Value);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return Value.GetHashCode();
    }
}