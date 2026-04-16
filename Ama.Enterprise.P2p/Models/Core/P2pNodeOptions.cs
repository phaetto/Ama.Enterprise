namespace Ama.Enterprise.P2p.Models.Core;

using System;

/// <summary>
/// Configuration options for tuning the core node identity across the P2P mesh.
/// </summary>
public sealed class P2pNodeOptions : IEquatable<P2pNodeOptions>
{
    private static readonly Guid ProcessGlobalPeerId = Guid.NewGuid();

    /// <summary>
    /// Gets or sets the unique identifier of the local peer node. Defaults to a process-wide unique identifier.
    /// </summary>
    public Guid LocalPeerId { get; set; } = ProcessGlobalPeerId;

    /// <inheritdoc />
    public bool Equals(P2pNodeOptions? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return LocalPeerId.Equals(other.LocalPeerId);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as P2pNodeOptions);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(LocalPeerId);
    }
}