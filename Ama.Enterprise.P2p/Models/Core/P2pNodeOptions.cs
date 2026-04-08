namespace Ama.Enterprise.P2p.Models.Core;

using System;
using System.Collections.Generic;

/// <summary>
/// Configuration options for tuning the core node identity across the P2P mesh.
/// </summary>
public sealed class P2pNodeOptions : IEquatable<P2pNodeOptions>
{
    /// <summary>
    /// Gets or sets the unique identifier of the local peer node.
    /// </summary>
    public Guid LocalPeerId { get; set; } = Guid.NewGuid();

    /// <summary>
    /// Gets or sets the protocol-agnostic local endpoint details to share with other peers.
    /// </summary>
    public PeerEndpoint LocalEndpoint { get; set; } = default!;

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

        return LocalPeerId.Equals(other.LocalPeerId) &&
               EqualityComparer<PeerEndpoint>.Default.Equals(LocalEndpoint, other.LocalEndpoint);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as P2pNodeOptions);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(LocalPeerId, LocalEndpoint);
    }
}