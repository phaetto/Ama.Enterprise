namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;

/// <summary>
/// Holds the logical scope identifier, host replica identifier, and composite CRDT replica identifier for an isolated scope.
/// </summary>
public sealed class CrdtScopeContext : IEquatable<CrdtScopeContext>
{
    /// <summary>
    /// Gets or sets the logical scope or compartment identifier.
    /// </summary>
    public string ScopeId { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the physical host replica identifier used to bind P2P meshes.
    /// </summary>
    public string HostReplicaId { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the unique CRDT replica identifier used in version vectors for this scope.
    /// </summary>
    public string ReplicaId { get; set; } = string.Empty;

    /// <inheritdoc />
    public bool Equals(CrdtScopeContext? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return string.Equals(ScopeId, other.ScopeId, StringComparison.Ordinal) &&
               string.Equals(HostReplicaId, other.HostReplicaId, StringComparison.Ordinal) &&
               string.Equals(ReplicaId, other.ReplicaId, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as CrdtScopeContext);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(ScopeId, HostReplicaId, ReplicaId);
}