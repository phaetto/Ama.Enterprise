namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Text.Json.Serialization;

/// <summary>
/// Represents metadata about an active or tombstoned distributed CRDT document natively mapped via the global cluster registry.
/// </summary>
public readonly record struct CrdtRegistryEntry : IEquatable<CrdtRegistryEntry>
{
    /// <summary>
    /// Gets the unique identifier of the target document.
    /// </summary>
    public string DocumentId { get; }

    /// <summary>
    /// Gets the type alias representing the model state to be dynamically instantiated.
    /// </summary>
    public string TypeAlias { get; }

    /// <summary>
    /// Gets a value indicating whether this document has been explicitly deleted/tombstoned by the cluster.
    /// </summary>
    public bool IsDeleted { get; init; }

    [JsonConstructor]
    public CrdtRegistryEntry(string documentId, string typeAlias, bool isDeleted)
    {
        DocumentId = documentId ?? throw new ArgumentNullException(nameof(documentId));
        TypeAlias = typeAlias ?? throw new ArgumentNullException(nameof(typeAlias));
        IsDeleted = isDeleted;
    }

    /// <inheritdoc />
    public bool Equals(CrdtRegistryEntry other)
    {
        return IsDeleted == other.IsDeleted && 
               string.Equals(DocumentId, other.DocumentId, StringComparison.Ordinal) && 
               string.Equals(TypeAlias, other.TypeAlias, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(DocumentId, TypeAlias, IsDeleted);
    }
}