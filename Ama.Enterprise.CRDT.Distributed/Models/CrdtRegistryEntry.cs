namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.CRDT.Services.Pooling;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Represents metadata about an active or tombstoned distributed CRDT document.
/// </summary>
public sealed record CrdtRegistryEntry : IEquatable<CrdtRegistryEntry>, IExtensibleDistributedPayload
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

    /// <summary>
    /// Formats a composite registry key combining the type alias and document identifier.
    /// </summary>
    /// <param name="typeAlias">The document type alias.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <returns>The composite key string.</returns>
    public static string GetRegistryKey(string? typeAlias, string documentId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);
        documentId = CrdtStringPool.Shared.GetOrAdd(documentId);

        if (string.IsNullOrWhiteSpace(typeAlias))
        {
            return documentId;
        }

        typeAlias = CrdtStringPool.Shared.GetOrAdd(typeAlias);
        return CrdtStringPool.Shared.GetOrAdd($"{typeAlias}:{documentId}");
    }

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    [JsonConstructor]
    public CrdtRegistryEntry(string documentId, string typeAlias, bool isDeleted)
    {
        ArgumentNullException.ThrowIfNull(documentId);
        ArgumentNullException.ThrowIfNull(typeAlias);

        DocumentId = CrdtStringPool.Shared.GetOrAdd(documentId);
        TypeAlias = CrdtStringPool.Shared.GetOrAdd(typeAlias);
        IsDeleted = isDeleted;
    }

    /// <inheritdoc />
    public bool Equals(CrdtRegistryEntry? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        
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