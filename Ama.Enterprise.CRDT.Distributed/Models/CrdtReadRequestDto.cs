namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Data structure representing a decoupled request to extract the materialized state of a remote CRDT document asynchronously.
/// </summary>
public sealed record CrdtReadRequestDto : IEquatable<CrdtReadRequestDto>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets the standard correlation ID utilized to map the asynchronous P2P network response natively.
    /// </summary>
    [JsonPropertyName("c")]
    public Guid CorrelationId { get; init; }

    /// <summary>
    /// Gets the explicitly bounded unique identifier of the target CRDT document instance.
    /// </summary>
    [JsonPropertyName("d")]
    public string DocumentId { get; init; } = string.Empty;

    /// <summary>
    /// Gets the optional document type alias used for lazy shard instantiation when outracing registry gossip.
    /// </summary>
    [JsonPropertyName("dt")]
    public string DocumentTypeAlias { get; init; } = string.Empty;

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(CrdtReadRequestDto? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return CorrelationId.Equals(other.CorrelationId) &&
               DocumentId == other.DocumentId &&
               DocumentTypeAlias == other.DocumentTypeAlias;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(CorrelationId, DocumentId, DocumentTypeAlias);
    }
}