namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Data structure representing the isolated response containing the fully materialized state of a requested remote CRDT document.
/// </summary>
public sealed record CrdtReadResponseDto : IEquatable<CrdtReadResponseDto>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets the standard correlation ID mapping directly back to the initial abstract request.
    /// </summary>
    [JsonPropertyName("c")]
    public Guid CorrelationId { get; init; }

    /// <summary>
    /// Gets the serialized, raw materialized state of the CRDT document safely decoupled from metadata.
    /// </summary>
    [JsonPropertyName("s")]
    public byte[] StatePayload { get; init; } = Array.Empty<byte>();

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(CrdtReadResponseDto? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return CorrelationId.Equals(other.CorrelationId) &&
               StatePayload.SequenceEqual(other.StatePayload);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(CorrelationId);
        hash.Add(StatePayload.Length);
        return hash.ToHashCode();
    }
}