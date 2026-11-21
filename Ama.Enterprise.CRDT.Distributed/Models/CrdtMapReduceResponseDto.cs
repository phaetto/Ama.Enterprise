namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// P2P RPC response payload returning the pre-combined intermediate accumulator for a remote replica's document batch,
/// along with any missing document IDs for failover retry.
/// </summary>
public sealed record CrdtMapReduceResponseDto : IEquatable<CrdtMapReduceResponseDto>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets the correlation ID matching the originating request.
    /// </summary>
    [JsonPropertyName("c")]
    public Guid CorrelationId { get; init; }

    /// <summary>
    /// Gets a value indicating whether the remote batch execution succeeded.
    /// </summary>
    [JsonPropertyName("s")]
    public bool IsSuccess { get; init; }

    /// <summary>
    /// Gets a value indicating whether an accumulator payload was produced.
    /// </summary>
    [JsonPropertyName("ha")]
    public bool HasAccumulator { get; init; }

    /// <summary>
    /// Gets the number of documents evaluated on the remote replica.
    /// </summary>
    [JsonPropertyName("ec")]
    public int EvaluatedDocumentCount { get; init; }

    /// <summary>
    /// Gets the serialized pre-combined accumulator bytes.
    /// </summary>
    [JsonPropertyName("a")]
    public byte[] SerializedAccumulator { get; init; } = Array.Empty<byte>();

    /// <summary>
    /// Gets the list of document identifiers that were not found on the remote replica.
    /// </summary>
    [JsonPropertyName("m")]
    public IList<string>? MissingDocumentIds { get; init; }

    /// <summary>
    /// Gets an optional error message if the remote execution failed.
    /// </summary>
    [JsonPropertyName("e")]
    public string? ErrorMessage { get; init; }

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(CrdtMapReduceResponseDto? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        var missingEqual = (MissingDocumentIds is null && other.MissingDocumentIds is null) ||
                           (MissingDocumentIds is not null && other.MissingDocumentIds is not null && MissingDocumentIds.SequenceEqual(other.MissingDocumentIds, StringComparer.Ordinal));

        return CorrelationId.Equals(other.CorrelationId) &&
               IsSuccess == other.IsSuccess &&
               HasAccumulator == other.HasAccumulator &&
               EvaluatedDocumentCount == other.EvaluatedDocumentCount &&
               SerializedAccumulator.SequenceEqual(other.SerializedAccumulator) &&
               missingEqual &&
               string.Equals(ErrorMessage, other.ErrorMessage, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(CorrelationId);
        hash.Add(IsSuccess);
        hash.Add(HasAccumulator);
        hash.Add(EvaluatedDocumentCount);
        hash.Add(SerializedAccumulator.Length);
        hash.Add(MissingDocumentIds?.Count ?? 0);
        hash.Add(ErrorMessage, StringComparer.Ordinal);
        return hash.ToHashCode();
    }
}