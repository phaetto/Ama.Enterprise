namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// P2P RPC request payload instructing a remote replica to execute the Map and local Combine phases
/// of a registered Map/Reduce job over a batch of documents hosted on that replica.
/// </summary>
public sealed record CrdtMapReduceRequestDto : IEquatable<CrdtMapReduceRequestDto>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets the correlation ID used to match the asynchronous P2P network response.
    /// </summary>
    [JsonPropertyName("c")]
    public Guid CorrelationId { get; init; }

    /// <summary>
    /// Gets the registered wire alias for the Map/Reduce job or query handler.
    /// </summary>
    [JsonPropertyName("j")]
    public string JobAlias { get; init; } = string.Empty;

    /// <summary>
    /// Gets the document type alias targeted by the job.
    /// </summary>
    [JsonPropertyName("dt")]
    public string DocumentTypeAlias { get; init; } = string.Empty;

    /// <summary>
    /// Gets the list of document identifiers to evaluate on the target replica.
    /// </summary>
    [JsonPropertyName("d")]
    public IList<string> DocumentIds { get; init; } = Array.Empty<string>();

    /// <summary>
    /// Gets the serialized input DTO bytes.
    /// </summary>
    [JsonPropertyName("i")]
    public byte[] SerializedInput { get; init; } = Array.Empty<byte>();

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(CrdtMapReduceRequestDto? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        var docsEqual = (DocumentIds is null && other.DocumentIds is null) ||
                        (DocumentIds is not null && other.DocumentIds is not null && DocumentIds.SequenceEqual(other.DocumentIds, StringComparer.Ordinal));

        return CorrelationId.Equals(other.CorrelationId) &&
               string.Equals(JobAlias, other.JobAlias, StringComparison.Ordinal) &&
               string.Equals(DocumentTypeAlias, other.DocumentTypeAlias, StringComparison.Ordinal) &&
               docsEqual &&
               SerializedInput.SequenceEqual(other.SerializedInput);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(CorrelationId);
        hash.Add(JobAlias, StringComparer.Ordinal);
        hash.Add(DocumentTypeAlias, StringComparer.Ordinal);
        hash.Add(DocumentIds?.Count ?? 0);
        hash.Add(SerializedInput.Length);
        return hash.ToHashCode();
    }
}