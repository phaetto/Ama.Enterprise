namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Data structure encapsulating a targeted generic intent forwarded directly bounding execution on a remote authoritative CRDT shard.
/// </summary>
public sealed record CrdtIntentForwardRequestDto : IEquatable<CrdtIntentForwardRequestDto>, IExtensibleDistributedPayload
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
    /// Gets the serialized raw bytes representing the specific P2P intent payload to be applied.
    /// </summary>
    [JsonPropertyName("i")]
    public byte[] SerializedIntent { get; init; } = Array.Empty<byte>();

    /// <summary>
    /// Gets the string alias identifying the intent type dynamically for AOT generic resolutions.
    /// </summary>
    [JsonPropertyName("t")]
    public string IntentTypeAlias { get; init; } = string.Empty;

    /// <summary>
    /// Gets the optional target property path string guiding the structural node applicator.
    /// </summary>
    [JsonPropertyName("p")]
    public string TargetProperty { get; init; } = string.Empty;

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(CrdtIntentForwardRequestDto? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return CorrelationId.Equals(other.CorrelationId) &&
               DocumentId == other.DocumentId &&
               SerializedIntent.SequenceEqual(other.SerializedIntent) &&
               IntentTypeAlias == other.IntentTypeAlias &&
               TargetProperty == other.TargetProperty;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(CorrelationId);
        hash.Add(DocumentId);
        hash.Add(SerializedIntent.Length);
        hash.Add(IntentTypeAlias);
        hash.Add(TargetProperty);
        return hash.ToHashCode();
    }
}