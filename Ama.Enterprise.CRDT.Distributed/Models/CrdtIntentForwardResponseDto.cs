namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Data structure representing the distinct evaluated result bounds of a remote intent proxy execution mapping success statuses securely.
/// </summary>
public sealed record CrdtIntentForwardResponseDto : IEquatable<CrdtIntentForwardResponseDto>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets the standard correlation ID mapping directly back to the initial abstract request.
    /// </summary>
    [JsonPropertyName("c")]
    public Guid CorrelationId { get; init; }

    /// <summary>
    /// Gets a value indicating whether the remote authoritative shard successfully executed and applied the intent.
    /// </summary>
    [JsonPropertyName("s")]
    public bool IsSuccess { get; init; }

    /// <summary>
    /// Gets an optional error message natively mapped if the execution failed generically.
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
    public bool Equals(CrdtIntentForwardResponseDto? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return CorrelationId.Equals(other.CorrelationId) &&
               IsSuccess == other.IsSuccess &&
               ErrorMessage == other.ErrorMessage;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(CorrelationId, IsSuccess, ErrorMessage);
    }
}