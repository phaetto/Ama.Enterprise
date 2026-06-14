namespace Ama.Enterprise.P2p.Telemetry.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// AOT friendly data structure identifying distinct multidimensional metrics mapping attributes safely.
/// </summary>
public record struct MetricTagDto(string Key, string Value) : IEquatable<MetricTagDto>, IExtensibleDistributedPayload
{
    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(MetricTagDto other)
    {
        return string.Equals(Key, other.Key, StringComparison.Ordinal) && 
               string.Equals(Value, other.Value, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(Key, Value);
    }
}