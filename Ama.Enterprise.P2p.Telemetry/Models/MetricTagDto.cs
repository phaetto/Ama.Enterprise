namespace Ama.Enterprise.P2p.Telemetry.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.CRDT.Services.Pooling;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// AOT friendly data structure identifying distinct multidimensional metrics mapping attributes.
/// </summary>
public record struct MetricTagDto : IEquatable<MetricTagDto>, IExtensibleDistributedPayload
{
    private readonly string key = string.Empty;
    private readonly string value = string.Empty;

    /// <summary>
    /// Initializes a new instance of the <see cref="MetricTagDto"/> struct.
    /// </summary>
    /// <param name="key">The tag key.</param>
    /// <param name="value">The tag value.</param>
    [JsonConstructor]
    public MetricTagDto(string key, string value)
    {
        this.key = CrdtStringPool.Shared.GetOrAdd(key ?? string.Empty);
        this.value = CrdtStringPool.Shared.GetOrAdd(value ?? string.Empty);
    }

    /// <summary>
    /// Gets or initializes the tag key.
    /// </summary>
    public string Key
    {
        get => key;
        init => key = CrdtStringPool.Shared.GetOrAdd(value ?? string.Empty);
    }

    /// <summary>
    /// Gets or initializes the tag value.
    /// </summary>
    public string Value
    {
        get => value;
        init => this.value = CrdtStringPool.Shared.GetOrAdd(value ?? string.Empty);
    }

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