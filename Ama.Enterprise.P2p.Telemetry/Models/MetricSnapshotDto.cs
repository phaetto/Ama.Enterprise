namespace Ama.Enterprise.P2p.Telemetry.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.CRDT.Services.Pooling;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Immutable payload holding flattened telemetry captures strictly ensuring AOT constraints natively decoupled from reflection SDK parameters.
/// </summary>
public sealed record MetricSnapshotDto : IEquatable<MetricSnapshotDto>, IExtensibleDistributedPayload
{
    private readonly string name = string.Empty;
    private readonly string type = string.Empty;

    /// <summary>
    /// Initializes a new instance of the <see cref="MetricSnapshotDto"/> class.
    /// </summary>
    public MetricSnapshotDto()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MetricSnapshotDto"/> class.
    /// </summary>
    /// <param name="name">The metric name.</param>
    /// <param name="type">The metric type.</param>
    /// <param name="value">The metric value.</param>
    /// <param name="tags">The metric tags.</param>
    public MetricSnapshotDto(string name, string type, long value, IReadOnlyList<MetricTagDto>? tags = null)
    {
        this.name = CrdtStringPool.Shared.GetOrAdd(name ?? string.Empty);
        this.type = CrdtStringPool.Shared.GetOrAdd(type ?? string.Empty);
        Value = value;
        Tags = tags ?? Array.Empty<MetricTagDto>();
    }

    /// <summary>
    /// Name representing the distinct mapped instrument constraint.
    /// </summary>
    public string Name
    {
        get => name;
        init => name = CrdtStringPool.Shared.GetOrAdd(value ?? string.Empty);
    }

    /// <summary>
    /// Identifies the primitive evaluation type tracked internally resolving histogram or counters.
    /// </summary>
    public string Type
    {
        get => type;
        init => type = CrdtStringPool.Shared.GetOrAdd(value ?? string.Empty);
    }

    /// <summary>
    /// Total aggregated sum or explicit absolute gauge evaluating measured attributes natively.
    /// </summary>
    public long Value { get; init; }

    /// <summary>
    /// List defining exact metadata properties filtering explicitly evaluated metric scopes.
    /// </summary>
    public IReadOnlyList<MetricTagDto> Tags { get; init; } = Array.Empty<MetricTagDto>();

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(MetricSnapshotDto? other)
    {
        if (other is null)
        {
            return false;
        }

        if (!string.Equals(Name, other.Name, StringComparison.Ordinal) || 
            !string.Equals(Type, other.Type, StringComparison.Ordinal) || 
            Value != other.Value)
        {
            return false;
        }

        if (Tags.Count != other.Tags.Count)
        {
            return false;
        }

        for (int i = 0; i < Tags.Count; i++)
        {
            if (!Tags[i].Equals(other.Tags[i]))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Name);
        hash.Add(Type);
        hash.Add(Value);

        foreach (var tag in Tags)
        {
            hash.Add(tag);
        }

        return hash.ToHashCode();
    }
}