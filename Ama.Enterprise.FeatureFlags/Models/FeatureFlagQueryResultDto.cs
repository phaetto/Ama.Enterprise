namespace Ama.Enterprise.FeatureFlags.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Query result DTO containing aggregated feature flags collected across cluster document shards.
/// </summary>
public sealed class FeatureFlagQueryResultDto : IEquatable<FeatureFlagQueryResultDto>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets or sets the aggregated feature flags map.
    /// </summary>
    public IDictionary<string, FeatureFlag> Flags { get; set; } = new Dictionary<string, FeatureFlag>();

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(FeatureFlagQueryResultDto? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        if (Flags.Count != other.Flags.Count) return false;

        foreach (var kvp in Flags)
        {
            if (!other.Flags.TryGetValue(kvp.Key, out var otherFlag)) return false;
            if (!kvp.Value.Equals(otherFlag)) return false;
        }

        return true;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as FeatureFlagQueryResultDto);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var kvp in Flags.OrderBy(k => k.Key))
        {
            hash.Add(kvp.Key);
            hash.Add(kvp.Value);
        }

        return hash.ToHashCode();
    }
}