namespace Ama.Enterprise.FeatureFlags.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Metadata structure encapsulating enterprise multi-tenancy and product domain data.
/// </summary>
public sealed class FeatureFlagMetadata : IEquatable<FeatureFlagMetadata>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets or sets the associated product ID.
    /// </summary>
    public string? ProductId { get; set; }

    /// <summary>
    /// Gets or sets the assigned tenant ID.
    /// </summary>
    public string? TenantId { get; set; }

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(FeatureFlagMetadata? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return string.Equals(ProductId, other.ProductId, StringComparison.Ordinal) &&
               string.Equals(TenantId, other.TenantId, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as FeatureFlagMetadata);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(ProductId, TenantId);
}