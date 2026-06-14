namespace Ama.Enterprise.FeatureFlags.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Audit trace structure tracking temporal modifications for a specific feature flag.
/// </summary>
public sealed class FeatureFlagAudit : IEquatable<FeatureFlagAudit>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets or sets the last modified by author.
    /// </summary>
    public string? LastModifiedBy { get; set; }

    /// <summary>
    /// Gets or sets the creation timestamp.
    /// </summary>
    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>
    /// Gets or sets the last updated timestamp.
    /// </summary>
    public DateTimeOffset UpdatedAt { get; set; }

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(FeatureFlagAudit? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return string.Equals(LastModifiedBy, other.LastModifiedBy, StringComparison.Ordinal) &&
               CreatedAt.Equals(other.CreatedAt) &&
               UpdatedAt.Equals(other.UpdatedAt);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as FeatureFlagAudit);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(LastModifiedBy, CreatedAt, UpdatedAt);
}