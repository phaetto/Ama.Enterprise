namespace Ama.Enterprise.FeatureFlags.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Represents a single enterprise feature flag in the system.
/// </summary>
public sealed class FeatureFlag : IEquatable<FeatureFlag>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets or sets the name of the feature flag.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets a value indicating whether the feature flag is enabled.
    /// </summary>
    public bool IsEnabled { get; set; }

    /// <summary>
    /// Gets or sets the metadata.
    /// </summary>
    public FeatureFlagMetadata Metadata { get; set; } = default!;

    /// <summary>
    /// Gets or sets the audit trace.
    /// </summary>
    public FeatureFlagAudit Audit { get; set; } = default!;

    /// <summary>
    /// Gets or sets the ownership details.
    /// </summary>
    public FeatureFlagOwnership Ownership { get; set; } = default!;

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(FeatureFlag? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return string.Equals(Name, other.Name, StringComparison.Ordinal) &&
               IsEnabled == other.IsEnabled &&
               EqualityComparer<FeatureFlagMetadata>.Default.Equals(Metadata, other.Metadata) &&
               EqualityComparer<FeatureFlagAudit>.Default.Equals(Audit, other.Audit) &&
               EqualityComparer<FeatureFlagOwnership>.Default.Equals(Ownership, other.Ownership);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as FeatureFlag);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(Name, IsEnabled, Metadata, Audit, Ownership);
}