namespace Ama.Enterprise.FeatureFlags.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Ownership details specifying team contact routing for a feature flag.
/// </summary>
public sealed class FeatureFlagOwnership : IEquatable<FeatureFlagOwnership>, IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets or sets the owner identity.
    /// </summary>
    public string? Owner { get; set; }

    /// <summary>
    /// Gets or sets the owner's contact info.
    /// </summary>
    public string? ContactInfo { get; set; }

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(FeatureFlagOwnership? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return string.Equals(Owner, other.Owner, StringComparison.Ordinal) &&
               string.Equals(ContactInfo, other.ContactInfo, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as FeatureFlagOwnership);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(Owner, ContactInfo);
}