namespace Ama.Enterprise.FeatureFlags.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using Ama.CRDT.Attributes.Strategies;

/// <summary>
/// The root state for the feature flags.
/// </summary>
public sealed class FeatureFlagState : IEquatable<FeatureFlagState>
{
    /// <summary>
    /// Gets or sets the document identifier tracking singleton bounds across instances.
    /// </summary>
    public string Id { get; set; } = "ama-enterprise-feature-flags-singleton";

    /// <summary>
    /// A map of feature flags where the key is the flag name.
    /// </summary>
    [CrdtOrMapStrategy]
    public IDictionary<string, FeatureFlag> Flags { get; set; } = new Dictionary<string, FeatureFlag>();

    /// <inheritdoc />
    public bool Equals(FeatureFlagState? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        
        if (Id != other.Id) return false;
        if (Flags.Count != other.Flags.Count) return false;

        foreach (var kvp in Flags)
        {
            if (!other.Flags.TryGetValue(kvp.Key, out var otherFlag)) return false;
            if (!kvp.Value.Equals(otherFlag)) return false;
        }

        return true;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as FeatureFlagState);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Id);
        
        foreach (var kvp in Flags.OrderBy(k => k.Key))
        {
            hash.Add(kvp.Key);
            hash.Add(kvp.Value);
        }
        
        return hash.ToHashCode();
    }
}