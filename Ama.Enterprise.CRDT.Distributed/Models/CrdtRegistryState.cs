namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Global P2P synced directory state handling distributed multi-document topologies.
/// </summary>
public sealed class CrdtRegistryState : IExtensibleDistributedPayload, IEquatable<CrdtRegistryState>
{
    /// <summary>
    /// Gets or sets the mapped sequence identifier routing this registry state across P2P limits.
    /// </summary>
    public string Id { get; set; } = Constants.SystemRegistryDocumentId;

    /// <summary>
    /// Gets or sets the registry map dynamically identifying active and tombstoned document scopes across the cluster.
    /// </summary>
    public ConcurrentDictionary<string, CrdtRegistryEntry> Documents { get; set; } = new(StringComparer.Ordinal);

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    /// <inheritdoc />
    public bool Equals(CrdtRegistryState? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        if (!string.Equals(Id, other.Id, StringComparison.Ordinal)) return false;
        if (Documents.Count != other.Documents.Count) return false;

        foreach (var kvp in Documents)
        {
            if (!other.Documents.TryGetValue(kvp.Key, out var otherValue) || !kvp.Value.Equals(otherValue))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as CrdtRegistryState);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Id, StringComparer.Ordinal);
        hash.Add(Documents.Count);
        return hash.ToHashCode();
    }
}