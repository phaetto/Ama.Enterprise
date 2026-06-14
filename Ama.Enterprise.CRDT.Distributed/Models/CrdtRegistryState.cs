namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Global P2P synced directory state handling distributed multi-document topologies.
/// </summary>
public sealed class CrdtRegistryState : IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets or sets the mapped sequence identifier routing this registry state across P2P limits securely.
    /// </summary>
    public string Id { get; set; } = "system-document-registry";

    /// <summary>
    /// Gets or sets the registry map dynamically identifying active and tombstoned document scopes across the cluster.
    /// </summary>
    public Dictionary<string, CrdtRegistryEntry> Documents { get; set; } = new(StringComparer.Ordinal);

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }
}