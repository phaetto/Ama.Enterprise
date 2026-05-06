namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;

/// <summary>
/// Global P2P synced directory state handling distributed multi-document topologies.
/// </summary>
public sealed class CrdtRegistryState
{
    /// <summary>
    /// Gets or sets the mapped sequence identifier routing this registry state across P2P limits securely.
    /// </summary>
    public string Id { get; set; } = "system-document-registry";

    /// <summary>
    /// Gets or sets the registry map dynamically identifying active and tombstoned document scopes across the cluster.
    /// </summary>
    public Dictionary<string, CrdtRegistryEntry> Documents { get; set; } = new(StringComparer.Ordinal);
}