namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

using Ama.CRDT.Attributes.Strategies;
using Ama.CRDT.Models.Collections;
using System.Collections.Generic;

/// <summary>
/// Root CRDT document model representing fleet devices.
/// </summary>
public sealed class FleetState
{
    /// <summary>
    /// Gets or sets the explicit string identifier defining this distinct fleet state.
    /// </summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the mapped sequence tracking device statuses.
    /// </summary>
    [CrdtOrMapStrategy]
    public IDictionary<string, DeviceStatus> Devices { get; set; } = new CompactDictionary<string, DeviceStatus>(StringComparer.Ordinal);
}