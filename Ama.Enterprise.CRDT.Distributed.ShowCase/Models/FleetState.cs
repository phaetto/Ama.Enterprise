namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using Ama.CRDT.Attributes.Strategies;

/// <summary>
/// Root CRDT document model representing fleet devices status.
/// </summary>
public sealed class FleetState : IEquatable<FleetState>
{
    /// <summary>
    /// Gets the singleton identifier for the fleet document.
    /// </summary>
    public string Id { get; init; } = "fleet-singleton";

    /// <summary>
    /// Gets or sets the devices mapped by their identifier.
    /// </summary>
    [CrdtOrMapStrategy]
    public IDictionary<string, DeviceStatus> Devices { get; set; } = new Dictionary<string, DeviceStatus>();

    /// <inheritdoc />
    public bool Equals(FleetState? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        if (Id != other.Id) return false;
        if (Devices.Count != other.Devices.Count) return false;

        foreach (var kvp in Devices)
        {
            if (!other.Devices.TryGetValue(kvp.Key, out var otherVal)) return false;
            if (!kvp.Value.Equals(otherVal)) return false;
        }

        return true;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as FleetState);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Id);
        
        foreach (var kvp in Devices.OrderBy(k => k.Key))
        {
            hash.Add(kvp.Key);
            hash.Add(kvp.Value);
        }
        
        return hash.ToHashCode();
    }
}