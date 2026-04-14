namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

/// <summary>
/// Interface for managing the distributed fleet status CRDT document.
/// </summary>
public interface IFleetManager
{
    /// <summary>
    /// Event triggered when the fleet state changes.
    /// </summary>
    event EventHandler? StateChanged;

    /// <summary>
    /// Retrieves the read-only dictionary of active devices for the specified document.
    /// </summary>
    IReadOnlyDictionary<string, DeviceStatus> GetDevices(string documentId);

    /// <summary>
    /// Adds or updates a device in the fleet state.
    /// </summary>
    Task SetDeviceAsync(string documentId, string id, bool isOnline, int batteryLevel, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a device from the fleet state.
    /// </summary>
    Task RemoveDeviceAsync(string documentId, string id, CancellationToken cancellationToken = default);
}