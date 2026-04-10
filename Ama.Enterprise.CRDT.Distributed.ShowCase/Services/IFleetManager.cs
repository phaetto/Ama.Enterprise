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
    /// Event triggered whenever the fleet document state has structurally changed.
    /// </summary>
    event EventHandler? StateChanged;

    /// <summary>
    /// Gets the current read-only dictionary of distributed device statuses.
    /// </summary>
    IReadOnlyDictionary<string, DeviceStatus> GetDevices();

    /// <summary>
    /// Sets or updates a device status in the distributed document.
    /// </summary>
    Task SetDeviceAsync(string id, bool isOnline, int batteryLevel, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a device from the distributed document.
    /// </summary>
    Task RemoveDeviceAsync(string id, CancellationToken cancellationToken = default);
}