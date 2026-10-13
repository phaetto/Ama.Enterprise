namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Services;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Models;

/// <summary>
/// Interface for managing the distributed fleet status CRDT document.
/// </summary>
public interface IFleetManager
{
    /// <summary>
    /// Retrieves the read-only dictionary of active devices for the specified document.
    /// </summary>
    /// <param name="documentId">The target document ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    ValueTask<IReadOnlyDictionary<string, DeviceStatus>> GetDevicesAsync(string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds or updates a device in the fleet state.
    /// </summary>
    Task SetDeviceAsync(string documentId, string id, bool isOnline, int batteryLevel, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a device from the fleet state.
    /// </summary>
    Task RemoveDeviceAsync(string documentId, string id, CancellationToken cancellationToken = default);
}