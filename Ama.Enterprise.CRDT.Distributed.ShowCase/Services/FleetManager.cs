namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

/// <summary>
/// Implementation handling intentions and queries for the fleet document.
/// </summary>
public sealed class FleetManager : IFleetManager
{
    private readonly IDistributedCrdtDocument<FleetState> documentManager;
    private readonly ICrdtPatcher patcher;
    private readonly object syncRoot = new();

    /// <inheritdoc />
    public event EventHandler? StateChanged;

    public FleetManager(
        IDistributedCrdtDocument<FleetState> documentManager,
        ICrdtPatcher patcher)
    {
        this.documentManager = documentManager ?? throw new ArgumentNullException(nameof(documentManager));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));

        this.documentManager.StateChanged += (sender, args) => StateChanged?.Invoke(this, args);
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, DeviceStatus> GetDevices()
    {
        lock (syncRoot)
        {
            return new ReadOnlyDictionary<string, DeviceStatus>(documentManager.Document.Data.Devices);
        }
    }

    /// <inheritdoc />
    public async Task SetDeviceAsync(string id, bool isOnline, int batteryLevel, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var item = new DeviceStatus(id, isOnline, batteryLevel);
        var operation = patcher.GenerateOperation(documentManager.Document, x => x.Devices, new MapSetIntent(id, item));
        var patch = new CrdtPatch(new[] { operation });

        await documentManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RemoveDeviceAsync(string id, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Id cannot be null or empty.", nameof(id));

        var operation = patcher.GenerateOperation(documentManager.Document, x => x.Devices, new MapRemoveIntent(id));
        var patch = new CrdtPatch(new[] { operation });

        await documentManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
    }
}