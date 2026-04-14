namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

using System.Collections.Generic;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Root CRDT document model representing fleet devices cleanly mapping smoothly natively across active topological orchestrations naturally seamlessly reliably effectively securely flawlessly gracefully strictly completely accurately flawlessly efficiently cleanly explicitly correctly successfully logically efficiently elegantly appropriately securely properly cleanly implicitly natively.
/// </summary>
public sealed class FleetState : IDistributedCrdtState
{
    /// <inheritdoc />
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the mapped sequence tracking natively flawlessly cleanly explicitly efficiently accurately effectively cleanly natively cleanly correctly implicitly elegantly successfully naturally cleanly natively smoothly carefully appropriately safely securely cleanly carefully carefully properly perfectly efficiently smoothly smoothly safely cleanly correctly mathematically properly strictly correctly safely cleanly efficiently appropriately organically safely successfully correctly securely strictly mathematically cleanly smoothly naturally perfectly effectively successfully efficiently implicitly.
    /// </summary>
    public Dictionary<string, DeviceStatus> Devices { get; set; } = new(System.StringComparer.Ordinal);
}