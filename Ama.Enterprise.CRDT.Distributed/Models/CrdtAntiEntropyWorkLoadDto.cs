namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;

/// <summary>
/// Data structure representing the current synchronization backlog and pipeline queue depth for a local CRDT replica scope.
/// Used by the anti-entropy service to throttle or scale intervals when thousands of operations or commands are in flight.
/// </summary>
public readonly record struct CrdtAntiEntropyWorkLoadDto : IEquatable<CrdtAntiEntropyWorkLoadDto>
{
    /// <summary>
    /// Gets a value indicating whether an anti-entropy synchronization or pipeline drain is currently busy.
    /// </summary>
    public bool IsBusy { get; init; }

    /// <summary>
    /// Gets the total number of commands currently queued across the document pipeline and orchestrator channels.
    /// </summary>
    public int PendingQueueDepth { get; init; }

    /// <summary>
    /// Gets the estimated number of causal version operations that remain to be synchronized across known peers.
    /// </summary>
    public long EstimatedPendingSyncItems { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="CrdtAntiEntropyWorkLoadDto"/> struct.
    /// </summary>
    /// <param name="isBusy">Whether the local pipeline or anti-entropy loop is busy.</param>
    /// <param name="pendingQueueDepth">The current channel queue depth.</param>
    /// <param name="estimatedPendingSyncItems">The estimated number of pending causal operations across peers.</param>
    public CrdtAntiEntropyWorkLoadDto(
        bool isBusy,
        int pendingQueueDepth,
        long estimatedPendingSyncItems)
    {
        IsBusy = isBusy;
        PendingQueueDepth = pendingQueueDepth;
        EstimatedPendingSyncItems = estimatedPendingSyncItems;
    }

    /// <inheritdoc />
    public bool Equals(CrdtAntiEntropyWorkLoadDto other)
    {
        return IsBusy == other.IsBusy &&
               PendingQueueDepth == other.PendingQueueDepth &&
               EstimatedPendingSyncItems == other.EstimatedPendingSyncItems;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(IsBusy, PendingQueueDepth, EstimatedPendingSyncItems);
    }
}