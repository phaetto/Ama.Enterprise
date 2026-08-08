namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Defines the generic, non-typed interface for a distributed CRDT document manager.
/// Extensively used by the routing dispatcher and anti-entropy services.
/// </summary>
public interface IDistributedCrdtDocument
{
    /// <summary>
    /// Gets the globally unique identifier for this document type (Topic) within the P2P network.
    /// </summary>
    string DocumentId { get; }

    /// <summary>
    /// Fired when a new patch has been generated locally and is ready to be broadcasted to remote peers.
    /// </summary>
    event EventHandler<CrdtPatch>? PatchGenerated;

    /// <summary>
    /// Initializes the document, loading initial state from persistent storage if configured natively.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the current local Dotted Version Vector representing the exact synchronization state.
    /// </summary>
    DottedVersionVector GetLocalState();

    /// <summary>
    /// Retrieves a completely materialized snapshot payload superseding local structure dependencies alongside explicitly targeted overarching global bounds.
    /// </summary>
    Task<CrdtSnapshotDataDto> GetSnapshotDataAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies incoming operations retrieved from a remote replica to the local document state.
    /// </summary>
    Task ApplyOperationsAsync(IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Applies operations natively loaded from the storage journal explicitly avoiding asynchronous re-journaling and broadcast triggers.
    /// </summary>
    Task ApplyJournaledOperationsAsync(IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a completely materialized snapshot payload superseding local structure dependencies alongside explicitly targeted overarching global bounds.
    /// </summary>
    Task MergeSnapshotAsync(byte[] snapshotData, DottedVersionVector globalState, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously saves the current in-memory document state to persistent storage natively.
    /// Does not directly manage overarching DVV mapping updates avoiding structural write-ahead gaps.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous checkpoint operation.</returns>
    Task CheckpointAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Evicts the state and metadata of a specific peer replica from this document's internal tracker.
    /// </summary>
    /// <param name="replicaId">The identifier of the remote replica to evict.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task EvictReplicaAsync(string replicaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resets the internal CRDT metadata and structures completely, used when forced to re-bootstrap identity following a cluster tombstone eviction.
    /// </summary>
    /// <param name="oldReplicaId">The pre-reboot replica ID allowing offline operation continuity to map correctly during the incoming fallback snapshot overwrite.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ResetLocalStateAsync(string oldReplicaId, CancellationToken cancellationToken = default);
}

/// <summary>
/// Defines the generic typed interface for a distributed CRDT document manager handling a specific domain model.
/// </summary>
public interface IDistributedCrdtDocument<TState> : IDistributedCrdtDocument where TState : class, new()
{
    /// <summary>
    /// Triggered when the inner document state has structurally changed (either via local intent or remote operations).
    /// </summary>
    event EventHandler? StateChanged;

    /// <summary>
    /// Exposes the strictly typed underlying CRDT document containing the application state.
    /// </summary>
    CrdtDocument<TState> Document { get; }

    /// <summary>
    /// Applies a locally generated patch intention over the state and initiates immediate replication if configured.
    /// </summary>
    Task ApplyPatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default);
}