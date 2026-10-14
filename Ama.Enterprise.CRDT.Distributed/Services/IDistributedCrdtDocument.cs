namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Defines the generic, non-typed interface for a distributed CRDT document manager.
/// Extensively used by the routing dispatcher and anti-entropy services representing active local matrices.
/// </summary>
public interface IDistributedCrdtDocument
{
    /// <summary>
    /// Gets the shard identifier grouping the causality boundaries for this document natively.
    /// </summary>
    string ShardId { get; }

    /// <summary>
    /// Gets the globally unique identifier for this document type (Topic) within the P2P network.
    /// </summary>
    string DocumentId { get; }

    /// <summary>
    /// Initializes the document, loading initial state from persistent storage if configured natively.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the current local Dotted Version Vector representing the exact synchronization state.
    /// </summary>
    ShardVersionVectorMap GetLocalState();

    /// <summary>
    /// Retrieves a completely materialized snapshot payload superseding local structure dependencies alongside explicitly targeted overarching global bounds.
    /// </summary>
    Task<CrdtSnapshotDataDto> GetSnapshotDataAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Evaluates a remotely forwarded execution intent natively generating local structural operations bypassing generic mapping limits explicitly.
    /// </summary>
    Task<bool> ApplyRemoteIntentAsync(IOperationIntent intent, string targetProperty, IAsyncCrdtPatcher patcher, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a locally generated patch intention over the state and initiates immediate replication if configured natively.
    /// </summary>
    Task ApplyPatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies an incoming remote patch retrieved from a proxy peer to the local document state natively bypassing broadcasting loops.
    /// </summary>
    Task ApplyRemotePatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Applies a patch natively loaded from the storage journal explicitly avoiding asynchronous re-journaling and broadcast triggers.
    /// </summary>
    Task ApplyJournaledPatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a completely materialized snapshot payload superseding local structure dependencies alongside explicitly targeted overarching global bounds.
    /// </summary>
    Task MergeSnapshotAsync(byte[] snapshotData, ShardVersionVectorMap globalState, CancellationToken cancellationToken = default);

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
/// Defines the generic typed interface for a distributed CRDT document manager handling a specific domain model actively processing locally natively.
/// </summary>
public interface IDistributedCrdtDocument<TState> : IDistributedCrdtDocument where TState : class, new()
{
    /// <summary>
    /// Exposes the strictly typed underlying CRDT document containing the application state actively tracked natively.
    /// </summary>
    ValueTask<CrdtDocument<TState>> GetDocumentAsync(CancellationToken cancellationToken = default);
}