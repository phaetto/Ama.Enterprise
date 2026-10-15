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
/// <example>
/// <code>
/// await doc.InitializeAsync(cancellationToken);
/// await doc.ApplyRemoteIntentAsync(intent, "Items", patcher, IntentDeliveryMode.Acknowledged, cancellationToken);
/// </code>
/// </example>
public interface IDistributedCrdtDocument
{
    /// <summary>
    /// Gets the shard identifier grouping the causality boundaries for this document.
    /// </summary>
    string ShardId { get; }

    /// <summary>
    /// Gets the globally unique identifier for this document type (Topic) within the P2P network.
    /// </summary>
    string DocumentId { get; }

    /// <summary>
    /// Initializes the document, loading initial state from persistent storage if configured.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the current local Dotted Version Vector representing the synchronization state.
    /// </summary>
    ShardVersionVectorMap GetLocalState();

    /// <summary>
    /// Retrieves a materialized snapshot payload superseding local structure dependencies alongside targeted global bounds.
    /// </summary>
    Task<CrdtSnapshotDataDto> GetSnapshotDataAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Evaluates a remotely forwarded execution intent generating local structural operations.
    /// </summary>
    Task<bool> ApplyRemoteIntentAsync(IOperationIntent intent, string targetProperty, IAsyncCrdtPatcher patcher, IntentDeliveryMode deliveryMode = IntentDeliveryMode.Acknowledged, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a locally generated patch intention over the state and initiates immediate replication if configured.
    /// </summary>
    Task ApplyPatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a locally generated patch intention over the state with the specified delivery mode.
    /// </summary>
    Task ApplyPatchAsync(CrdtPatch patch, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies an incoming remote patch retrieved from a proxy peer to the local document state bypassing broadcasting loops.
    /// </summary>
    Task ApplyRemotePatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies an incoming remote patch retrieved from a proxy peer to the local document state with the specified delivery mode.
    /// </summary>
    Task ApplyRemotePatchAsync(CrdtPatch patch, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Applies a patch loaded from the storage journal avoiding asynchronous re-journaling and broadcast triggers.
    /// </summary>
    Task ApplyJournaledPatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a materialized snapshot payload superseding local structure dependencies alongside targeted global bounds.
    /// </summary>
    Task MergeSnapshotAsync(byte[] snapshotData, ShardVersionVectorMap globalState, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a materialized snapshot payload with the specified delivery mode.
    /// </summary>
    Task MergeSnapshotAsync(byte[] snapshotData, ShardVersionVectorMap globalState, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously saves the current in-memory document state to persistent storage.
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
    /// Evicts the state and metadata of a specific peer replica with the specified delivery mode.
    /// </summary>
    /// <param name="replicaId">The identifier of the remote replica to evict.</param>
    /// <param name="deliveryMode">The delivery mode specifying whether to wait for completion or enqueue as fire-and-forget.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task EvictReplicaAsync(string replicaId, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resets the internal CRDT metadata and structures, used when forced to re-bootstrap identity following a cluster tombstone eviction.
    /// </summary>
    /// <param name="oldReplicaId">The pre-reboot replica ID allowing offline operation continuity to map during the incoming fallback snapshot overwrite.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ResetLocalStateAsync(string oldReplicaId, CancellationToken cancellationToken = default);
}

/// <summary>
/// Defines the generic typed interface for a distributed CRDT document manager handling a specific domain model.
/// </summary>
/// <example>
/// <code>
/// var crdtDoc = await typedDoc.GetDocumentAsync(cancellationToken);
/// </code>
/// </example>
public interface IDistributedCrdtDocument<TState> : IDistributedCrdtDocument where TState : class, new()
{
    /// <summary>
    /// Exposes the strictly typed underlying CRDT document containing the application state.
    /// </summary>
    ValueTask<CrdtDocument<TState>> GetDocumentAsync(CancellationToken cancellationToken = default);
}