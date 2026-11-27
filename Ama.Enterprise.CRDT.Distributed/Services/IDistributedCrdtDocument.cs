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
    /// Gets the registered string alias for the document state type.
    /// </summary>
    string TypeAlias { get; }

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
    /// Applies a materialized snapshot payload with an explicit list of expected shard document keys from the sender's snapshot batch.
    /// </summary>
    /// <param name="snapshotData">The serialized CRDT document snapshot bytes.</param>
    /// <param name="globalState">The shard version vector map attached to the snapshot.</param>
    /// <param name="expectedShardDocumentIds">The optional list of document keys included in the sender's shard snapshot batch.</param>
    /// <param name="deliveryMode">The delivery mode specifying whether to wait for completion or enqueue as fire-and-forget.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous merge operation.</returns>
    Task MergeSnapshotAsync(
        byte[] snapshotData,
        ShardVersionVectorMap globalState,
        IReadOnlyList<string>? expectedShardDocumentIds,
        IntentDeliveryMode deliveryMode = IntentDeliveryMode.Acknowledged,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously saves the current in-memory document state to persistent storage.
    /// Does not directly manage overarching DVV mapping updates avoiding structural write-ahead gaps.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous checkpoint operation.</returns>
    Task CheckpointAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to extract a dirty checkpoint payload from memory and marks the document clean so the caller can persist multiple documents in a single storage batch.
    /// If the batch save fails, the caller must invoke <see cref="RestoreDirtyAfterFailedCheckpoint"/>.
    /// </summary>
    /// <param name="checkpoint">When this method returns <c>true</c>, contains the serialized document checkpoint DTO.</param>
    /// <returns><c>true</c> if the document had pending changes to checkpoint; otherwise, <c>false</c>.</returns>
    bool TryPrepareCheckpoint(out SerializedDocumentCheckpointDto checkpoint)
    {
        checkpoint = default;
        return false;
    }

    /// <summary>
    /// Restores the document's dirty flag if a batch checkpoint persistence operation fails.
    /// </summary>
    void RestoreDirtyAfterFailedCheckpoint()
    {
    }

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

    /// <summary>
    /// Compacts the internal CRDT metadata (such as tombstones and seen exceptions) of this document according to the configured compaction policy.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task CompactMetadataAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Compacts the internal CRDT metadata (such as tombstones and seen exceptions) of this document with the specified delivery mode.
    /// </summary>
    /// <param name="deliveryMode">The delivery mode specifying whether to wait for completion or enqueue as fire-and-forget.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task CompactMetadataAsync(IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Flushes any buffered debounced active-sync operations to registered patch observers immediately.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous flush operation.</returns>
    Task FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Estimates the total in-memory size in bytes of the entire document manager instance, including state, metadata, channels, pools, and telemetry.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation and contains the estimated size in bytes.</returns>
    ValueTask<long> EstimateObjectSizeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Estimates the detailed in-memory footprint of the document data, CRDT metadata, whole document manager instance, and shard version vector without serializing.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation and contains the detailed memory footprint estimates.</returns>
    ValueTask<DocumentMemoryFootprintDto> EstimateMemoryFootprintAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Defines the generic typed interface for a distributed CRDT document manager handling a specific domain model.
/// </summary>
/// <example>
/// <code>
/// var data = typedDoc.ReadData();
/// var crdtDoc = await typedDoc.GetDocumentAsync(cancellationToken);
/// </code>
/// </example>
public interface IDistributedCrdtDocument<TState> : IDistributedCrdtDocument where TState : class, new()
{
    /// <summary>
    /// Gets the active in-memory residency mode for this document instance.
    /// </summary>
    CrdtDocumentResidencyMode ResidencyMode { get; }

    /// <summary>
    /// Synchronously reads the materialized domain data (<typeparamref name="TState"/>).
    /// In <see cref="CrdtDocumentResidencyMode.Unpacked"/> and <see cref="CrdtDocumentResidencyMode.PackedMetadata"/> modes, this is an O(1) reference return without unpacking metadata.
    /// In <see cref="CrdtDocumentResidencyMode.FullySerialized"/> mode, this deserializes the state on demand.
    /// </summary>
    /// <returns>The domain data model instance.</returns>
    TState ReadData();

    /// <summary>
    /// Synchronously reads the full <see cref="CrdtDocument{TState}"/> including causal <see cref="CrdtMetadata"/>.
    /// In <see cref="CrdtDocumentResidencyMode.PackedMetadata"/> mode, this unpacks the binary metadata.
    /// In <see cref="CrdtDocumentResidencyMode.FullySerialized"/> mode, this deserializes the full document.
    /// </summary>
    /// <returns>The full materialized CRDT document.</returns>
    CrdtDocument<TState> ReadFullDocument();

    /// <summary>
    /// Gets a thread-safe snapshot of the strictly typed underlying CRDT document directly from memory without channel or asynchronous overhead.
    /// </summary>
    CrdtDocument<TState> CurrentDocument => ReadFullDocument();

    /// <summary>
    /// Asynchronously retrieves the domain data (<typeparamref name="TState"/>) without unpacking metadata when in <see cref="CrdtDocumentResidencyMode.PackedMetadata"/> mode.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A value task containing the domain data model.</returns>
    ValueTask<TState> GetDataAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Exposes the strictly typed underlying CRDT document containing the application state and causal metadata.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A value task containing the full CRDT document.</returns>
    ValueTask<CrdtDocument<TState>> GetDocumentAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Generates and applies an operation intent directly against this local active document using the replica's patcher.
    /// </summary>
    /// <param name="intent">The operation intent to apply.</param>
    /// <param name="targetProperty">The target property name on <typeparamref name="TState"/>.</param>
    /// <param name="deliveryMode">The delivery mode specifying whether to wait for completion or enqueue as fire-and-forget.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns><c>true</c> if the intent was applied; otherwise, <c>false</c>.</returns>
    Task<bool> ApplyIntentAsync(
        IOperationIntent intent,
        string targetProperty,
        IntentDeliveryMode deliveryMode = IntentDeliveryMode.Acknowledged,
        CancellationToken cancellationToken = default);
}