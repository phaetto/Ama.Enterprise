namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Services;

/// <summary>
/// Defines the command types processed by the shared document state machine pipeline.
/// </summary>
public enum DocumentCommandType
{
    /// <summary>
    /// Initializes the document state from storage.
    /// </summary>
    Initialize,

    /// <summary>
    /// Generates and applies an operation intent.
    /// </summary>
    ApplyRemoteIntent,

    /// <summary>
    /// Applies a locally generated CRDT patch.
    /// </summary>
    ApplyPatch,

    /// <summary>
    /// Applies a remotely received CRDT patch.
    /// </summary>
    ApplyRemotePatch,

    /// <summary>
    /// Applies a journaled CRDT patch during replay.
    /// </summary>
    ApplyJournaledPatch,

    /// <summary>
    /// Serializes the document and retrieves its shard version vector snapshot.
    /// </summary>
    GetSnapshotData,

    /// <summary>
    /// Merges a full state snapshot into the document.
    /// </summary>
    MergeSnapshot,

    /// <summary>
    /// Persists the current document checkpoint to storage.
    /// </summary>
    Checkpoint,

    /// <summary>
    /// Evicts a replica's metadata from the document.
    /// </summary>
    EvictReplica,

    /// <summary>
    /// Resets the local state of the document to its initial state.
    /// </summary>
    ResetLocalState,

    /// <summary>
    /// Compacts CRDT metadata using the configured compaction policy.
    /// </summary>
    CompactMetadata
}

/// <summary>
/// A zero-allocation custom awaitable envelope implementing an <see cref="IValueTaskSource"/> object pool for the shared document command pipeline.
/// </summary>
public sealed class PooledDocumentCommand : IValueTaskSource
{
    private ManualResetValueTaskSourceCore<bool> core;

    /// <summary>
    /// Initializes a new instance of the <see cref="PooledDocumentCommand"/> class.
    /// </summary>
    public PooledDocumentCommand()
    {
        // Vital: Forces the continuation of the ValueTask to run on the ThreadPool.
        // If false, SetResult() executes the caller's continuation inline, which can hijack the
        // channel reader thread and block it (e.g., if the caller executes a synchronous wait like Console.ReadLine).
        core.RunContinuationsAsynchronously = true;
    }

    /// <summary>
    /// Gets or sets the target document processor that executes this command.
    /// </summary>
    public IDocumentCommandTarget? Target { get; set; }

    /// <summary>
    /// Gets or sets the command type.
    /// </summary>
    public DocumentCommandType Type { get; set; }

    /// <summary>
    /// Gets or sets the operation intent to apply.
    /// </summary>
    public IOperationIntent? Intent { get; set; }

    /// <summary>
    /// Gets or sets the target property name for the intent.
    /// </summary>
    public string? TargetProperty { get; set; }

    /// <summary>
    /// Gets or sets the patcher used to generate the CRDT operation.
    /// </summary>
    public IAsyncCrdtPatcher? Patcher { get; set; }

    /// <summary>
    /// Gets or sets the CRDT patch payload.
    /// </summary>
    public CrdtPatch? Patch { get; set; }

    /// <summary>
    /// Gets or sets the serialized snapshot payload.
    /// </summary>
    public byte[]? SnapshotData { get; set; }

    /// <summary>
    /// Gets or sets the global shard version vector map.
    /// </summary>
    public ShardVersionVectorMap? GlobalState { get; set; }

    /// <summary>
    /// Gets or sets an optional explicit timestamp.
    /// </summary>
    public CrdtTimestamp? ExplicitTimestamp { get; set; }

    /// <summary>
    /// Gets or sets the replica identifier to evict.
    /// </summary>
    public string? ReplicaIdToEvict { get; set; }

    /// <summary>
    /// Gets or sets the old replica identifier when resetting local state.
    /// </summary>
    public string? OldReplicaId { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the caller does not await completion of this command.
    /// </summary>
    public bool IsFireAndForget { get; set; }

    /// <summary>
    /// Gets or sets the cancellation token for the command.
    /// </summary>
    public CancellationToken CancellationToken { get; set; }

    /// <summary>
    /// Gets or sets the output serialized snapshot bytes.
    /// </summary>
    public byte[]? ResultSnapshotData { get; set; }

    /// <summary>
    /// Gets or sets the output shard version vector map.
    /// </summary>
    public ShardVersionVectorMap? ResultGlobalState { get; set; }

    /// <summary>
    /// Resets the command state before returning it to the pool.
    /// </summary>
    public void Reset()
    {
        core.Reset();
        Target = null;
        Type = default;
        Intent = null;
        TargetProperty = null;
        Patcher = null;
        Patch = null;
        SnapshotData = null;
        GlobalState = null;
        ExplicitTimestamp = null;
        ReplicaIdToEvict = null;
        OldReplicaId = null;
        IsFireAndForget = false;
        CancellationToken = default;

        ResultSnapshotData = null;
        ResultGlobalState = null;
    }

    /// <summary>
    /// Returns a <see cref="ValueTask"/> backed by this pooled command instance.
    /// </summary>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous execution.</returns>
    public ValueTask ExecuteAsync() => new ValueTask(this, core.Version);

    /// <summary>
    /// Signals that the command completed.
    /// </summary>
    public void SetResult() => core.SetResult(true);

    /// <summary>
    /// Signals that the command faulted with an exception.
    /// </summary>
    /// <param name="ex">The exception that occurred.</param>
    public void SetException(Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        core.SetException(ex);
    }

    /// <inheritdoc />
    public void GetResult(short token) => core.GetResult(token);

    /// <inheritdoc />
    public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);

    /// <inheritdoc />
    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => core.OnCompleted(continuation, state, token, flags);
}