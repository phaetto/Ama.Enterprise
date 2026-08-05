namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Providers;

internal enum DocumentCommandType
{
    Initialize,
    ApplyPatch,
    ApplyOperations,
    GetSnapshotData,
    MergeSnapshot,
    Checkpoint,
    EvictReplica,
    ResetLocalState
}

/// <summary>
/// A zero-allocation custom awaitable envelope implementing an IValueTaskSource object pool for the Single-Reader document channel.
/// </summary>
internal sealed class PooledDocumentCommand<TState> : IValueTaskSource where TState : class, new()
{
    private ManualResetValueTaskSourceCore<bool> core;

    public PooledDocumentCommand()
    {
        // Vital: Forces the continuation of the ValueTask to run on the ThreadPool.
        // If false, SetResult() executes the caller's continuation inline, which can hijack the
        // channel reader thread and block it (e.g., if the caller executes a synchronous wait like Console.ReadLine).
        core.RunContinuationsAsynchronously = true;
    }

    public DocumentCommandType Type { get; set; }
    public CrdtPatch? Patch { get; set; }
    public IReadOnlyList<CrdtOperation>? Operations { get; set; }
    public byte[]? SnapshotData { get; set; }
    public DottedVersionVector? GlobalState { get; set; }
    public ICrdtTimestamp? ExplicitTimestamp { get; set; }
    public string? ReplicaIdToEvict { get; set; }
    public string? OldReplicaId { get; set; }
    public CancellationToken CancellationToken { get; set; }

    public byte[]? ResultSnapshotData { get; set; }
    public DottedVersionVector? ResultGlobalState { get; set; }

    public void Reset()
    {
        core.Reset();
        Type = default;
        Patch = null;
        Operations = null;
        SnapshotData = null;
        GlobalState = null;
        ExplicitTimestamp = null;
        ReplicaIdToEvict = null;
        OldReplicaId = null;
        CancellationToken = default;
        
        ResultSnapshotData = null;
        ResultGlobalState = null;
    }

    public ValueTask ExecuteAsync() => new ValueTask(this, core.Version);

    public void SetResult() => core.SetResult(true);
    public void SetException(Exception ex) => core.SetException(ex);

    public void GetResult(short token) => core.GetResult(token);
    public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);
    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => core.OnCompleted(continuation, state, token, flags);
}