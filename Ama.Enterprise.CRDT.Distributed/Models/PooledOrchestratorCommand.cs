namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Ama.CRDT.Models;
using Ama.Enterprise.P2p.Models.Core;

internal enum OrchestratorCommandType
{
    Initialize,
    SyncDocuments,
    CreateDocument,
    DeleteDocument,
    DispatchAntiEntropyState,
    ProvideSnapshot,
    BroadcastOperations
}

/// <summary>
/// A zero-allocation custom awaitable envelope implementing an IValueTaskSource object pool for the Single-Reader orchestrator channel.
/// </summary>
internal sealed class PooledOrchestratorCommand : IValueTaskSource
{
    private ManualResetValueTaskSourceCore<bool> core;

    public OrchestratorCommandType Type { get; set; }
    public string? DocumentId { get; set; }
    public string? TypeAlias { get; set; }
    public string? TargetReplicaId { get; set; }
    public PeerId? TargetPeerId { get; set; }
    public IReadOnlyList<CrdtOperation>? Operations { get; set; }
    public CancellationToken CancellationToken { get; set; }

    public void Reset()
    {
        core.Reset();
        Type = default;
        DocumentId = null;
        TypeAlias = null;
        TargetReplicaId = null;
        TargetPeerId = null;
        Operations = null;
        CancellationToken = default;
    }

    public ValueTask ExecuteAsync() => new ValueTask(this, core.Version);

    public void SetResult() => core.SetResult(true);
    public void SetException(Exception ex) => core.SetException(ex);

    public void GetResult(short token) => core.GetResult(token);
    public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);
    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => core.OnCompleted(continuation, state, token, flags);
}