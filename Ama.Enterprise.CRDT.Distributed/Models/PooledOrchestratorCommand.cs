namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Providers;

internal enum OrchestratorCommandType
{
    Initialize,
    SyncDocuments,
    CreateDocument,
    DeleteDocument
}

/// <summary>
/// A zero-allocation custom awaitable envelope implementing an IValueTaskSource object pool for the Single-Reader orchestrator channel.
/// </summary>
internal sealed class PooledOrchestratorCommand : IValueTaskSource
{
    private ManualResetValueTaskSourceCore<bool> core;

    public PooledOrchestratorCommand()
    {
        // Vital: Forces the continuation of the ValueTask to run on the ThreadPool.
        // If false, SetResult() executes the caller's continuation inline, which can hijack the
        // channel reader thread and block it (e.g., if the caller executes a synchronous wait like Console.ReadLine).
        core.RunContinuationsAsynchronously = true;
    }

    public OrchestratorCommandType Type { get; set; }
    public string? DocumentId { get; set; }
    public string? TypeAlias { get; set; }
    public ICrdtTimestamp? ExplicitTimestamp { get; set; }
    public CancellationToken CancellationToken { get; set; }

    public void Reset()
    {
        core.Reset();
        Type = default;
        DocumentId = null;
        TypeAlias = null;
        ExplicitTimestamp = null;
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