namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Services.Commands;

public enum OrchestratorCommandType
{
    Initialize,
    CreateDocument,
    DeleteDocument,
    ReevaluateTopology,
    FlushBarrier
}

/// <summary>
/// A zero-allocation custom awaitable envelope implementing an IValueTaskSource object pool for the Single-Reader orchestrator channel.
/// </summary>
internal sealed class PooledOrchestratorCommand : IValueTaskSource
{
    private ManualResetValueTaskSourceCore<bool> core;
    private short capturedVersion;
    private int pendingLifecycleRefs;

    public PooledOrchestratorCommand()
    {
        // Vital: Forces the continuation of the ValueTask to run on the ThreadPool.
        // If false, SetResult() executes the caller's continuation inline, which can hijack the
        // channel reader thread and block it (e.g., if the caller executes a synchronous wait like Console.ReadLine).
        core.RunContinuationsAsynchronously = true;
    }

    public IOrchestratorCommandTarget? Target { get; set; }
    public OrchestratorCommandType Type { get; set; }
    public string? DocumentId { get; set; }
    public string? TypeAlias { get; set; }
    public CrdtTimestamp? ExplicitTimestamp { get; set; }
    public bool IsFireAndForget { get; set; }
    public CancellationToken CancellationToken { get; set; }

    public void PrepareExecution()
    {
        capturedVersion = core.Version;
        Volatile.Write(ref pendingLifecycleRefs, 2);
    }

    public void Reset()
    {
        if (Interlocked.Decrement(ref pendingLifecycleRefs) > 0)
        {
            return;
        }

        Volatile.Write(ref pendingLifecycleRefs, 0);
        core.Reset();
        Target = null;
        Type = default;
        DocumentId = null;
        TypeAlias = null;
        ExplicitTimestamp = null;
        IsFireAndForget = false;
        CancellationToken = default;
    }

    public ValueTask ExecuteAsync() => new ValueTask(this, capturedVersion);

    public void SetResult()
    {
        core.SetResult(true);
        Reset();
    }

    public void SetException(Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        core.SetException(ex);
        Reset();
    }

    public void GetResult(short token) => core.GetResult(token);
    public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);
    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => core.OnCompleted(continuation, state, token, flags);
}