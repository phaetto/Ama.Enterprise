namespace Ama.Enterprise.CRDT.Distributed.Services.Commands;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Lightweight scoped facade over the shared node-wide <see cref="IDocumentCommandStripePool"/>.
/// Eliminates per-scope channels, background worker tasks, object pools, and telemetry gauge registrations.
/// </summary>
public sealed class DocumentCommandPipeline : IDocumentCommandPipeline, IDisposable
{
    private readonly IDocumentCommandStripePool stripePool;
    private readonly string scopeReplicaId;
    private int scopePendingCommandCount;
    private volatile bool isDisposed;

    /// <inheritdoc />
    public bool IsCurrentThreadPipelineProcessor => stripePool.IsCurrentThreadStripeProcessor;

    /// <inheritdoc />
    public int PendingCommandCount => Math.Max(0, Volatile.Read(ref scopePendingCommandCount));

    /// <summary>
    /// Initializes a new instance of the <see cref="DocumentCommandPipeline"/> class.
    /// </summary>
    /// <param name="stripePool">The shared node-wide striped command processor.</param>
    /// <param name="scopeContext">The optional scope context identifying the current replica scope.</param>
    public DocumentCommandPipeline(
        IDocumentCommandStripePool stripePool,
        CrdtScopeContext? scopeContext = null)
    {
        this.stripePool = stripePool ?? throw new ArgumentNullException(nameof(stripePool));
        this.scopeReplicaId = scopeContext?.ReplicaId ?? string.Empty;
    }

    /// <inheritdoc />
    public PooledDocumentCommand RentCommand() => stripePool.RentCommand();

    /// <inheritdoc />
    public void ReturnCommand(PooledDocumentCommand command) => stripePool.ReturnCommand(command);

    /// <inheritdoc />
    public async ValueTask EnqueueAndWaitAsync(PooledDocumentCommand command, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);

        Interlocked.Increment(ref scopePendingCommandCount);
        try
        {
            await stripePool.EnqueueAndWaitAsync(command, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref scopePendingCommandCount);
        }
    }

    /// <inheritdoc />
    public ValueTask EnqueueFireAndForgetAsync(PooledDocumentCommand command, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        return stripePool.EnqueueFireAndForgetAsync(command, cancellationToken);
    }

    /// <inheritdoc />
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (isDisposed)
        {
            return Task.CompletedTask;
        }

        return stripePool.FlushScopeAsync(scopeReplicaId, cancellationToken);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        isDisposed = true;
    }
}