namespace Ama.Enterprise.CRDT.Distributed.Services.Storage;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Scoped service that coordinates trimming operations within a node or DI container to ensure only one
/// aggressive or maintenance trim runs at a time per node without blocking independent nodes or parallel tests.
/// </summary>
public sealed class CrdtTrimCoordinator : IDisposable
{
    private readonly SemaphoreSlim trimLock = new(1, 1);

    /// <summary>
    /// Asynchronously waits to enter the node-wide journal trim lock.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the lock.</param>
    /// <returns>A task that completes when the lock has been acquired.</returns>
    public Task WaitAsync(CancellationToken cancellationToken = default)
    {
        return trimLock.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// Releases the node-wide journal trim lock.
    /// </summary>
    public void Release()
    {
        trimLock.Release();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        trimLock.Dispose();
    }
}