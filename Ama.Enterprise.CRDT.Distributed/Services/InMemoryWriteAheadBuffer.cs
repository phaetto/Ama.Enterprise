namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.Options;

/// <summary>
/// An in-memory write-ahead buffer that stages <see cref="JournaledOperation"/> instances in a lock-free queue
/// with a shared node-wide capacity semaphore for batched background flushing.
/// </summary>
public sealed class InMemoryWriteAheadBuffer : ICrdtWriteAheadBuffer
{
    private static readonly ConcurrentDictionary<int, SemaphoreSlim> SharedCapacitySemaphores = new();

    private readonly ConcurrentQueue<JournaledOperation> queue = new();
    private readonly SemaphoreSlim capacitySemaphore;
    private readonly object signalLock = new();
    private TaskCompletionSource<bool>? itemAvailableTcs;
    private int pendingCount;
    private volatile bool isCompleted;
    private bool disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryWriteAheadBuffer"/> class.
    /// </summary>
    /// <param name="options">The distributed CRDT configuration options.</param>
    public InMemoryWriteAheadBuffer(IOptions<DistributedCrdtOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var capacity = options.Value.WalChannelCapacity > 0 ? options.Value.WalChannelCapacity : 10_000;
        this.capacitySemaphore = SharedCapacitySemaphores.GetOrAdd(capacity, static cap => new SemaphoreSlim(cap, cap));
    }

    /// <inheritdoc />
    public int PendingCount => Volatile.Read(ref this.pendingCount);

    /// <inheritdoc />
    public void Enqueue(IReadOnlyList<JournaledOperation> operations)
    {
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0 || this.isCompleted)
        {
            return;
        }

        for (int i = 0; i < operations.Count; i++)
        {
            if (this.isCompleted)
            {
                break;
            }

            this.capacitySemaphore.Wait();
            if (this.isCompleted)
            {
                this.capacitySemaphore.Release();
                break;
            }

            this.queue.Enqueue(operations[i]);
            Interlocked.Increment(ref this.pendingCount);
        }

        this.SignalItemAvailable();
    }

    /// <inheritdoc />
    public async ValueTask EnqueueAsync(IReadOnlyList<JournaledOperation> operations, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0 || this.isCompleted)
        {
            return;
        }

        for (int i = 0; i < operations.Count; i++)
        {
            if (this.isCompleted)
            {
                break;
            }

            await this.capacitySemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (this.isCompleted)
            {
                this.capacitySemaphore.Release();
                break;
            }

            this.queue.Enqueue(operations[i]);
            Interlocked.Increment(ref this.pendingCount);
        }

        this.SignalItemAvailable();
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<JournaledOperation>> ReadNextBatchAsync(int maxBatchSize, TimeSpan maxWaitTime, CancellationToken cancellationToken = default)
    {
        if (maxBatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBatchSize), "Max batch size must be greater than zero.");
        }

        if (!await this.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return Array.Empty<JournaledOperation>();
        }

        var batch = new List<JournaledOperation>(Math.Min(maxBatchSize, 64));
        this.DrainUpTo(batch, maxBatchSize);

        if (batch.Count >= maxBatchSize || maxWaitTime <= TimeSpan.Zero || this.isCompleted)
        {
            return batch;
        }

        var sw = Stopwatch.StartNew();
        while (batch.Count < maxBatchSize && !this.isCompleted)
        {
            var remaining = maxWaitTime - sw.Elapsed;
            if (remaining <= TimeSpan.Zero)
            {
                break;
            }

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(remaining);

            try
            {
                if (!await this.WaitToReadAsync(timeoutCts.Token).ConfigureAwait(false))
                {
                    break;
                }

                this.DrainUpTo(batch, maxBatchSize);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }

        return batch;
    }

    /// <inheritdoc />
    public ValueTask AcknowledgeBatchAsync(IReadOnlyList<JournaledOperation> flushedOperations, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(flushedOperations);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask RecoverAsync(IDistributedCrdtStorage targetStorage, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(targetStorage);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public void Complete()
    {
        this.isCompleted = true;
        this.SignalItemAvailable();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (this.disposed)
        {
            return;
        }

        this.disposed = true;
        this.Complete();

        int drained = 0;
        while (this.queue.TryDequeue(out _))
        {
            drained++;
        }

        if (drained > 0)
        {
            Interlocked.Add(ref this.pendingCount, -drained);
            this.capacitySemaphore.Release(drained);
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        this.Dispose();
        return ValueTask.CompletedTask;
    }

    private void DrainUpTo(List<JournaledOperation> batch, int maxBatchSize)
    {
        int dequeued = 0;
        while (batch.Count < maxBatchSize && this.queue.TryDequeue(out var item))
        {
            batch.Add(item);
            dequeued++;
        }

        if (dequeued > 0)
        {
            Interlocked.Add(ref this.pendingCount, -dequeued);
            this.capacitySemaphore.Release(dequeued);
        }
    }

    private ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
    {
        if (!this.queue.IsEmpty)
        {
            return new ValueTask<bool>(true);
        }

        if (this.isCompleted)
        {
            return new ValueTask<bool>(!this.queue.IsEmpty);
        }

        TaskCompletionSource<bool> tcs;
        lock (this.signalLock)
        {
            if (!this.queue.IsEmpty)
            {
                return new ValueTask<bool>(true);
            }

            if (this.isCompleted)
            {
                return new ValueTask<bool>(false);
            }

            this.itemAvailableTcs ??= new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcs = this.itemAvailableTcs;
        }

        return this.WaitWithCancellationAsync(tcs, cancellationToken);
    }

    private async ValueTask<bool> WaitWithCancellationAsync(TaskCompletionSource<bool> tcs, CancellationToken cancellationToken)
    {
        await using var reg = cancellationToken.Register(static state => ((TaskCompletionSource<bool>)state!).TrySetResult(false), tcs);
        await tcs.Task.ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return !this.queue.IsEmpty;
    }

    private void SignalItemAvailable()
    {
        TaskCompletionSource<bool>? tcs = null;
        lock (this.signalLock)
        {
            if (this.itemAvailableTcs != null)
            {
                tcs = this.itemAvailableTcs;
                this.itemAvailableTcs = null;
            }
        }

        tcs?.TrySetResult(true);
    }
}