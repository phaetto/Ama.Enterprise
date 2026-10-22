namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.Options;

/// <summary>
/// An in-memory write-ahead buffer backed by a bounded channel for batching journaled operations.
/// </summary>
public sealed class InMemoryWriteAheadBuffer : ICrdtWriteAheadBuffer
{
    private readonly Channel<JournaledOperation> channel;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryWriteAheadBuffer"/> class.
    /// </summary>
    /// <param name="options">The distributed CRDT configuration options.</param>
    public InMemoryWriteAheadBuffer(IOptions<DistributedCrdtOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var capacity = options.Value.WalChannelCapacity > 0 ? options.Value.WalChannelCapacity : 10_000;

        this.channel = Channel.CreateBounded<JournaledOperation>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
    }

    /// <inheritdoc />
    public void Enqueue(IReadOnlyList<JournaledOperation> operations)
    {
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0)
        {
            return;
        }

        var writer = this.channel.Writer;
        for (int i = 0; i < operations.Count; i++)
        {
            var op = operations[i];
            if (!writer.TryWrite(op))
            {
                EnqueueSlowSync(writer, op);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask EnqueueAsync(IReadOnlyList<JournaledOperation> operations, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0)
        {
            return;
        }

        var writer = this.channel.Writer;
        for (int i = 0; i < operations.Count; i++)
        {
            var op = operations[i];
            if (!writer.TryWrite(op))
            {
                await writer.WriteAsync(op, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<JournaledOperation>> ReadNextBatchAsync(int maxBatchSize, TimeSpan maxWaitTime, CancellationToken cancellationToken = default)
    {
        if (maxBatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBatchSize), "Max batch size must be greater than zero.");
        }

        var reader = this.channel.Reader;
        if (!await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return Array.Empty<JournaledOperation>();
        }

        var batch = new List<JournaledOperation>(Math.Min(maxBatchSize, 64));
        while (batch.Count < maxBatchSize && reader.TryRead(out var item))
        {
            batch.Add(item);
        }

        if (batch.Count >= maxBatchSize || maxWaitTime <= TimeSpan.Zero)
        {
            return batch;
        }

        var sw = Stopwatch.StartNew();
        while (batch.Count < maxBatchSize)
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
                if (!await reader.WaitToReadAsync(timeoutCts.Token).ConfigureAwait(false))
                {
                    break;
                }

                while (batch.Count < maxBatchSize && reader.TryRead(out var item))
                {
                    batch.Add(item);
                }
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
        this.channel.Writer.TryComplete();
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        this.Complete();
        return ValueTask.CompletedTask;
    }

    private static void EnqueueSlowSync(ChannelWriter<JournaledOperation> writer, JournaledOperation operation)
    {
        while (writer.WaitToWriteAsync().AsTask().GetAwaiter().GetResult())
        {
            if (writer.TryWrite(operation))
            {
                return;
            }
        }
    }
}