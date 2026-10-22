namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// A disk-backed write-ahead log (WAL) buffer that appends serialized operations sequentially to local segment files
/// before staging them in memory for batched background flushing.
/// </summary>
public sealed class DiskWriteAheadBuffer : ICrdtWriteAheadBuffer
{
    private readonly ReplicaContext replicaContext;
    private readonly ICrdtSerializer serializer;
    private readonly DistributedCrdtOptions options;
    private readonly ILogger<DiskWriteAheadBuffer> logger;
    private readonly Channel<JournaledOperation> channel;
    private readonly SemaphoreSlim fileLock = new(1, 1);
    private readonly string walDirectory;
    private readonly string safeReplicaId;

    private FileStream? activeStream;
    private string? activeSegmentPath;
    private long currentSegmentId;
    private long unacknowledgedCount;
    private bool disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="DiskWriteAheadBuffer"/> class.
    /// </summary>
    /// <param name="replicaContext">The local replica context.</param>
    /// <param name="serializer">The CRDT serializer.</param>
    /// <param name="options">The distributed CRDT options.</param>
    /// <param name="logger">The logger instance.</param>
    public DiskWriteAheadBuffer(
        ReplicaContext replicaContext,
        ICrdtSerializer serializer,
        IOptions<DistributedCrdtOptions> options,
        ILogger<DiskWriteAheadBuffer> logger)
    {
        this.replicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        ArgumentNullException.ThrowIfNull(options);
        this.options = options.Value;
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.safeReplicaId = SanitizeFileName(this.replicaContext.ReplicaId);
        this.walDirectory = string.IsNullOrWhiteSpace(this.options.WalDirectoryPath)
            ? Path.Combine(Path.GetTempPath(), "ama-crdt-wal")
            : this.options.WalDirectoryPath;

        Directory.CreateDirectory(this.walDirectory);

        var capacity = this.options.WalChannelCapacity > 0 ? this.options.WalChannelCapacity : 10_000;
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

        this.fileLock.Wait();
        try
        {
            this.EnsureActiveStream();
            var lengthBuffer = ArrayPool<byte>.Shared.Rent(4);
            try
            {
                for (int i = 0; i < operations.Count; i++)
                {
                    var payload = this.serializer.SerializeToBytes(operations[i]);
                    BinaryPrimitives.WriteInt32LittleEndian(lengthBuffer.AsSpan(0, 4), payload.Length);
                    this.activeStream!.Write(lengthBuffer, 0, 4);
                    this.activeStream.Write(payload, 0, payload.Length);
                }

                this.activeStream!.Flush(flushToDisk: true);
                this.unacknowledgedCount += operations.Count;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(lengthBuffer);
            }
        }
        finally
        {
            this.fileLock.Release();
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

        await this.fileLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            this.EnsureActiveStream();
            var lengthBuffer = ArrayPool<byte>.Shared.Rent(4);
            try
            {
                for (int i = 0; i < operations.Count; i++)
                {
                    var payload = this.serializer.SerializeToBytes(operations[i]);
                    BinaryPrimitives.WriteInt32LittleEndian(lengthBuffer.AsSpan(0, 4), payload.Length);
                    await this.activeStream!.WriteAsync(lengthBuffer.AsMemory(0, 4), cancellationToken).ConfigureAwait(false);
                    await this.activeStream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
                }

                await this.activeStream!.FlushAsync(cancellationToken).ConfigureAwait(false);
                this.unacknowledgedCount += operations.Count;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(lengthBuffer);
            }
        }
        finally
        {
            this.fileLock.Release();
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
    public async ValueTask AcknowledgeBatchAsync(IReadOnlyList<JournaledOperation> flushedOperations, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(flushedOperations);
        if (flushedOperations.Count == 0)
        {
            return;
        }

        await this.fileLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            this.unacknowledgedCount = Math.Max(0, this.unacknowledgedCount - flushedOperations.Count);
            if (this.unacknowledgedCount == 0 && this.activeStream != null)
            {
                this.activeStream.SetLength(0);
                await this.activeStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            this.fileLock.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask RecoverAsync(IDistributedCrdtStorage targetStorage, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(targetStorage);

        await this.fileLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.activeStream != null)
            {
                await this.activeStream.DisposeAsync().ConfigureAwait(false);
                this.activeStream = null;
                this.activeSegmentPath = null;
            }

            if (!Directory.Exists(this.walDirectory))
            {
                return;
            }

            var pattern = $"{this.safeReplicaId}_wal_*.log";
            var segmentFiles = Directory.GetFiles(this.walDirectory, pattern)
                .OrderBy(f => f, StringComparer.Ordinal)
                .ToArray();

            if (segmentFiles.Length == 0)
            {
                return;
            }

            var recoveredOps = new List<JournaledOperation>();
            var lengthBuffer = ArrayPool<byte>.Shared.Rent(4);

            try
            {
                foreach (var file in segmentFiles)
                {
                    try
                    {
                        await using var fs = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                        while (fs.Position + 4 <= fs.Length)
                        {
                            int headerRead = await ReadExactAsync(fs, lengthBuffer.AsMemory(0, 4), cancellationToken).ConfigureAwait(false);
                            if (headerRead < 4)
                            {
                                break;
                            }

                            int frameLength = BinaryPrimitives.ReadInt32LittleEndian(lengthBuffer.AsSpan(0, 4));
                            if (frameLength <= 0 || fs.Position + frameLength > fs.Length)
                            {
                                break;
                            }

                            var payloadBuffer = new byte[frameLength];
                            int payloadRead = await ReadExactAsync(fs, payloadBuffer.AsMemory(0, frameLength), cancellationToken).ConfigureAwait(false);
                            if (payloadRead < frameLength)
                            {
                                break;
                            }

                            var op = this.serializer.DeserializeFromBytes<JournaledOperation>(payloadBuffer);
                            if (op != null)
                            {
                                recoveredOps.Add(op);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        this.logger.LogWarning(ex, "Encountered corrupted or unreadable WAL segment file '{File}' during recovery.", file);
                    }
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(lengthBuffer);
            }

            if (recoveredOps.Count > 0)
            {
                this.logger.LogInformation("Recovering {Count} uncommitted operations from local disk WAL for replica '{ReplicaId}'.", recoveredOps.Count, this.replicaContext.ReplicaId);
                var batchSize = this.options.WalMaxBatchSize > 0 ? this.options.WalMaxBatchSize : 500;
                foreach (var chunk in recoveredOps.Chunk(batchSize))
                {
                    await targetStorage.AppendBatchAsync(chunk, cancellationToken).ConfigureAwait(false);
                }
            }

            foreach (var file in segmentFiles)
            {
                try
                {
                    File.Delete(file);
                }
                catch (Exception ex)
                {
                    this.logger.LogDebug(ex, "Failed to delete recovered WAL segment file '{File}'.", file);
                }
            }

            this.unacknowledgedCount = 0;
        }
        finally
        {
            this.fileLock.Release();
        }
    }

    /// <inheritdoc />
    public void Complete()
    {
        this.channel.Writer.TryComplete();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (this.disposed)
        {
            return;
        }

        this.disposed = true;
        this.Complete();

        await this.fileLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (this.activeStream != null)
            {
                var shouldDelete = this.unacknowledgedCount == 0 && this.activeStream.Length == 0;
                var pathToDelete = this.activeSegmentPath;

                await this.activeStream.DisposeAsync().ConfigureAwait(false);
                this.activeStream = null;
                this.activeSegmentPath = null;

                if (shouldDelete && !string.IsNullOrEmpty(pathToDelete) && File.Exists(pathToDelete))
                {
                    try
                    {
                        File.Delete(pathToDelete);
                    }
                    catch
                    {
                        // Ignore cleanup errors on shutdown
                    }
                }
            }
        }
        finally
        {
            this.fileLock.Release();
            this.fileLock.Dispose();
        }
    }

    private void EnsureActiveStream()
    {
        if (this.activeStream != null)
        {
            return;
        }

        Directory.CreateDirectory(this.walDirectory);
        this.currentSegmentId++;
        var fileName = $"{this.safeReplicaId}_wal_{this.currentSegmentId.ToString("D6", CultureInfo.InvariantCulture)}.log";
        this.activeSegmentPath = Path.Combine(this.walDirectory, fileName);
        this.activeStream = new FileStream(
            this.activeSegmentPath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.Read,
            bufferSize: 4096,
            FileOptions.SequentialScan);
        this.activeStream.Seek(0, SeekOrigin.End);
    }

    private static async ValueTask<int> ReadExactAsync(Stream stream, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.Slice(totalRead), cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                break;
            }

            totalRead += read;
        }

        return totalRead;
    }

    private static string SanitizeFileName(string name)
    {
        var invalidChars = Path.GetInvalidFileNameChars();
        return new string(name.Select(ch => invalidChars.Contains(ch) ? '_' : ch).ToArray());
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