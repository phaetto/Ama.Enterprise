namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Decorates an <see cref="IDistributedCrdtStorage"/> instance with an in-flight staging index and an <see cref="ICrdtWriteAheadBuffer"/>
/// to decouple critical mutation paths from slower storage engines while preserving read-your-own-writes consistency.
/// </summary>
public sealed class BufferedDistributedCrdtStorageDecorator : IDistributedCrdtStorage, IAsyncDisposable, IDisposable
{
    private readonly IDistributedCrdtStorage innerStorage;
    private readonly ICrdtWriteAheadBuffer writeAheadBuffer;
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, JournaledOperation>> inFlightByShard = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, long>> lastTrimmedVectors = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<(string ShardId, string RawDocId), long> deletedDocuments = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="BufferedDistributedCrdtStorageDecorator"/> class.
    /// </summary>
    /// <param name="innerStorage">The underlying storage implementation.</param>
    /// <param name="writeAheadBuffer">The write-ahead buffer managing staging and durability.</param>
    public BufferedDistributedCrdtStorageDecorator(
        IDistributedCrdtStorage innerStorage,
        ICrdtWriteAheadBuffer writeAheadBuffer)
    {
        this.innerStorage = innerStorage ?? throw new ArgumentNullException(nameof(innerStorage));
        this.writeAheadBuffer = writeAheadBuffer ?? throw new ArgumentNullException(nameof(writeAheadBuffer));
    }

    /// <summary>
    /// Gets the underlying storage wrapped by this decorator.
    /// </summary>
    public IDistributedCrdtStorage InnerStorage => this.innerStorage;

    /// <summary>
    /// Gets the write-ahead buffer associated with this decorator.
    /// </summary>
    public ICrdtWriteAheadBuffer WriteAheadBuffer => this.writeAheadBuffer;

    /// <inheritdoc />
    public void Append(IComparable documentId, IReadOnlyList<CrdtOperation> operations)
    {
        ArgumentNullException.ThrowIfNull(documentId);
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0)
        {
            return;
        }

        var journaledList = this.StageOperations(documentId, operations);
        this.writeAheadBuffer.Enqueue(journaledList);
    }

    /// <inheritdoc />
    public async Task AppendAsync(IComparable documentId, IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(documentId);
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0)
        {
            return;
        }

        var journaledList = this.StageOperations(documentId, operations);
        await this.writeAheadBuffer.EnqueueAsync(journaledList, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task AppendBatchAsync(IReadOnlyList<JournaledOperation> operations, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0)
        {
            return;
        }

        this.StageJournaledOperations(operations);
        await this.writeAheadBuffer.EnqueueAsync(operations, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(
        string shardId,
        string originReplicaId,
        long minShardClock,
        long maxShardClock,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(shardId);
        ArgumentException.ThrowIfNullOrEmpty(originReplicaId);

        var yieldedIds = new HashSet<Guid>();

        await foreach (var op in this.innerStorage.GetOperationsByRangeAsync(shardId, originReplicaId, minShardClock, maxShardClock, cancellationToken).ConfigureAwait(false))
        {
            if (yieldedIds.Add(op.Operation.Id))
            {
                yield return op;
            }
        }

        if (this.inFlightByShard.TryGetValue(shardId, out var shardDict))
        {
            var stagedMatches = shardDict.Values
                .Where(o => string.Equals(o.Operation.ReplicaId, originReplicaId, StringComparison.Ordinal)
                         && o.Operation.ShardClock > minShardClock
                         && o.Operation.ShardClock <= maxShardClock)
                .OrderBy(o => o.Operation.ShardClock)
                .ToList();

            foreach (var op in stagedMatches)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (yieldedIds.Add(op.Operation.Id))
                {
                    yield return op;
                }
            }
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(
        string shardId,
        string originReplicaId,
        IEnumerable<long> shardClocks,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(shardId);
        ArgumentException.ThrowIfNullOrEmpty(originReplicaId);
        ArgumentNullException.ThrowIfNull(shardClocks);

        var clockSet = shardClocks as HashSet<long> ?? shardClocks.ToHashSet();
        if (clockSet.Count == 0)
        {
            yield break;
        }

        var yieldedIds = new HashSet<Guid>();

        await foreach (var op in this.innerStorage.GetOperationsByDotsAsync(shardId, originReplicaId, clockSet, cancellationToken).ConfigureAwait(false))
        {
            if (yieldedIds.Add(op.Operation.Id))
            {
                yield return op;
            }
        }

        if (this.inFlightByShard.TryGetValue(shardId, out var shardDict))
        {
            var stagedMatches = shardDict.Values
                .Where(o => string.Equals(o.Operation.ReplicaId, originReplicaId, StringComparison.Ordinal)
                         && clockSet.Contains(o.Operation.ShardClock))
                .OrderBy(o => o.Operation.ShardClock)
                .ToList();

            foreach (var op in stagedMatches)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (yieldedIds.Add(op.Operation.Id))
                {
                    yield return op;
                }
            }
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<JournaledOperation> GetAllJournaledOperationsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var yieldedIds = new HashSet<Guid>();

        await foreach (var op in this.innerStorage.GetAllJournaledOperationsAsync(cancellationToken).ConfigureAwait(false))
        {
            if (yieldedIds.Add(op.Operation.Id))
            {
                yield return op;
            }
        }

        var stagedSnapshot = this.inFlightByShard.Values
            .SelectMany(d => d.Values)
            .OrderBy(o => o.Operation.ShardId, StringComparer.Ordinal)
            .ThenBy(o => o.Operation.ShardClock)
            .ToList();

        foreach (var op in stagedSnapshot)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (yieldedIds.Add(op.Operation.Id))
            {
                yield return op;
            }
        }
    }

    /// <inheritdoc />
    public async Task<long> GetJournalCountAsync(string shardId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(shardId);

        var innerCount = await this.innerStorage.GetJournalCountAsync(shardId, cancellationToken).ConfigureAwait(false);
        var inFlightCount = this.inFlightByShard.TryGetValue(shardId, out var shardDict) ? shardDict.Count : 0;

        return innerCount + inFlightCount;
    }

    /// <inheritdoc />
    public async Task TrimAsync(string shardId, IReadOnlyDictionary<string, long> globalMinimumVersionVector, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(shardId);
        ArgumentNullException.ThrowIfNull(globalMinimumVersionVector);

        if (globalMinimumVersionVector.Count > 0)
        {
            var shardTrimMap = this.lastTrimmedVectors.GetOrAdd(shardId, _ => new ConcurrentDictionary<string, long>(StringComparer.Ordinal));
            foreach (var kvp in globalMinimumVersionVector)
            {
                shardTrimMap.AddOrUpdate(kvp.Key, kvp.Value, (_, existing) => Math.Max(existing, kvp.Value));
            }

            if (this.inFlightByShard.TryGetValue(shardId, out var shardDict))
            {
                foreach (var kvp in shardDict)
                {
                    var op = kvp.Value.Operation;
                    if (globalMinimumVersionVector.TryGetValue(op.ReplicaId, out var minClock) && op.ShardClock <= minClock)
                    {
                        shardDict.TryRemove(kvp.Key, out _);
                    }
                }
            }
        }

        await this.innerStorage.TrimAsync(shardId, globalMinimumVersionVector, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task DeleteDocumentAsync(string shardId, string documentId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId))
        {
            return;
        }

        var normalizedShardId = shardId ?? string.Empty;
        var rawDocId = ExtractRawDocumentId(documentId);

        this.deletedDocuments[(normalizedShardId, rawDocId)] = Environment.TickCount64;

        if (!string.IsNullOrEmpty(normalizedShardId))
        {
            if (this.inFlightByShard.TryGetValue(normalizedShardId, out var shardDict))
            {
                RemoveMatchingDocumentOperations(shardDict, documentId, rawDocId);
            }
        }
        else
        {
            foreach (var shardDict in this.inFlightByShard.Values)
            {
                RemoveMatchingDocumentOperations(shardDict, documentId, rawDocId);
            }
        }

        await this.innerStorage.DeleteDocumentAsync(shardId, documentId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<ShardVersionVectorMap?> LoadShardVersionVectorMapAsync(string replicaId, CancellationToken cancellationToken = default)
        => this.innerStorage.LoadShardVersionVectorMapAsync(replicaId, cancellationToken);

    /// <inheritdoc />
    public Task SaveShardVersionVectorMapAsync(string replicaId, ShardVersionVectorMap shardVersionVectorMap, CancellationToken cancellationToken = default)
        => this.innerStorage.SaveShardVersionVectorMapAsync(replicaId, shardVersionVectorMap, cancellationToken);

    /// <inheritdoc />
    public Task<ClusterStateSnapshotDto?> LoadClusterStateAsync(string replicaId, CancellationToken cancellationToken = default)
        => this.innerStorage.LoadClusterStateAsync(replicaId, cancellationToken);

    /// <inheritdoc />
    public Task SaveClusterStateAsync(string replicaId, ClusterStateSnapshotDto state, CancellationToken cancellationToken = default)
        => this.innerStorage.SaveClusterStateAsync(replicaId, state, cancellationToken);

    /// <inheritdoc />
    public Task<CrdtDocument<TState>?> LoadDocumentAsync<TState>(string shardId, string documentId, CancellationToken cancellationToken = default) where TState : class, new()
        => this.innerStorage.LoadDocumentAsync<TState>(shardId, documentId, cancellationToken);

    /// <inheritdoc />
    public Task<CrdtDocument<TState>?> LoadOrphanedDocumentAsync<TState>(string shardId, string documentId, CancellationToken cancellationToken = default) where TState : class, new()
        => this.innerStorage.LoadOrphanedDocumentAsync<TState>(shardId, documentId, cancellationToken);

    /// <inheritdoc />
    public Task SaveDocumentAsync<TState>(string shardId, string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default) where TState : class, new()
        => this.innerStorage.SaveDocumentAsync(shardId, documentId, document, cancellationToken);

    /// <inheritdoc />
    public Task SaveSerializedDocumentAsync<TState>(string shardId, string documentId, byte[] serializedDocument, CancellationToken cancellationToken = default) where TState : class, new()
        => this.innerStorage.SaveSerializedDocumentAsync<TState>(shardId, documentId, serializedDocument, cancellationToken);

    /// <summary>
    /// Flushes a single batch of buffered operations to the inner storage if any are available.
    /// </summary>
    /// <param name="maxBatchSize">The maximum number of operations to flush in one batch.</param>
    /// <param name="maxWaitTime">The maximum duration to wait for a batch to fill.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of operations read from the buffer.</returns>
    public async ValueTask<int> FlushNextBatchAsync(int maxBatchSize, TimeSpan maxWaitTime, CancellationToken cancellationToken = default)
    {
        var rawBatch = await this.writeAheadBuffer.ReadNextBatchAsync(maxBatchSize, maxWaitTime, cancellationToken).ConfigureAwait(false);
        if (rawBatch.Count == 0)
        {
            return 0;
        }

        var validBatch = new List<JournaledOperation>(rawBatch.Count);
        for (int i = 0; i < rawBatch.Count; i++)
        {
            var jOp = rawBatch[i];
            if (this.ShouldFlushOperation(jOp))
            {
                validBatch.Add(jOp);
            }
        }

        if (validBatch.Count > 0)
        {
            await this.innerStorage.AppendBatchAsync(validBatch, cancellationToken).ConfigureAwait(false);
        }

        this.CompleteInFlightBatch(rawBatch);
        await this.writeAheadBuffer.AcknowledgeBatchAsync(rawBatch, cancellationToken).ConfigureAwait(false);

        return rawBatch.Count;
    }

    /// <summary>
    /// Completes the write-ahead buffer and drains all remaining buffered operations to the inner storage.
    /// </summary>
    /// <param name="maxBatchSize">The maximum batch size per flush iteration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous drain operation.</returns>
    public async Task DrainAsync(int maxBatchSize, CancellationToken cancellationToken = default)
    {
        this.writeAheadBuffer.Complete();
        var effectiveBatchSize = maxBatchSize > 0 ? maxBatchSize : 500;

        while (!cancellationToken.IsCancellationRequested)
        {
            var flushed = await this.FlushNextBatchAsync(effectiveBatchSize, TimeSpan.Zero, cancellationToken).ConfigureAwait(false);
            if (flushed == 0)
            {
                break;
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        this.writeAheadBuffer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        if (this.innerStorage is IDisposable disposableInner)
        {
            disposableInner.Dispose();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await this.writeAheadBuffer.DisposeAsync().ConfigureAwait(false);
        if (this.innerStorage is IAsyncDisposable asyncDisposableInner)
        {
            await asyncDisposableInner.DisposeAsync().ConfigureAwait(false);
        }
        else if (this.innerStorage is IDisposable disposableInner)
        {
            disposableInner.Dispose();
        }
    }

    private List<JournaledOperation> StageOperations(IComparable documentId, IReadOnlyList<CrdtOperation> operations)
    {
        var docIdStr = documentId.ToString() ?? string.Empty;
        var rawDocId = ExtractRawDocumentId(docIdStr);
        var journaledList = new List<JournaledOperation>(operations.Count);

        for (int i = 0; i < operations.Count; i++)
        {
            var op = operations[i];
            var shardId = op.ShardId ?? string.Empty;
            this.deletedDocuments.TryRemove((shardId, rawDocId), out _);
            this.deletedDocuments.TryRemove((string.Empty, rawDocId), out _);

            var jOp = new JournaledOperation(documentId, op);
            journaledList.Add(jOp);

            var shardDict = this.inFlightByShard.GetOrAdd(shardId, _ => new ConcurrentDictionary<Guid, JournaledOperation>());
            shardDict[op.Id] = jOp;
        }

        return journaledList;
    }

    private void StageJournaledOperations(IReadOnlyList<JournaledOperation> operations)
    {
        for (int i = 0; i < operations.Count; i++)
        {
            var jOp = operations[i];
            var docIdStr = jOp.DocumentId?.ToString() ?? string.Empty;
            var rawDocId = ExtractRawDocumentId(docIdStr);
            var shardId = jOp.Operation.ShardId ?? string.Empty;

            this.deletedDocuments.TryRemove((shardId, rawDocId), out _);
            this.deletedDocuments.TryRemove((string.Empty, rawDocId), out _);

            var shardDict = this.inFlightByShard.GetOrAdd(shardId, _ => new ConcurrentDictionary<Guid, JournaledOperation>());
            shardDict[jOp.Operation.Id] = jOp;
        }
    }

    private bool ShouldFlushOperation(JournaledOperation journaledOperation)
    {
        var op = journaledOperation.Operation;
        var shardId = op.ShardId ?? string.Empty;

        // If the operation was already removed from the in-flight index by a concurrent TrimAsync or DeleteDocumentAsync, skip it.
        if (!this.inFlightByShard.TryGetValue(shardId, out var shardDict) || !shardDict.ContainsKey(op.Id))
        {
            return false;
        }

        if (this.lastTrimmedVectors.TryGetValue(shardId, out var trimMap) &&
            trimMap.TryGetValue(op.ReplicaId, out var trimmedClock) &&
            op.ShardClock <= trimmedClock)
        {
            return false;
        }

        var docIdStr = journaledOperation.DocumentId?.ToString() ?? string.Empty;
        var rawDocId = ExtractRawDocumentId(docIdStr);
        if (this.deletedDocuments.ContainsKey((shardId, rawDocId)) || this.deletedDocuments.ContainsKey((string.Empty, rawDocId)))
        {
            return false;
        }

        return true;
    }

    private void CompleteInFlightBatch(IReadOnlyList<JournaledOperation> flushedOperations)
    {
        for (int i = 0; i < flushedOperations.Count; i++)
        {
            var jOp = flushedOperations[i];
            var shardId = jOp.Operation.ShardId ?? string.Empty;
            if (this.inFlightByShard.TryGetValue(shardId, out var shardDict))
            {
                shardDict.TryRemove(jOp.Operation.Id, out _);
            }
        }
    }

    private static void RemoveMatchingDocumentOperations(ConcurrentDictionary<Guid, JournaledOperation> shardDict, string documentId, string rawDocId)
    {
        foreach (var kvp in shardDict)
        {
            var opDocId = kvp.Value.DocumentId?.ToString();
            var opRawDocId = opDocId != null ? ExtractRawDocumentId(opDocId) : string.Empty;
            if (string.Equals(opDocId, documentId, StringComparison.Ordinal) ||
                string.Equals(opDocId, rawDocId, StringComparison.Ordinal) ||
                string.Equals(opRawDocId, rawDocId, StringComparison.Ordinal))
            {
                shardDict.TryRemove(kvp.Key, out _);
            }
        }
    }

    private static string ExtractRawDocumentId(string documentId)
    {
        var colonIndex = documentId.IndexOf(':', StringComparison.Ordinal);
        return colonIndex >= 0 && colonIndex < documentId.Length - 1
            ? documentId[(colonIndex + 1)..]
            : documentId;
    }
}