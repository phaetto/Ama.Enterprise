namespace Ama.Enterprise.CRDT.Distributed.TableStorage.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.TableStorage.Models;

/// <summary>
/// A unified Azure Table Storage implementation managing CRDT states, global version vectors, and operation journals explicitly without reflection to ensure AOT compatibility.
/// Includes dynamic tenant isolation bounding to securely isolate node data when sharing a single Table Storage backend across multiple local replicas.
/// </summary>
public sealed class TableStorageDistributedCrdtStorage : IDistributedCrdtStorage, IDisposable
{
    private const string GlobalDvvPartitionKey = "GlobalDVV";
    private const string DocumentStatePartitionKey = "DocumentState";
    private const string ClusterStatePartitionKey = "ClusterState";
    private const string JournalPartitionPrefix = "Journal_";

    private readonly IServiceProvider serviceProvider;
    private readonly TableServiceClient tableServiceClient;
    private readonly TableClient tableClient;
    private readonly ICrdtSerializer serializer;
    private readonly IAsyncCrdtMerger? merger;
    private readonly bool useBinarySerialization;
    private readonly ILogger<TableStorageDistributedCrdtStorage> logger;

    private readonly Meter meter;
    private readonly Counter<long> operationsReadCounter;
    private readonly Counter<long> operationsWriteCounter;
    private readonly Counter<long> operationsDeleteCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    static TableStorageDistributedCrdtStorage()
    {
        // Explicitly invoke the parameterless constructor to prevent the .NET AOT trimmer
        // from stripping it, as the Azure Data Tables SDK relies on it internally via reflection constraints 
        // during GetEntityAsync<T> and QueryAsync<T>.
        _ = new TableEntity();
    }

    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(TableEntity))]
    public TableStorageDistributedCrdtStorage(
        IServiceProvider serviceProvider,
        IOptions<TableStorageCrdtOptions> options,
        JsonCrdtSerializer textJsonSerializer,
        IEnumerable<ICrdtSerializer> availableSerializers,
        ILogger<TableStorageDistributedCrdtStorage> logger,
        IMeterFactory? meterFactory = null)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        ArgumentNullException.ThrowIfNull(options);

        var storageOptions = options.Value ?? throw new ArgumentException("Options value cannot be null.", nameof(options));

        ArgumentException.ThrowIfNullOrEmpty(storageOptions.ConnectionString);
        ArgumentException.ThrowIfNullOrEmpty(storageOptions.TableName);

        this.useBinarySerialization = storageOptions.UseBinarySerialization;

        if (storageOptions.UseBinarySerialization)
        {
            this.serializer = availableSerializers.FirstOrDefault(s => s.GetType() != typeof(JsonCrdtSerializer)) 
                ?? throw new InvalidOperationException("Binary serialization was requested for Table Storage, but no alternative ICrdtSerializer (e.g., MessagePack) was found in the DI container.");
        }
        else
        {
            this.serializer = textJsonSerializer ?? throw new ArgumentNullException(nameof(textJsonSerializer));
        }

        this.merger = this.serviceProvider.GetService<IAsyncCrdtMerger>();
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.tableServiceClient = new TableServiceClient(storageOptions.ConnectionString);
        this.tableClient = this.tableServiceClient.GetTableClient(storageOptions.TableName);

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.TableStorage") ?? new Meter("Ama.Enterprise.CRDT.Distributed.TableStorage");
        this.operationsReadCounter = this.meter.CreateCounter<long>(
            "crdt.storage.table.reads", 
            "operations", 
            "Total read operations from table storage");
        this.operationsWriteCounter = this.meter.CreateCounter<long>(
            "crdt.storage.table.writes", 
            "operations", 
            "Total write operations to table storage");
        this.operationsDeleteCounter = this.meter.CreateCounter<long>(
            "crdt.storage.table.deletes", 
            "operations", 
            "Total delete operations from table storage");
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>(
            "crdt.storage.table.payload_bytes", 
            "bytes", 
            "Size of payloads read or written");
    }

    public async Task<ShardVersionVectorMap?> LoadShardVersionVectorMapAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);

        try
        {
            var response = await this.tableClient.GetEntityAsync<TableEntity>(this.GetGlobalDvvPartitionKey(), replicaId, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (response?.Value != null)
            {
                var tags = new KeyValuePair<string, object?>[] { new("type", "dvv") };
                this.operationsReadCounter.Add(1, tags);

                var payload = CrdtTableEntity.GetPayload(response.Value);
                if (payload != null)
                {
                    this.payloadBytesHistogram.Record(payload.Length, tags);
                }

                return this.serializer.DeserializeFromBytes<ShardVersionVectorMap>(payload);
            }
        }
        catch (RequestFailedException ex) when (ex.Status == 404 && ex.ErrorCode != "TableNotFound")
        {
            return null;
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Storage connection failed during LoadShardVersionVectorMapAsync. Failing fast to prevent state corruption.");
            throw;
        }

        return null;
    }

    public async Task SaveShardVersionVectorMapAsync(string replicaId, ShardVersionVectorMap shardVersionVectorMap, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(shardVersionVectorMap);

        try
        {
            var tags = new KeyValuePair<string, object?>[] { new("type", "dvv") };
            this.operationsWriteCounter.Add(1, tags);

            var payload = this.serializer.SerializeToBytes(shardVersionVectorMap);
            if (payload != null)
            {
                this.payloadBytesHistogram.Record(payload.Length, tags);
            }

            var entity = new TableEntity(this.GetGlobalDvvPartitionKey(), replicaId);
            CrdtTableEntity.SetPayload(entity, payload);

            await this.tableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Storage connection failed during SaveShardVersionVectorMapAsync.");
            throw;
        }
    }

    public async Task<ClusterStateSnapshotDto?> LoadClusterStateAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);

        try
        {
            var response = await this.tableClient.GetEntityAsync<TableEntity>(this.GetClusterStatePartitionKey(), replicaId, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (response?.Value != null)
            {
                var tags = new KeyValuePair<string, object?>[] { new("type", "cluster_state") };
                this.operationsReadCounter.Add(1, tags);

                var payload = CrdtTableEntity.GetPayload(response.Value);
                if (payload != null)
                {
                    this.payloadBytesHistogram.Record(payload.Length, tags);
                }

                return this.serializer.DeserializeFromBytes<ClusterStateSnapshotDto>(payload);
            }
        }
        catch (RequestFailedException ex) when (ex.Status == 404 && ex.ErrorCode != "TableNotFound")
        {
            return null;
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Storage connection failed during LoadClusterStateAsync. Failing fast to prevent state corruption.");
            throw;
        }

        return null;
    }

    public async Task SaveClusterStateAsync(string replicaId, ClusterStateSnapshotDto state, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(state);

        try
        {
            var tags = new KeyValuePair<string, object?>[] { new("type", "cluster_state") };
            this.operationsWriteCounter.Add(1, tags);

            var payload = this.serializer.SerializeToBytes(state);
            if (payload != null)
            {
                this.payloadBytesHistogram.Record(payload.Length, tags);
            }

            var entity = new TableEntity(this.GetClusterStatePartitionKey(), replicaId);
            CrdtTableEntity.SetPayload(entity, payload);

            await this.tableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Storage connection failed during SaveClusterStateAsync.");
            throw;
        }
    }

    public async Task<CrdtDocument<TState>?> LoadDocumentAsync<TState>(string shardId, string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);

        var payload = await this.LoadRawPayloadAsync(shardId, documentId, cancellationToken).ConfigureAwait(false);
        return payload is { Length: > 0 } ? this.serializer.DeserializeFromBytes<CrdtDocument<TState>>(payload) : null;
    }

    public async Task<CrdtDocument<TState>?> LoadDocumentFromAnyShardAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);

        var payload = await this.LoadRawPayloadFromAnyShardAsync(documentId, cancellationToken).ConfigureAwait(false);
        return payload is { Length: > 0 } ? this.serializer.DeserializeFromBytes<CrdtDocument<TState>>(payload) : null;
    }

    public async Task<byte[]?> LoadSerializedDocumentAsync(string shardId, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);

        if (!this.useBinarySerialization)
        {
            return null;
        }

        return await this.LoadRawPayloadAsync(shardId, documentId, cancellationToken).ConfigureAwait(false);
    }

    public async Task<byte[]?> LoadSerializedDocumentFromAnyShardAsync(string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);

        if (!this.useBinarySerialization)
        {
            return null;
        }

        return await this.LoadRawPayloadFromAnyShardAsync(documentId, cancellationToken).ConfigureAwait(false);
    }

    public async Task<CrdtDocument<TState>?> LoadOrphanedDocumentAsync<TState>(string shardId, string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);

        try
        {
            var localReplicaId = this.GetLocalReplicaId();
            var localPartitionKey = this.GetDocumentStatePartitionKey(shardId);
            var matchedEntities = new List<TableEntity>();

            var clusterStateTracker = this.serviceProvider.GetService<IClusterStateTracker>();
            if (clusterStateTracker != null)
            {
                var candidateReplicas = new HashSet<string>(clusterStateTracker.GetActiveReplicaIds(), StringComparer.Ordinal);
                var snapshot = clusterStateTracker.ExportState();
                foreach (var knownReplica in snapshot.PeerStates.Keys)
                {
                    candidateReplicas.Add(knownReplica);
                }

                if (!string.IsNullOrWhiteSpace(localReplicaId))
                {
                    candidateReplicas.Remove(localReplicaId);
                }

                if (candidateReplicas.Count == 1)
                {
                    foreach (var peerReplicaId in candidateReplicas)
                    {
                        var entity = await this.TryGetPeerDocumentEntityAsync(peerReplicaId, shardId, documentId, cancellationToken).ConfigureAwait(false);
                        if (entity != null)
                        {
                            matchedEntities.Add(entity);
                        }
                    }
                }
                else if (candidateReplicas.Count > 1)
                {
                    var tasks = new List<Task<TableEntity?>>(candidateReplicas.Count);
                    foreach (var peerReplicaId in candidateReplicas)
                    {
                        tasks.Add(this.TryGetPeerDocumentEntityAsync(peerReplicaId, shardId, documentId, cancellationToken));
                    }

                    var results = await Task.WhenAll(tasks).ConfigureAwait(false);
                    for (int i = 0; i < results.Length; i++)
                    {
                        if (results[i] != null)
                        {
                            matchedEntities.Add(results[i]!);
                        }
                    }
                }
            }

            var orchestrator = this.serviceProvider.GetService<ICrdtDocumentOrchestrator>();
            var isColdBoot = orchestrator == null || orchestrator.InitializationStage != OrchestratorInitializationStage.Initialized;

            if (matchedEntities.Count == 0 && isColdBoot)
            {
                // Fallback for cold boot before cluster state tracker has discovered peer replica IDs
                var expectedSuffix = $"_{DocumentStatePartitionKey}_{shardId}";
                var exactUnprefixedPartition = $"{DocumentStatePartitionKey}_{shardId}";
                var filter = TableClient.CreateQueryFilter($"RowKey eq {documentId}");
                var query = this.tableClient.QueryAsync<TableEntity>(filter, cancellationToken: cancellationToken);

                await foreach (var entity in query.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    if (entity.PartitionKey == null || string.Equals(entity.PartitionKey, localPartitionKey, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    if (entity.PartitionKey.EndsWith(expectedSuffix, StringComparison.Ordinal) ||
                        string.Equals(entity.PartitionKey, exactUnprefixedPartition, StringComparison.Ordinal) ||
                        entity.PartitionKey.Contains(DocumentStatePartitionKey, StringComparison.OrdinalIgnoreCase))
                    {
                        matchedEntities.Add(entity);
                    }
                }
            }

            if (matchedEntities.Count > 0)
            {
                matchedEntities.Sort((a, b) => Nullable.Compare(b.Timestamp, a.Timestamp));

                var tags = new KeyValuePair<string, object?>[] { new("type", "document_bootstrap") };
                CrdtDocument<TState>? mergedDocument = null;

                foreach (var entity in matchedEntities)
                {
                    this.operationsReadCounter.Add(1, tags);

                    var payload = CrdtTableEntity.GetPayload(entity);
                    if (payload == null || payload.Length == 0)
                    {
                        continue;
                    }

                    this.payloadBytesHistogram.Record(payload.Length, tags);
                    var doc = this.serializer.DeserializeFromBytes<CrdtDocument<TState>>(payload);
                    if (doc.Data == null)
                    {
                        continue;
                    }

                    if (mergedDocument == null)
                    {
                        mergedDocument = doc;
                    }
                    else if (this.merger != null)
                    {
                        await this.merger.MergeStateAsync(mergedDocument.Value, doc, cancellationToken).ConfigureAwait(false);
                    }
                }

                if (mergedDocument != null)
                {
                    this.logger.LogInformation("Bootstrapped and merged orphaned document '{DocumentId}' across {PartitionCount} peer partition(s).", documentId, matchedEntities.Count);
                    return mergedDocument;
                }
            }
        }
        catch (Exception ex)
        {
            this.logger.LogWarning(ex, "Failed to evaluate orphaned documents for '{DocumentId}'. Proceeding with empty initialization.", documentId);
        }
        
        return null;
    }

    public async Task SaveDocumentAsync<TState>(string shardId, string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default) where TState : class, new()
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);
        ArgumentNullException.ThrowIfNull(document);

        var payload = this.serializer.SerializeToBytes(document);
        await this.SavePayloadCoreAsync(shardId, documentId, payload, cancellationToken).ConfigureAwait(false);
    }

    public async Task SaveSerializedDocumentAsync<TState>(string shardId, string documentId, byte[] serializedDocument, CancellationToken cancellationToken = default) where TState : class, new()
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);
        ArgumentNullException.ThrowIfNull(serializedDocument);

        byte[] payload = serializedDocument;

        if (!this.useBinarySerialization && (serializedDocument.Length == 0 || serializedDocument[0] != (byte)'{'))
        {
            var defaultSerializer = this.serviceProvider.GetRequiredService<ICrdtSerializer>();
            var doc = defaultSerializer.DeserializeFromBytes<CrdtDocument<TState>>(serializedDocument);
            payload = this.serializer.SerializeToBytes(doc);
        }

        await this.SavePayloadCoreAsync(shardId, documentId, payload, cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteDocumentAsync(string shardId, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);

        try
        {
            await this.tableClient.DeleteEntityAsync(this.GetDocumentStatePartitionKey(shardId), documentId, cancellationToken: cancellationToken).ConfigureAwait(false);
            
            var tags = new KeyValuePair<string, object?>[] { new("type", "document") };
            this.operationsDeleteCounter.Add(1, tags);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // Ignore if not exists
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Storage connection failed during DeleteDocumentAsync.");
            throw;
        }
    }

    public void Append(IComparable documentId, IReadOnlyList<CrdtOperation> operations)
    {
        ArgumentNullException.ThrowIfNull(documentId);
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0) return;

        var docIdStr = documentId.ToString()!;
        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };
        this.operationsWriteCounter.Add(operations.Count, tags);

        var groupedByShard = operations.GroupBy(op => op.ShardId);

        foreach (var group in groupedByShard)
        {
            var partitionKey = this.GetJournalPartitionKey(group.Key);
            var batch = new List<TableTransactionAction>();

            foreach (var op in group)
            {
                var rowKey = GetJournalRowKey(op.ShardClock, op.ReplicaId);
                var journaledOp = new JournaledOperation(docIdStr, op);
                var payload = this.serializer.SerializeToBytes(journaledOp);

                if (payload != null)
                {
                    this.payloadBytesHistogram.Record(payload.Length, tags);
                }

                var entity = new TableEntity(partitionKey, rowKey);
                CrdtTableEntity.SetPayload(entity, payload);

                batch.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, entity));

                if (batch.Count == 100)
                {
                    this.tableClient.SubmitTransaction(batch);
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                this.tableClient.SubmitTransaction(batch);
            }
        }
    }

    public async Task AppendAsync(IComparable documentId, IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(documentId);
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0) return;

        var docIdStr = documentId.ToString()!;
        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };
        this.operationsWriteCounter.Add(operations.Count, tags);

        var groupedByShard = operations.GroupBy(op => op.ShardId);

        foreach (var group in groupedByShard)
        {
            var partitionKey = this.GetJournalPartitionKey(group.Key);
            var batch = new List<TableTransactionAction>();

            foreach (var op in group)
            {
                var rowKey = GetJournalRowKey(op.ShardClock, op.ReplicaId);
                var journaledOp = new JournaledOperation(docIdStr, op);
                var payload = this.serializer.SerializeToBytes(journaledOp);

                if (payload != null)
                {
                    this.payloadBytesHistogram.Record(payload.Length, tags);
                }

                var entity = new TableEntity(partitionKey, rowKey);
                CrdtTableEntity.SetPayload(entity, payload);

                batch.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, entity));

                if (batch.Count == 100)
                {
                    await this.tableClient.SubmitTransactionAsync(batch, cancellationToken).ConfigureAwait(false);
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await this.tableClient.SubmitTransactionAsync(batch, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async Task AppendBatchAsync(IReadOnlyList<JournaledOperation> operations, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0) return;

        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };
        this.operationsWriteCounter.Add(operations.Count, tags);

        var groupedByShard = operations.GroupBy(jOp => jOp.Operation.ShardId);

        foreach (var group in groupedByShard)
        {
            var partitionKey = this.GetJournalPartitionKey(group.Key);
            var batch = new List<TableTransactionAction>();
            var rowKeysInBatch = new HashSet<string>(StringComparer.Ordinal);

            foreach (var journaledOp in group)
            {
                var op = journaledOp.Operation;
                var rowKey = GetJournalRowKey(op.ShardClock, op.ReplicaId);
                var payload = this.serializer.SerializeToBytes(journaledOp);

                if (payload != null)
                {
                    this.payloadBytesHistogram.Record(payload.Length, tags);
                }

                var entity = new TableEntity(partitionKey, rowKey);
                CrdtTableEntity.SetPayload(entity, payload);

                if (!rowKeysInBatch.Add(rowKey))
                {
                    await this.tableClient.SubmitTransactionAsync(batch, cancellationToken).ConfigureAwait(false);
                    batch.Clear();
                    rowKeysInBatch.Clear();
                    rowKeysInBatch.Add(rowKey);
                }

                batch.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, entity));

                if (batch.Count == 100)
                {
                    await this.tableClient.SubmitTransactionAsync(batch, cancellationToken).ConfigureAwait(false);
                    batch.Clear();
                    rowKeysInBatch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await this.tableClient.SubmitTransactionAsync(batch, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string shardId, string originReplicaId, long minShardClock, long maxShardClock, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(shardId);
        ArgumentException.ThrowIfNullOrEmpty(originReplicaId);

        var partitionKey = this.GetJournalPartitionKey(shardId);
        var minRowKey = GetJournalRowKey(minShardClock, "");
        var maxRowKey = GetJournalRowKey(maxShardClock, "~");

        var filter = TableClient.CreateQueryFilter($"PartitionKey eq {partitionKey} and RowKey gt {minRowKey} and RowKey le {maxRowKey}");

        var query = this.tableClient.QueryAsync<TableEntity>(filter, cancellationToken: cancellationToken);
        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };

        await foreach (var entity in query.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            this.operationsReadCounter.Add(1, tags);

            var payload = CrdtTableEntity.GetPayload(entity);
            if (payload != null)
            {
                this.payloadBytesHistogram.Record(payload.Length, tags);
            }

            var op = this.serializer.DeserializeFromBytes<JournaledOperation>(payload);
            if (op != null && op.Operation.ReplicaId == originReplicaId)
            {
                yield return op;
            }
        }
    }

    public async IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string shardId, string originReplicaId, IEnumerable<long> shardClocks, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(shardId);
        ArgumentException.ThrowIfNullOrEmpty(originReplicaId);
        ArgumentNullException.ThrowIfNull(shardClocks);

        var partitionKey = this.GetJournalPartitionKey(shardId);
        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };

        foreach (var clock in shardClocks)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var rowKey = GetJournalRowKey(clock, originReplicaId);

            Response<TableEntity>? response = null;
            try
            {
                response = await this.tableClient.GetEntityAsync<TableEntity>(partitionKey, rowKey, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (RequestFailedException ex) when (ex.Status == 404 && ex.ErrorCode != "TableNotFound")
            {
                continue;
            }

            if (response?.Value != null)
            {
                this.operationsReadCounter.Add(1, tags);

                var payload = CrdtTableEntity.GetPayload(response.Value);
                if (payload != null)
                {
                    this.payloadBytesHistogram.Record(payload.Length, tags);
                }

                var op = this.serializer.DeserializeFromBytes<JournaledOperation>(payload);
                if (op != null)
                {
                    yield return op;
                }
            }
        }
    }

    public async Task<long> GetJournalCountAsync(string shardId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(shardId);
        
        var partitionKey = this.GetJournalPartitionKey(shardId);
        var filter = TableClient.CreateQueryFilter($"PartitionKey eq {partitionKey}");

        var query = this.tableClient.QueryAsync<TableEntity>(filter, select: new[] { "PartitionKey", "RowKey" }, cancellationToken: cancellationToken);
        
        long count = 0;
        var tags = new KeyValuePair<string, object?>[] { new("type", "journal_count") };

        await foreach (var _ in query.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            count++;
            this.operationsReadCounter.Add(1, tags);
        }

        return count;
    }

    public async IAsyncEnumerable<JournaledOperation> GetAllJournaledOperationsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var startPartition = this.GetJournalStartPartition();
        var endPartition = this.GetJournalEndPartition();

        var filter = TableClient.CreateQueryFilter($"PartitionKey ge {startPartition} and PartitionKey le {endPartition}");

        var query = this.tableClient.QueryAsync<TableEntity>(filter, cancellationToken: cancellationToken);
        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };

        await foreach (var entity in query.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            this.operationsReadCounter.Add(1, tags);

            var payload = CrdtTableEntity.GetPayload(entity);
            if (payload != null)
            {
                this.payloadBytesHistogram.Record(payload.Length, tags);
            }

            var op = this.serializer.DeserializeFromBytes<JournaledOperation>(payload);
            if (op != null)
            {
                yield return op;
            }
        }
    }

    public async Task TrimAsync(string shardId, IReadOnlyDictionary<string, long> globalMinimumVersionVector, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(shardId);
        ArgumentNullException.ThrowIfNull(globalMinimumVersionVector);
        if (globalMinimumVersionVector.Count == 0) return;

        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };
        var partitionKey = this.GetJournalPartitionKey(shardId);
        
        var maxClockOverall = globalMinimumVersionVector.Values.Max();
        var maxRowKey = GetJournalRowKey(maxClockOverall, "~");

        var filter = TableClient.CreateQueryFilter($"PartitionKey eq {partitionKey} and RowKey le {maxRowKey}");

        var query = this.tableClient.QueryAsync<TableEntity>(filter, select: new[] { "PartitionKey", "RowKey" }, cancellationToken: cancellationToken);

        var batch = new List<TableTransactionAction>();

        await foreach (var entity in query.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            // Parse replicaId and clock from RowKey (Format: {shardClock:D20}_{replicaId})
            var parts = entity.RowKey.Split('_', 2);
            if (parts.Length == 2 && long.TryParse(parts[0], out var clock))
            {
                var repId = parts[1];
                if (globalMinimumVersionVector.TryGetValue(repId, out var maxClockForReplica) && clock <= maxClockForReplica)
                {
                    this.operationsDeleteCounter.Add(1, tags);
                    batch.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity, ETag.All));

                    if (batch.Count == 100)
                    {
                        await this.tableClient.SubmitTransactionAsync(batch, cancellationToken).ConfigureAwait(false);
                        batch.Clear();
                    }
                }
            }
        }

        if (batch.Count > 0)
        {
            await this.tableClient.SubmitTransactionAsync(batch, cancellationToken).ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        this.meter.Dispose();
    }

    private string? GetLocalReplicaId()
    {
        try
        {
            var context = this.serviceProvider.GetService<ReplicaContext>();
            if (context != null && !string.IsNullOrWhiteSpace(context.ReplicaId))
            {
                return context.ReplicaId;
            }
        }
        catch (InvalidOperationException)
        {
            // Ignore scope validation failures and return null, defaulting to non-prefixed partitions
        }

        return null;
    }

    private string GetGlobalDvvPartitionKey()
    {
        var localId = this.GetLocalReplicaId();
        return localId != null ? $"{localId}_{GlobalDvvPartitionKey}" : GlobalDvvPartitionKey;
    }

    private string GetDocumentStatePartitionKey(string shardId)
    {
        var localId = this.GetLocalReplicaId();
        return localId != null ? $"{localId}_{DocumentStatePartitionKey}_{shardId}" : $"{DocumentStatePartitionKey}_{shardId}";
    }

    private string GetClusterStatePartitionKey()
    {
        var localId = this.GetLocalReplicaId();
        return localId != null ? $"{localId}_{ClusterStatePartitionKey}" : ClusterStatePartitionKey;
    }

    private string GetJournalPartitionKey(string shardId)
    {
        var localId = this.GetLocalReplicaId();
        return localId != null ? $"{localId}_{JournalPartitionPrefix}{shardId}" : $"{JournalPartitionPrefix}{shardId}";
    }

    private string GetJournalStartPartition()
    {
        var localId = this.GetLocalReplicaId();
        return localId != null ? $"{localId}_{JournalPartitionPrefix}" : JournalPartitionPrefix;
    }

    private string GetJournalEndPartition()
    {
        var localId = this.GetLocalReplicaId();
        return localId != null ? $"{localId}_{JournalPartitionPrefix}~" : JournalPartitionPrefix + "~";
    }

    private async Task SavePayloadCoreAsync(string shardId, string documentId, byte[] payload, CancellationToken cancellationToken)
    {
        try
        {
            var tags = new KeyValuePair<string, object?>[] { new("type", "document") };
            this.operationsWriteCounter.Add(1, tags);

            if (payload != null)
            {
                this.payloadBytesHistogram.Record(payload.Length, tags);
            }

            var entity = new TableEntity(this.GetDocumentStatePartitionKey(shardId), documentId);
            CrdtTableEntity.SetPayload(entity, payload);

            await this.tableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Storage connection failed during SaveDocumentAsync.");
            throw;
        }
    }

    private async Task<TableEntity?> TryGetPeerDocumentEntityAsync(
        string peerReplicaId,
        string shardId,
        string documentId,
        CancellationToken cancellationToken)
    {
        var peerPartitionKey = $"{peerReplicaId}_{DocumentStatePartitionKey}_{shardId}";
        try
        {
            var response = await this.tableClient.GetEntityAsync<TableEntity>(peerPartitionKey, documentId, cancellationToken: cancellationToken).ConfigureAwait(false);
            return response?.Value;
        }
        catch (RequestFailedException ex) when (ex.Status == 404 && ex.ErrorCode != "TableNotFound")
        {
            return null;
        }
    }

    private async Task<byte[]?> LoadRawPayloadAsync(string shardId, string documentId, CancellationToken cancellationToken)
    {
        try
        {
            var response = await this.tableClient.GetEntityAsync<TableEntity>(this.GetDocumentStatePartitionKey(shardId), documentId, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (response?.Value != null)
            {
                var tags = new KeyValuePair<string, object?>[] { new("type", "document") };
                this.operationsReadCounter.Add(1, tags);

                var payload = CrdtTableEntity.GetPayload(response.Value);
                if (payload != null)
                {
                    this.payloadBytesHistogram.Record(payload.Length, tags);
                }

                return payload;
            }
        }
        catch (RequestFailedException ex) when (ex.Status == 404 && ex.ErrorCode != "TableNotFound")
        {
            return null;
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Storage connection failed during LoadDocumentAsync. Failing fast to prevent state corruption.");
            throw;
        }

        return null;
    }

    private async Task<byte[]?> LoadRawPayloadFromAnyShardAsync(string documentId, CancellationToken cancellationToken)
    {
        try
        {
            var localReplicaId = this.GetLocalReplicaId();
            var localPrefix = localReplicaId != null
                ? $"{localReplicaId}_{DocumentStatePartitionKey}_"
                : $"{DocumentStatePartitionKey}_";
            var localPrefixEnd = $"{localPrefix}~";

            var filter = TableClient.CreateQueryFilter($"PartitionKey ge {localPrefix} and PartitionKey le {localPrefixEnd} and RowKey eq {documentId}");
            var query = this.tableClient.QueryAsync<TableEntity>(filter, cancellationToken: cancellationToken);

            TableEntity? latestEntity = null;
            await foreach (var entity in query.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (latestEntity == null || Nullable.Compare(entity.Timestamp, latestEntity.Timestamp) > 0)
                {
                    latestEntity = entity;
                }
            }

            if (latestEntity != null)
            {
                var tags = new KeyValuePair<string, object?>[] { new("type", "document") };
                this.operationsReadCounter.Add(1, tags);

                var payload = CrdtTableEntity.GetPayload(latestEntity);
                if (payload != null)
                {
                    this.payloadBytesHistogram.Record(payload.Length, tags);
                }

                return payload;
            }
        }
        catch (RequestFailedException ex) when (ex.Status == 404 && ex.ErrorCode != "TableNotFound")
        {
            return null;
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Storage connection failed during LoadDocumentFromAnyShardAsync. Failing fast to prevent state corruption.");
            throw;
        }

        return null;
    }
    
    private static string GetJournalRowKey(long shardClock, string replicaId) => $"{shardClock.ToString("D20", CultureInfo.InvariantCulture)}_{replicaId}";
}