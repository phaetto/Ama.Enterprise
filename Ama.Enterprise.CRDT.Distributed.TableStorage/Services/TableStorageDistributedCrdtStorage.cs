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

        if (storageOptions.UseBinarySerialization)
        {
            this.serializer = availableSerializers.FirstOrDefault(s => s.GetType() != typeof(JsonCrdtSerializer)) 
                ?? throw new InvalidOperationException("Binary serialization was requested for Table Storage, but no alternative ICrdtSerializer (e.g., MessagePack) was found in the DI container.");
        }
        else
        {
            this.serializer = textJsonSerializer ?? throw new ArgumentNullException(nameof(textJsonSerializer));
        }

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

    public async Task<DottedVersionVector?> LoadGlobalVersionVectorAsync(string replicaId, CancellationToken cancellationToken = default)
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

                return this.serializer.DeserializeFromBytes<DottedVersionVector>(payload);
            }
        }
        catch (RequestFailedException ex) when (ex.Status == 404 && ex.ErrorCode != "TableNotFound")
        {
            return null;
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Storage connection failed during LoadGlobalVersionVectorAsync. Failing fast to prevent state corruption.");
            throw;
        }

        return null;
    }

    public async Task SaveGlobalVersionVectorAsync(string replicaId, DottedVersionVector globalVersionVector, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(globalVersionVector);

        try
        {
            var tags = new KeyValuePair<string, object?>[] { new("type", "dvv") };
            this.operationsWriteCounter.Add(1, tags);

            var payload = this.serializer.SerializeToBytes(globalVersionVector);
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
            this.logger.LogError(ex, "Storage connection failed during SaveGlobalVersionVectorAsync.");
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

    public async Task<CrdtDocument<TState>?> LoadDocumentAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);

        try
        {
            var response = await this.tableClient.GetEntityAsync<TableEntity>(this.GetDocumentStatePartitionKey(), documentId, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (response?.Value != null)
            {
                var tags = new KeyValuePair<string, object?>[] { new("type", "document") };
                this.operationsReadCounter.Add(1, tags);

                var payload = CrdtTableEntity.GetPayload(response.Value);
                if (payload != null)
                {
                    this.payloadBytesHistogram.Record(payload.Length, tags);
                }

                return this.serializer.DeserializeFromBytes<CrdtDocument<TState>>(payload);
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

    public async Task<CrdtDocument<TState>?> LoadOrphanedDocumentAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);

        try
        {
            // Query for any RowKey matching the documentId across all partitions
            var filter = TableClient.CreateQueryFilter($"RowKey eq {documentId}");
            var query = this.tableClient.QueryAsync<TableEntity>(filter, cancellationToken: cancellationToken);
            
            TableEntity? latestOrphan = null;
            
            await foreach (var entity in query.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (entity.PartitionKey != null && entity.PartitionKey.EndsWith(DocumentStatePartitionKey, StringComparison.OrdinalIgnoreCase))
                {
                    if (latestOrphan == null || entity.Timestamp > latestOrphan.Timestamp)
                    {
                        latestOrphan = entity;
                    }
                }
            }
            
            if (latestOrphan != null)
            {
                var tags = new KeyValuePair<string, object?>[] { new("type", "document_bootstrap") };
                this.operationsReadCounter.Add(1, tags);
                
                var payload = CrdtTableEntity.GetPayload(latestOrphan);
                if (payload != null)
                {
                    this.payloadBytesHistogram.Record(payload.Length, tags);
                }
                
                var document = this.serializer.DeserializeFromBytes<CrdtDocument<TState>>(payload);
                if (document != null)
                {
                    this.logger.LogInformation("Successfully bootstrapped orphaned document '{DocumentId}' from partition '{PartitionKey}'.", documentId, latestOrphan.PartitionKey);
                    return document;
                }
            }
        }
        catch (Exception ex)
        {
            this.logger.LogWarning(ex, "Failed to evaluate orphaned documents for '{DocumentId}'. Proceeding with empty initialization.", documentId);
        }
        
        return null;
    }

    public async Task SaveDocumentAsync<TState>(string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default) where TState : class, new()
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);
        ArgumentNullException.ThrowIfNull(document);

        try
        {
            var tags = new KeyValuePair<string, object?>[] { new("type", "document") };
            this.operationsWriteCounter.Add(1, tags);

            var payload = this.serializer.SerializeToBytes(document);
            if (payload != null)
            {
                this.payloadBytesHistogram.Record(payload.Length, tags);
            }

            var entity = new TableEntity(this.GetDocumentStatePartitionKey(), documentId);
            CrdtTableEntity.SetPayload(entity, payload);

            await this.tableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Storage connection failed during SaveDocumentAsync.");
            throw;
        }
    }

    public async Task DeleteDocumentAsync(string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);

        try
        {
            await this.tableClient.DeleteEntityAsync(this.GetDocumentStatePartitionKey(), documentId, cancellationToken: cancellationToken).ConfigureAwait(false);
            
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

    public void Append(string documentId, IReadOnlyList<CrdtOperation> operations)
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0) return;

        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };
        this.operationsWriteCounter.Add(operations.Count, tags);

        var groupedByReplica = operations.GroupBy(op => op.ReplicaId);

        foreach (var group in groupedByReplica)
        {
            var partitionKey = this.GetJournalPartitionKey(group.Key);
            var batch = new List<TableTransactionAction>();

            foreach (var op in group)
            {
                var rowKey = GetJournalRowKey(op.GlobalClock);
                var journaledOp = new JournaledOperation(documentId, op);
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

    public async Task AppendAsync(string documentId, IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(documentId);
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0) return;

        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };
        this.operationsWriteCounter.Add(operations.Count, tags);

        var groupedByReplica = operations.GroupBy(op => op.ReplicaId);

        foreach (var group in groupedByReplica)
        {
            var partitionKey = this.GetJournalPartitionKey(group.Key);
            var batch = new List<TableTransactionAction>();

            foreach (var op in group)
            {
                var rowKey = GetJournalRowKey(op.GlobalClock);
                var journaledOp = new JournaledOperation(documentId, op);
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

    public async IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string originReplicaId, long minGlobalClock, long maxGlobalClock, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(originReplicaId);

        var partitionKey = this.GetJournalPartitionKey(originReplicaId);
        var minRowKey = GetJournalRowKey(minGlobalClock);
        var maxRowKey = GetJournalRowKey(maxGlobalClock);

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
            if (op != null)
            {
                yield return op;
            }
        }
    }

    public async IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string originReplicaId, IEnumerable<long> globalClocks, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(originReplicaId);
        ArgumentNullException.ThrowIfNull(globalClocks);

        var partitionKey = this.GetJournalPartitionKey(originReplicaId);
        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };

        foreach (var clock in globalClocks)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var rowKey = GetJournalRowKey(clock);

            Response<TableEntity>? response = null;
            try
            {
                response = await this.tableClient.GetEntityAsync<TableEntity>(partitionKey, rowKey, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (RequestFailedException ex) when (ex.Status == 404 && ex.ErrorCode != "TableNotFound")
            {
                // Unregistered dot bypassed.
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

    public async Task<long> GetJournalCountAsync(CancellationToken cancellationToken = default)
    {
        var startPartition = this.GetJournalStartPartition();
        var endPartition = this.GetJournalEndPartition();

        var filter = TableClient.CreateQueryFilter($"PartitionKey ge {startPartition} and PartitionKey le {endPartition}");

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

    public async Task TrimAsync(IReadOnlyDictionary<string, long> globalMinimumVersionVector, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(globalMinimumVersionVector);
        if (globalMinimumVersionVector.Count == 0) return;

        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };

        foreach (var kvp in globalMinimumVersionVector)
        {
            var replicaId = kvp.Key;
            var maxClock = kvp.Value;

            var partitionKey = this.GetJournalPartitionKey(replicaId);
            var maxRowKey = GetJournalRowKey(maxClock);

            var filter = TableClient.CreateQueryFilter($"PartitionKey eq {partitionKey} and RowKey le {maxRowKey}");

            var query = this.tableClient.QueryAsync<TableEntity>(filter, select: new[] { "PartitionKey", "RowKey" }, cancellationToken: cancellationToken);

            var batch = new List<TableTransactionAction>();

            await foreach (var entity in query.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                this.operationsDeleteCounter.Add(1, tags);
                
                // Azure Table Storage requires an ETag for Delete operations. 
                // Since we restrict the query with 'select', the entity's ETag is not populated. 
                // We use ETag.All to bypass the check and force delete the entry safely.
                batch.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity, ETag.All));

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

    private string GetDocumentStatePartitionKey()
    {
        var localId = this.GetLocalReplicaId();
        return localId != null ? $"{localId}_{DocumentStatePartitionKey}" : DocumentStatePartitionKey;
    }

    private string GetClusterStatePartitionKey()
    {
        var localId = this.GetLocalReplicaId();
        return localId != null ? $"{localId}_{ClusterStatePartitionKey}" : ClusterStatePartitionKey;
    }

    private string GetJournalPartitionKey(string originReplicaId)
    {
        var localId = this.GetLocalReplicaId();
        return localId != null ? $"{localId}_{JournalPartitionPrefix}{originReplicaId}" : $"{JournalPartitionPrefix}{originReplicaId}";
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
    
    private static string GetJournalRowKey(long globalClock) => globalClock.ToString("D20", CultureInfo.InvariantCulture);
}