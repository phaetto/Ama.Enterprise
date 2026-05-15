namespace Ama.Enterprise.CRDT.Distributed.TableStorage.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.TableStorage.Models;

/// <summary>
/// A centralized unified Azure Table Storage implementation capturing overarching CRDT trees bounding asynchronously mapped DVV tracking bounds.
/// </summary>
public sealed class TableStorageDistributedCrdtStorage : IDistributedCrdtStorage, IDisposable
{
    private const string GlobalDvvPartitionKey = "GlobalDVV";
    private const string DocumentStatePartitionKey = "DocumentState";
    private const string JournalPartitionPrefix = "Journal_";

    private readonly TableServiceClient tableServiceClient;
    private readonly TableClient tableClient;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<TableStorageDistributedCrdtStorage> logger;

    private readonly Meter meter;
    private readonly Counter<long> operationsReadCounter;
    private readonly Counter<long> operationsWriteCounter;
    private readonly Counter<long> operationsDeleteCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    public TableStorageDistributedCrdtStorage(
        IOptions<TableStorageCrdtOptions> options,
        ICrdtSerializer serializer,
        ILogger<TableStorageDistributedCrdtStorage> logger,
        IMeterFactory? meterFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        var storageOptions = options.Value ?? throw new ArgumentException("Options value cannot be null.", nameof(options));

        if (string.IsNullOrEmpty(storageOptions.ConnectionString))
        {
            throw new ArgumentException("Table storage connection string cannot be null or empty.", nameof(options));
        }

        if (string.IsNullOrEmpty(storageOptions.TableName))
        {
            throw new ArgumentException("Table name cannot be null or empty.", nameof(options));
        }

        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.tableServiceClient = new TableServiceClient(storageOptions.ConnectionString);
        this.tableClient = this.tableServiceClient.GetTableClient(storageOptions.TableName);

        if (storageOptions.CreateTableIfNotExists)
        {
            this.tableClient.CreateIfNotExists();
        }

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
        if (string.IsNullOrEmpty(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));

        try
        {
            var response = await this.tableClient.GetEntityAsync<CrdtTableEntity>(GlobalDvvPartitionKey, replicaId, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (response?.Value != null)
            {
                var tags = new KeyValuePair<string, object?>[] { new("type", "dvv") };
                this.operationsReadCounter.Add(1, tags);

                var payload = response.Value.GetPayload();
                if (payload != null)
                {
                    this.payloadBytesHistogram.Record(payload.Length, tags);
                }

                return this.serializer.DeserializeFromBytes<DottedVersionVector>(payload);
            }
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Failed to load global DVV.");
        }

        return null;
    }

    public async Task SaveGlobalVersionVectorAsync(string replicaId, DottedVersionVector globalVersionVector, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));
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

            var entity = new CrdtTableEntity
            {
                PartitionKey = GlobalDvvPartitionKey,
                RowKey = replicaId
            };
            entity.SetPayload(payload);

            await this.tableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Failed to save global DVV.");
        }
    }

    public async Task<CrdtDocument<TState>?> LoadDocumentAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        try
        {
            var response = await this.tableClient.GetEntityAsync<CrdtTableEntity>(DocumentStatePartitionKey, documentId, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (response?.Value != null)
            {
                var tags = new KeyValuePair<string, object?>[] { new("type", "document") };
                this.operationsReadCounter.Add(1, tags);

                var payload = response.Value.GetPayload();
                if (payload != null)
                {
                    this.payloadBytesHistogram.Record(payload.Length, tags);
                }

                return this.serializer.DeserializeFromBytes<CrdtDocument<TState>>(payload);
            }
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Failed to load document.");
        }

        return null;
    }

    public async Task SaveDocumentAsync<TState>(string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default) where TState : class, new()
    {
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        if (document == null) throw new ArgumentNullException(nameof(document));

        try
        {
            var tags = new KeyValuePair<string, object?>[] { new("type", "document") };
            this.operationsWriteCounter.Add(1, tags);

            var payload = this.serializer.SerializeToBytes(document);
            if (payload != null)
            {
                this.payloadBytesHistogram.Record(payload.Length, tags);
            }

            var entity = new CrdtTableEntity
            {
                PartitionKey = DocumentStatePartitionKey,
                RowKey = documentId
            };
            entity.SetPayload(payload);

            await this.tableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Failed to save document.");
        }
    }

    public async Task DeleteDocumentAsync(string documentId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        try
        {
            await this.tableClient.DeleteEntityAsync(DocumentStatePartitionKey, documentId, cancellationToken: cancellationToken).ConfigureAwait(false);
            
            var tags = new KeyValuePair<string, object?>[] { new("type", "document") };
            this.operationsDeleteCounter.Add(1, tags);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // Ignore if not exists
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Failed to delete document state.");
        }
    }

    public void Append(string documentId, IReadOnlyList<CrdtOperation> operations)
    {
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0) return;

        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };
        this.operationsWriteCounter.Add(operations.Count, tags);

        var groupedByReplica = operations.GroupBy(op => op.ReplicaId);

        foreach (var group in groupedByReplica)
        {
            var partitionKey = GetJournalPartitionKey(group.Key);
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

                var entity = new CrdtTableEntity
                {
                    PartitionKey = partitionKey,
                    RowKey = rowKey
                };
                entity.SetPayload(payload);

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
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0) return;

        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };
        this.operationsWriteCounter.Add(operations.Count, tags);

        var groupedByReplica = operations.GroupBy(op => op.ReplicaId);

        foreach (var group in groupedByReplica)
        {
            var partitionKey = GetJournalPartitionKey(group.Key);
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

                var entity = new CrdtTableEntity
                {
                    PartitionKey = partitionKey,
                    RowKey = rowKey
                };
                entity.SetPayload(payload);

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
        if (string.IsNullOrEmpty(originReplicaId)) throw new ArgumentException("Origin replica ID cannot be null or empty.", nameof(originReplicaId));

        var partitionKey = GetJournalPartitionKey(originReplicaId);
        var minRowKey = GetJournalRowKey(minGlobalClock);
        var maxRowKey = GetJournalRowKey(maxGlobalClock);

        var filter = $"PartitionKey eq '{partitionKey}' and RowKey gt '{minRowKey}' and RowKey le '{maxRowKey}'";

        var query = this.tableClient.QueryAsync<CrdtTableEntity>(filter, cancellationToken: cancellationToken);
        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };

        await foreach (var entity in query.WithCancellation(cancellationToken))
        {
            this.operationsReadCounter.Add(1, tags);

            var payload = entity.GetPayload();
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
        if (string.IsNullOrEmpty(originReplicaId)) throw new ArgumentException("Origin replica ID cannot be null or empty.", nameof(originReplicaId));
        ArgumentNullException.ThrowIfNull(globalClocks);

        var partitionKey = GetJournalPartitionKey(originReplicaId);
        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };

        foreach (var clock in globalClocks)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var rowKey = GetJournalRowKey(clock);

            Response<CrdtTableEntity>? response = null;
            try
            {
                response = await this.tableClient.GetEntityAsync<CrdtTableEntity>(partitionKey, rowKey, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                // Unregistered dot bypassed.
                continue;
            }

            if (response?.Value != null)
            {
                this.operationsReadCounter.Add(1, tags);

                var payload = response.Value.GetPayload();
                if (payload != null)
                {
                    this.payloadBytesHistogram.Record(payload.Length, tags);
                }

                var op = this.serializer.DeserializeFromBytes<JournaledOperation>(payload);
                yield return op;
            }
        }
    }

    public async Task<long> GetJournalCountAsync(CancellationToken cancellationToken = default)
    {
        var startPartition = JournalPartitionPrefix;
        var endPartition = JournalPartitionPrefix + "~";

        var filter = $"PartitionKey ge '{startPartition}' and PartitionKey le '{endPartition}'";

        var query = this.tableClient.QueryAsync<CrdtTableEntity>(filter, select: new[] { "PartitionKey", "RowKey" }, cancellationToken: cancellationToken);
        
        long count = 0;
        var tags = new KeyValuePair<string, object?>[] { new("type", "journal_count") };

        await foreach (var _ in query.WithCancellation(cancellationToken))
        {
            count++;
            this.operationsReadCounter.Add(1, tags);
        }

        return count;
    }

    public async IAsyncEnumerable<JournaledOperation> GetAllJournaledOperationsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var startPartition = JournalPartitionPrefix;
        var endPartition = JournalPartitionPrefix + "~";

        var filter = $"PartitionKey ge '{startPartition}' and PartitionKey le '{endPartition}'";

        var query = this.tableClient.QueryAsync<CrdtTableEntity>(filter, cancellationToken: cancellationToken);
        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };

        await foreach (var entity in query.WithCancellation(cancellationToken))
        {
            this.operationsReadCounter.Add(1, tags);

            var payload = entity.GetPayload();
            if (payload != null)
            {
                this.payloadBytesHistogram.Record(payload.Length, tags);
            }

            var op = this.serializer.DeserializeFromBytes<JournaledOperation>(payload);
            yield return op;
        }
    }

    public async Task TrimAsync(IReadOnlyDictionary<string, long> globalMinimumVersionVector, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(globalMinimumVersionVector);

        var tags = new KeyValuePair<string, object?>[] { new("type", "journal") };

        foreach (var kvp in globalMinimumVersionVector)
        {
            var replicaId = kvp.Key;
            var maxClock = kvp.Value;

            var partitionKey = GetJournalPartitionKey(replicaId);
            var maxRowKey = GetJournalRowKey(maxClock);

            var filter = $"PartitionKey eq '{partitionKey}' and RowKey le '{maxRowKey}'";

            var query = this.tableClient.QueryAsync<CrdtTableEntity>(filter, cancellationToken: cancellationToken);

            var batch = new List<TableTransactionAction>();

            await foreach (var entity in query.WithCancellation(cancellationToken))
            {
                this.operationsDeleteCounter.Add(1, tags);
                batch.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity));

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

    private static string GetJournalPartitionKey(string replicaId) => $"{JournalPartitionPrefix}{replicaId}";
    
    private static string GetJournalRowKey(long globalClock) => globalClock.ToString("D20");

    public void Dispose()
    {
        this.meter.Dispose();
    }
}