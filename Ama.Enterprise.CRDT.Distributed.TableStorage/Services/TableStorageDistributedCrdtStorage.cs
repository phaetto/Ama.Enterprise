namespace Ama.Enterprise.CRDT.Distributed.TableStorage.Services;

using System;
using System.Collections.Generic;
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
public sealed class TableStorageDistributedCrdtStorage : IDistributedCrdtStorage
{
    private const string GlobalDvvPartitionKey = "GlobalDVV";
    private const string DocumentStatePartitionKey = "DocumentState";
    private const string JournalPartitionPrefix = "Journal_";

    private readonly TableServiceClient tableServiceClient;
    private readonly TableClient tableClient;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<TableStorageDistributedCrdtStorage> logger;

    public TableStorageDistributedCrdtStorage(
        IOptions<TableStorageCrdtOptions> options,
        ICrdtSerializer serializer,
        ILogger<TableStorageDistributedCrdtStorage> logger)
    {
        if (options == null) throw new ArgumentNullException(nameof(options));
        
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
    }

    public async Task<DottedVersionVector?> LoadGlobalVersionVectorAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));

        try
        {
            var response = await this.tableClient.GetEntityAsync<CrdtTableEntity>(GlobalDvvPartitionKey, replicaId, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (response?.Value != null)
            {
                var payload = response.Value.GetPayload();
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
        if (globalVersionVector == null) throw new ArgumentNullException(nameof(globalVersionVector));

        try
        {
            var payload = this.serializer.SerializeToBytes(globalVersionVector);
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
                var payload = response.Value.GetPayload();
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
            var payload = this.serializer.SerializeToBytes(document);
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

    public void Append(string documentId, IReadOnlyList<CrdtOperation> operations)
    {
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        if (operations == null) throw new ArgumentNullException(nameof(operations));
        if (operations.Count == 0) return;

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
        if (operations == null) throw new ArgumentNullException(nameof(operations));
        if (operations.Count == 0) return;

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

        await foreach (var entity in query.WithCancellation(cancellationToken))
        {
            var payload = entity.GetPayload();
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
        if (globalClocks == null) throw new ArgumentNullException(nameof(globalClocks));

        var partitionKey = GetJournalPartitionKey(originReplicaId);

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
                var payload = response.Value.GetPayload();
                var op = this.serializer.DeserializeFromBytes<JournaledOperation>(payload);
                yield return op;
            }
        }
    }

    public async IAsyncEnumerable<JournaledOperation> GetAllJournaledOperationsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var startPartition = JournalPartitionPrefix;
        var endPartition = JournalPartitionPrefix + "~";

        var filter = $"PartitionKey ge '{startPartition}' and PartitionKey le '{endPartition}'";

        var query = this.tableClient.QueryAsync<CrdtTableEntity>(filter, cancellationToken: cancellationToken);

        await foreach (var entity in query.WithCancellation(cancellationToken))
        {
            var payload = entity.GetPayload();
            var op = this.serializer.DeserializeFromBytes<JournaledOperation>(payload);
            yield return op;
        }
    }

    public async Task TrimAsync(IReadOnlyDictionary<string, long> globalMinimumVersionVector, CancellationToken cancellationToken = default)
    {
        if (globalMinimumVersionVector == null) throw new ArgumentNullException(nameof(globalMinimumVersionVector));

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
}