namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

/// <summary>
/// Showcase implementation of a unified storage mechanism mapping the entire CRDT state tree, global DVV, and journaling locally via Native AOT friendly SQLite.
/// Utilizes the Single-Writer / Multiple-Reader WAL pattern maximizing extreme asynchronous throughput without DB lock contention.
/// </summary>
public sealed class ShowCaseCrdtStorage : IDistributedCrdtStorage, IDisposable
{
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<ShowCaseCrdtStorage> logger;
    private readonly string connectionString;
    
    // SQLite allows unlimited concurrent readers, but strictly 1 writer.
    // To prevent the managed Connection Pool from thrashing and throwing "database is locked" errors during heavy Hammer tests,
    // we dedicate a single permanently open connection for all writes and protect it with an in-memory Semaphore.
    private readonly SqliteConnection writeConnection;
    private readonly SemaphoreSlim writeLock = new(1, 1);
    private bool disposed;

    public ShowCaseCrdtStorage(
        ReplicaContext replicaContext,
        ICrdtSerializer serializer,
        ILogger<ShowCaseCrdtStorage> logger)
    {
        ArgumentNullException.ThrowIfNull(replicaContext);
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.connectionString = $"Data Source=showcase_crdt_{replicaContext.ReplicaId}.db;Cache=Shared;Pooling=True;";
        
        InitializeDatabase();

        // Establish the dedicated write connection
        this.writeConnection = new SqliteConnection(connectionString);
        this.writeConnection.Open();
    }

    public async Task<CrdtDocument<TState>?> LoadDocumentAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Value cannot be null or empty.", nameof(documentId));

        try
        {
            using var connection = new SqliteConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT Payload FROM Documents WHERE DocumentId = @DocId";
            AddParameter(command, "@DocId", documentId);

            var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (result is byte[] bytes)
            {
                return serializer.DeserializeFromBytes<CrdtDocument<TState>>(bytes);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load document '{DocumentId}' from SQLite", documentId);
        }

        return null;
    }

    public async Task SaveDocumentAsync<TState>(string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default) where TState : class, new()
    {
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Value cannot be null or empty.", nameof(documentId));

        var bytes = serializer.SerializeToBytes(document);

        await writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var command = writeConnection.CreateCommand();
            command.CommandText = "INSERT OR REPLACE INTO Documents (DocumentId, Payload) VALUES (@DocId, @Payload)";
            AddParameter(command, "@DocId", documentId);
            AddParameter(command, "@Payload", bytes);
            
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to save document '{DocumentId}' to SQLite", documentId);
        }
        finally
        {
            writeLock.Release();
        }
    }

    public async Task DeleteDocumentAsync(string documentId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(documentId)) return;

        await writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var transaction = writeConnection.BeginTransaction();

            using var cmd1 = writeConnection.CreateCommand();
            cmd1.Transaction = transaction;
            cmd1.CommandText = "DELETE FROM Documents WHERE DocumentId = @DocId";
            AddParameter(cmd1, "@DocId", documentId);
            await cmd1.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            using var cmd2 = writeConnection.CreateCommand();
            cmd2.Transaction = transaction;
            cmd2.CommandText = "DELETE FROM Journal WHERE DocumentId = @DocId";
            AddParameter(cmd2, "@DocId", documentId);
            await cmd2.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to completely actively delete mapped CRDT document '{DocumentId}' from SQLite", documentId);
        }
        finally
        {
            writeLock.Release();
        }
    }

    public async Task<DottedVersionVector?> LoadGlobalVersionVectorAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(replicaId)) throw new ArgumentException("Value cannot be null or empty.", nameof(replicaId));

        try
        {
            using var connection = new SqliteConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT Payload FROM GlobalDvv WHERE ReplicaId = @RepId";
            AddParameter(command, "@RepId", replicaId);

            var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (result is byte[] bytes)
            {
                return serializer.DeserializeFromBytes<DottedVersionVector>(bytes);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load global DVV for '{ReplicaId}' from SQLite", replicaId);
        }

        return null;
    }

    public async Task SaveGlobalVersionVectorAsync(string replicaId, DottedVersionVector globalVersionVector, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(replicaId)) throw new ArgumentException("Value cannot be null or empty.", nameof(replicaId));
        ArgumentNullException.ThrowIfNull(globalVersionVector);

        var bytes = serializer.SerializeToBytes(globalVersionVector);

        await writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var command = writeConnection.CreateCommand();
            command.CommandText = "INSERT OR REPLACE INTO GlobalDvv (ReplicaId, Payload) VALUES (@RepId, @Payload)";
            AddParameter(command, "@RepId", replicaId);
            AddParameter(command, "@Payload", bytes);
            
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to save global DVV to SQLite");
        }
        finally
        {
            writeLock.Release();
        }
    }

    public async Task<ClusterStateSnapshotDto?> LoadClusterStateAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(replicaId)) throw new ArgumentException("Value cannot be null or empty.", nameof(replicaId));

        try
        {
            using var connection = new SqliteConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT Payload FROM ClusterState WHERE ReplicaId = @RepId";
            AddParameter(command, "@RepId", replicaId);

            var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (result is byte[] bytes)
            {
                return serializer.DeserializeFromBytes<ClusterStateSnapshotDto>(bytes);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load explicit overarching cluster states structurally resolving amnesia limits safely natively.");
        }

        return null;
    }

    public async Task SaveClusterStateAsync(string replicaId, ClusterStateSnapshotDto state, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(replicaId)) throw new ArgumentException("Value cannot be null or empty.", nameof(replicaId));
        ArgumentNullException.ThrowIfNull(state);

        var bytes = serializer.SerializeToBytes(state);

        await writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var command = writeConnection.CreateCommand();
            command.CommandText = "INSERT OR REPLACE INTO ClusterState (ReplicaId, Payload) VALUES (@RepId, @Payload)";
            AddParameter(command, "@RepId", replicaId);
            AddParameter(command, "@Payload", bytes);
            
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to explicitly persist complete cluster map matrices preventing zombie edge cases seamlessly natively.");
        }
        finally
        {
            writeLock.Release();
        }
    }

    public void Append(string documentId, IReadOnlyList<CrdtOperation> operationsList)
    {
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Value cannot be null or empty.", nameof(documentId));
        ArgumentNullException.ThrowIfNull(operationsList);
        if (operationsList.Count == 0) return;

        writeLock.Wait();
        try
        {
            using var transaction = writeConnection.BeginTransaction();
            
            using var command = writeConnection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "INSERT OR IGNORE INTO Journal (Id, DocumentId, ReplicaId, GlobalClock, InsertedAt, Payload) VALUES (@Id, @DocId, @RepId, @Clock, @InsertedAt, @Payload)";

            var pId = command.CreateParameter(); pId.ParameterName = "@Id"; command.Parameters.Add(pId);
            var pDoc = command.CreateParameter(); pDoc.ParameterName = "@DocId"; command.Parameters.Add(pDoc);
            var pRep = command.CreateParameter(); pRep.ParameterName = "@RepId"; command.Parameters.Add(pRep);
            var pClock = command.CreateParameter(); pClock.ParameterName = "@Clock"; command.Parameters.Add(pClock);
            var pInsertedAt = command.CreateParameter(); pInsertedAt.ParameterName = "@InsertedAt"; command.Parameters.Add(pInsertedAt);
            var pPayload = command.CreateParameter(); pPayload.ParameterName = "@Payload"; command.Parameters.Add(pPayload);

            foreach (var op in operationsList)
            {
                var journaled = new JournaledOperation(documentId, op);
                pId.Value = op.Id;
                pDoc.Value = documentId;
                pRep.Value = op.ReplicaId;
                pClock.Value = op.GlobalClock;
                pInsertedAt.Value = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                pPayload.Value = serializer.SerializeToBytes(journaled);
                
                command.ExecuteNonQuery();
            }

            transaction.Commit();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to append synchronously to SQLite journal");
        }
        finally
        {
            writeLock.Release();
        }
    }

    public async Task AppendAsync(string documentId, IReadOnlyList<CrdtOperation> operationsList, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Value cannot be null or empty.", nameof(documentId));
        ArgumentNullException.ThrowIfNull(operationsList);
        if (operationsList.Count == 0) return;

        await writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var transaction = writeConnection.BeginTransaction();
            
            using var command = writeConnection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "INSERT OR IGNORE INTO Journal (Id, DocumentId, ReplicaId, GlobalClock, InsertedAt, Payload) VALUES (@Id, @DocId, @RepId, @Clock, @InsertedAt, @Payload)";

            var pId = command.CreateParameter(); pId.ParameterName = "@Id"; command.Parameters.Add(pId);
            var pDoc = command.CreateParameter(); pDoc.ParameterName = "@DocId"; command.Parameters.Add(pDoc);
            var pRep = command.CreateParameter(); pRep.ParameterName = "@RepId"; command.Parameters.Add(pRep);
            var pClock = command.CreateParameter(); pClock.ParameterName = "@Clock"; command.Parameters.Add(pClock);
            var pInsertedAt = command.CreateParameter(); pInsertedAt.ParameterName = "@InsertedAt"; command.Parameters.Add(pInsertedAt);
            var pPayload = command.CreateParameter(); pPayload.ParameterName = "@Payload"; command.Parameters.Add(pPayload);

            foreach (var op in operationsList)
            {
                var journaled = new JournaledOperation(documentId, op);
                pId.Value = op.Id;
                pDoc.Value = documentId;
                pRep.Value = op.ReplicaId;
                pClock.Value = op.GlobalClock;
                pInsertedAt.Value = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                pPayload.Value = serializer.SerializeToBytes(journaled);
                
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to asynchronously save operations to SQLite journal");
        }
        finally
        {
            writeLock.Release();
        }
    }

    public async IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string originReplicaId, long minGlobalClock, long maxGlobalClock, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(originReplicaId)) throw new ArgumentException("Value cannot be null or empty.", nameof(originReplicaId));

        using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT Payload FROM Journal WHERE ReplicaId = @RepId AND GlobalClock > @Min AND GlobalClock <= @Max ORDER BY GlobalClock ASC";
        AddParameter(command, "@RepId", originReplicaId);
        AddParameter(command, "@Min", minGlobalClock);
        AddParameter(command, "@Max", maxGlobalClock);

        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (reader["Payload"] is byte[] bytes)
            {
                var op = serializer.DeserializeFromBytes<JournaledOperation>(bytes);
                if (op != null)
                {
                    yield return op;
                }
            }
        }
    }

    public async IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string originReplicaId, IEnumerable<long> globalClocks, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(originReplicaId)) throw new ArgumentException("Value cannot be null or empty.", nameof(originReplicaId));
        ArgumentNullException.ThrowIfNull(globalClocks);

        var clocksList = globalClocks.ToList();
        if (clocksList.Count == 0) yield break;

        using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        foreach (var chunk in clocksList.Chunk(900))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var inClause = string.Join(",", chunk);
            
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT Payload FROM Journal WHERE ReplicaId = @RepId AND GlobalClock IN ({inClause}) ORDER BY GlobalClock ASC";
            AddParameter(command, "@RepId", originReplicaId);

            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (reader["Payload"] is byte[] bytes)
                {
                    var op = serializer.DeserializeFromBytes<JournaledOperation>(bytes);
                    if (op != null)
                    {
                        yield return op;
                    }
                }
            }
        }
    }

    public async IAsyncEnumerable<JournaledOperation> GetAllJournaledOperationsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT Payload FROM Journal ORDER BY GlobalClock ASC";

        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (reader["Payload"] is byte[] bytes)
            {
                var op = serializer.DeserializeFromBytes<JournaledOperation>(bytes);
                if (op != null)
                {
                    yield return op;
                }
            }
        }
    }

    public async Task<long> GetJournalCountAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var connection = new SqliteConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT COUNT(*) FROM Journal";

            var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return Convert.ToInt64(result);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get SQLite journal count");
            return 0;
        }
    }

    public void Trim(IReadOnlyDictionary<string, long> gmvv) => Trim(gmvv, null);

    public void Trim(IReadOnlyDictionary<string, long> gmvv, DateTimeOffset? retainBufferTime)
    {
        ArgumentNullException.ThrowIfNull(gmvv);
        if (gmvv.Count == 0) return;

        writeLock.Wait();
        try
        {
            using var transaction = writeConnection.BeginTransaction();
            
            using var command = writeConnection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "DELETE FROM Journal WHERE ReplicaId = @RepId AND GlobalClock <= @MaxClock";
            
            if (retainBufferTime.HasValue)
            {
                command.CommandText += " AND InsertedAt < @RetainTime";
                var pTime = command.CreateParameter(); 
                pTime.ParameterName = "@RetainTime"; 
                pTime.Value = retainBufferTime.Value.ToUnixTimeMilliseconds();
                command.Parameters.Add(pTime);
            }

            var pRepId = command.CreateParameter(); pRepId.ParameterName = "@RepId"; command.Parameters.Add(pRepId);
            var pMax = command.CreateParameter(); pMax.ParameterName = "@MaxClock"; command.Parameters.Add(pMax);

            foreach (var kvp in gmvv)
            {
                pRepId.Value = kvp.Key;
                pMax.Value = kvp.Value;
                command.ExecuteNonQuery();
            }

            transaction.Commit();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to synchronously trim SQLite journal natively");
        }
        finally
        {
            writeLock.Release();
        }
    }

    public Task TrimAsync(IReadOnlyDictionary<string, long> globalMinimumVersionVector, CancellationToken cancellationToken = default)
        => TrimAsync(globalMinimumVersionVector, null, cancellationToken);

    public async Task TrimAsync(IReadOnlyDictionary<string, long> globalMinimumVersionVector, DateTimeOffset? retainBufferTime, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(globalMinimumVersionVector);
        if (globalMinimumVersionVector.Count == 0) return;

        await writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var transaction = writeConnection.BeginTransaction();
            
            using var command = writeConnection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "DELETE FROM Journal WHERE ReplicaId = @RepId AND GlobalClock <= @MaxClock";
            
            if (retainBufferTime.HasValue)
            {
                command.CommandText += " AND InsertedAt < @RetainTime";
                var pTime = command.CreateParameter(); 
                pTime.ParameterName = "@RetainTime"; 
                pTime.Value = retainBufferTime.Value.ToUnixTimeMilliseconds();
                command.Parameters.Add(pTime);
            }

            var pRepId = command.CreateParameter(); pRepId.ParameterName = "@RepId"; command.Parameters.Add(pRepId);
            var pMax = command.CreateParameter(); pMax.ParameterName = "@MaxClock"; command.Parameters.Add(pMax);

            foreach (var kvp in globalMinimumVersionVector)
            {
                pRepId.Value = kvp.Key;
                pMax.Value = kvp.Value;
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to asynchronously trim SQLite journal gracefully");
        }
        finally
        {
            writeLock.Release();
        }
    }

    private void InitializeDatabase()
    {
        try
        {
            using var connection = new SqliteConnection(connectionString);
            connection.Open();
            
            using var initCommand = connection.CreateCommand();
            initCommand.CommandText = "PRAGMA journal_mode = 'wal'; PRAGMA synchronous = OFF; PRAGMA temp_store = MEMORY;";
            initCommand.ExecuteNonQuery();

            using var command = connection.CreateCommand();
            command.CommandText = @"
                CREATE TABLE IF NOT EXISTS Documents (
                    DocumentId TEXT PRIMARY KEY,
                    Payload BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS GlobalDvv (
                    ReplicaId TEXT PRIMARY KEY,
                    Payload BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ClusterState (
                    ReplicaId TEXT PRIMARY KEY,
                    Payload BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS Journal (
                    Id TEXT PRIMARY KEY,
                    DocumentId TEXT NOT NULL,
                    ReplicaId TEXT NOT NULL,
                    GlobalClock INTEGER NOT NULL,
                    Payload BLOB NOT NULL
                );
                CREATE INDEX IF NOT EXISTS IDX_Journal_Replica_Clock ON Journal(ReplicaId, GlobalClock);
            ";
            command.ExecuteNonQuery();

            // Safely migrate mapping bounds appending InsertedAt tracking explicitly for temporal delays gracefully
            using var checkCmd = connection.CreateCommand();
            checkCmd.CommandText = "PRAGMA table_info(Journal);";
            using var reader = checkCmd.ExecuteReader();
            bool hasInsertedAt = false;
            while(reader.Read())
            {
                if (reader["name"].ToString() == "InsertedAt") hasInsertedAt = true;
            }

            if (!hasInsertedAt)
            {
                using var alterCmd = connection.CreateCommand();
                alterCmd.CommandText = "ALTER TABLE Journal ADD COLUMN InsertedAt INTEGER NOT NULL DEFAULT 0;";
                alterCmd.ExecuteNonQuery();
            }
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Failed to natively structurally initialize SQLite showcase schema");
            throw;
        }
    }

    private static void AddParameter(SqliteCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }

    public void Dispose()
    {
        if (disposed) return;
        disposed = true;
        
        writeConnection.Dispose();
        writeLock.Dispose();
    }
}