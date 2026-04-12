namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Showcase implementation of a unified storage mechanism mapping the entire CRDT state tree, global DVV, and journaling natively locally.
/// </summary>
public sealed class ShowCaseCrdtStorage : IDistributedCrdtStorage, IDisposable
{
    private readonly IOptionsMonitor<DistributedCrdtOptions> optionsMonitor;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<ShowCaseCrdtStorage> logger;
    private readonly IDisposable? optionsChangeToken;
    
    private readonly List<JournaledOperation> journal = new();
    private readonly object syncRoot = new();

    private string loadedReplicaId;

    public ShowCaseCrdtStorage(
        IOptionsMonitor<DistributedCrdtOptions> optionsMonitor,
        ICrdtSerializer serializer,
        ILogger<ShowCaseCrdtStorage> logger)
    {
        this.optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.loadedReplicaId = this.optionsMonitor.CurrentValue.ReplicaId;
        
        LoadJournalSynchronously();

        this.optionsChangeToken = this.optionsMonitor.OnChange(OnOptionsChanged);
    }

    private void OnOptionsChanged(DistributedCrdtOptions newOptions)
    {
        lock (syncRoot)
        {
            if (!string.Equals(this.loadedReplicaId, newOptions.ReplicaId, StringComparison.Ordinal))
            {
                this.logger.LogInformation("ReplicaId changed from {OldId} to {NewId}. Reloading journal.", this.loadedReplicaId, newOptions.ReplicaId);
                this.loadedReplicaId = newOptions.ReplicaId;
                this.journal.Clear();
                LoadJournalSynchronously();
            }
        }
    }

    public async Task<CrdtDocument<TState>?> LoadDocumentAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Value cannot be null or empty.", nameof(documentId));

        var filePath = GetDocumentFilePath(documentId);
        if (!File.Exists(filePath))
        {
            return null;
        }

        try
        {
            var bytes = await File.ReadAllBytesAsync(filePath, cancellationToken).ConfigureAwait(false);
            return serializer.DeserializeFromBytes<CrdtDocument<TState>>(bytes);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load document from {FilePath}", filePath);
            return null;
        }
    }

    public async Task SaveDocumentAsync<TState>(string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default) where TState : class, new()
    {
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Value cannot be null or empty.", nameof(documentId));
        if (document == null) throw new ArgumentNullException(nameof(document));

        var filePath = GetDocumentFilePath(documentId);
        try
        {
            var bytes = serializer.SerializeToBytes(document);
            await File.WriteAllBytesAsync(filePath, bytes, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to save document to {FilePath}", filePath);
        }
    }

    public async Task<DottedVersionVector?> LoadGlobalVersionVectorAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(replicaId)) throw new ArgumentException("Value cannot be null or empty.", nameof(replicaId));

        var filePath = GetGlobalFilePath(replicaId);
        if (!File.Exists(filePath))
        {
            return null;
        }

        try
        {
            var bytes = await File.ReadAllBytesAsync(filePath, cancellationToken).ConfigureAwait(false);
            return serializer.DeserializeFromBytes<DottedVersionVector>(bytes);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load global DVV from {FilePath}", filePath);
            return null;
        }
    }

    public async Task SaveGlobalVersionVectorAsync(string replicaId, DottedVersionVector globalVersionVector, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(replicaId)) throw new ArgumentException("Value cannot be null or empty.", nameof(replicaId));
        if (globalVersionVector == null) throw new ArgumentNullException(nameof(globalVersionVector));

        var filePath = GetGlobalFilePath(replicaId);
        try
        {
            var bytes = serializer.SerializeToBytes(globalVersionVector);
            await File.WriteAllBytesAsync(filePath, bytes, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to save global DVV to {FilePath}", filePath);
        }
    }

    public void Append(string documentId, IReadOnlyList<CrdtOperation> operationsList)
    {
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Value cannot be null or empty.", nameof(documentId));
        if (operationsList == null) throw new ArgumentNullException(nameof(operationsList));

        lock (syncRoot)
        {
            var added = false;
            foreach (var op in operationsList)
            {
                if (!journal.Any(o => o.Operation.Id == op.Id))
                {
                    journal.Add(new JournaledOperation(documentId, op));
                    added = true;
                }
            }

            if (added)
            {
                SaveJournalSynchronously();
            }
        }
    }

    public async Task AppendAsync(string documentId, IReadOnlyList<CrdtOperation> operationsList, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(documentId)) throw new ArgumentException("Value cannot be null or empty.", nameof(documentId));
        if (operationsList == null) throw new ArgumentNullException(nameof(operationsList));

        bool added = false;
        List<JournaledOperation> snapshot;
        string filePath;

        lock (syncRoot)
        {
            foreach (var op in operationsList)
            {
                if (!journal.Any(o => o.Operation.Id == op.Id))
                {
                    journal.Add(new JournaledOperation(documentId, op));
                    added = true;
                }
            }
            snapshot = journal.ToList();
            filePath = GetJournalFilePath();
        }

        if (added)
        {
            try
            {
                var bytes = serializer.SerializeToBytes(snapshot);
                await File.WriteAllBytesAsync(filePath, bytes, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to asynchronously save journal to {FilePath}", filePath);
            }
        }
    }

    public async IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string originReplicaId, long minGlobalClock, long maxGlobalClock, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(originReplicaId)) throw new ArgumentException("Value cannot be null or empty.", nameof(originReplicaId));

        List<JournaledOperation> snapshot;
        lock (syncRoot) { snapshot = journal.ToList(); }

        foreach (var op in snapshot.Where(o => o.Operation.ReplicaId == originReplicaId && o.Operation.GlobalClock > minGlobalClock && o.Operation.GlobalClock <= maxGlobalClock))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return op;
        }
        await Task.CompletedTask;
    }

    public async IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string originReplicaId, IEnumerable<long> globalClocks, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(originReplicaId)) throw new ArgumentException("Value cannot be null or empty.", nameof(originReplicaId));
        if (globalClocks == null) throw new ArgumentNullException(nameof(globalClocks));

        var clocks = globalClocks.ToHashSet();
        List<JournaledOperation> snapshot;
        lock (syncRoot) { snapshot = journal.ToList(); }

        foreach (var op in snapshot.Where(o => o.Operation.ReplicaId == originReplicaId && clocks.Contains(o.Operation.GlobalClock)))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return op;
        }
        await Task.CompletedTask;
    }

    public async IAsyncEnumerable<JournaledOperation> GetAllJournaledOperationsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<JournaledOperation> snapshot;
        lock (syncRoot) { snapshot = journal.ToList(); }

        foreach (var op in snapshot)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return op;
        }
        await Task.CompletedTask;
    }

    public void Trim(IReadOnlyDictionary<string, long> gmvv)
    {
        if (gmvv == null) throw new ArgumentNullException(nameof(gmvv));

        lock (syncRoot)
        {
            var removedCount = journal.RemoveAll(op => gmvv.TryGetValue(op.Operation.ReplicaId, out var minKnown) && op.Operation.GlobalClock <= minKnown);
            if (removedCount > 0)
            {
                SaveJournalSynchronously();
            }
        }
    }

    public Task TrimAsync(IReadOnlyDictionary<string, long> globalMinimumVersionVector, CancellationToken cancellationToken = default)
    {
        Trim(globalMinimumVersionVector);
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        optionsChangeToken?.Dispose();
    }

    private void LoadJournalSynchronously()
    {
        var filePath = GetJournalFilePath();
        if (!File.Exists(filePath))
        {
            return;
        }

        try
        {
            var bytes = File.ReadAllBytes(filePath);
            var loaded = serializer.DeserializeFromBytes<List<JournaledOperation>>(bytes);
            if (loaded != null)
            {
                journal.AddRange(loaded);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load initialized journal from {FilePath}", filePath);
        }
    }

    private void SaveJournalSynchronously()
    {
        var filePath = GetJournalFilePath();
        try
        {
            var bytes = serializer.SerializeToBytes(journal);
            File.WriteAllBytes(filePath, bytes);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to synchronously save journal to {FilePath}", filePath);
        }
    }

    private string GetDocumentFilePath(string docId) => $"{optionsMonitor.CurrentValue.ReplicaId}_{docId}_state.json";
    
    private string GetGlobalFilePath(string rid) => $"{rid}_global_dvv.json";
    
    private string GetJournalFilePath() => $"{loadedReplicaId}_journal.json";
}