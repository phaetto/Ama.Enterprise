namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Providers;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// <summary>
/// Background hosted service that bridges a source CRDT replica and document type to a target CRDT replica and document type,
/// handling live mutation streaming, background registry backfill, tombstone propagation, and zero-data-loss draining.
/// </summary>
/// <typeparam name="TSourceState">The source CRDT document state type.</typeparam>
/// <typeparam name="TTargetState">The target CRDT document state type.</typeparam>
public sealed class CrdtMigrationBridgeService<TSourceState, TTargetState> : BackgroundService, ICrdtMigrationBridge
    where TSourceState : class, new()
    where TTargetState : class, new()
{
    private readonly CrdtMigrationOptions options;
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly IServiceProvider rootServiceProvider;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<CrdtMigrationBridgeService<TSourceState, TTargetState>> logger;

    private readonly Channel<string> dirtyDocumentChannel;
    private readonly ConcurrentDictionary<string, byte> queuedDocuments = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> documentLocks = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> backfilledDocumentsSet = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> propagatedTombstonesSet = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, CrdtDocument<TSourceState>> previousSourceSnapshots = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim drainLock = new(1, 1);

    private int stage = (int)CrdtMigrationStage.NotStarted;
    private int inFlightTranslations;
    private int registryScanRequested;
    private long totalSourceDocumentsDiscovered;
    private long backfilledDocumentsCount;
    private long liveEventsProcessedCount;
    private long tombstonesProcessedCount;
    private long failedTranslationsCount;
    private bool initialBackfillCompleted;

    public CrdtMigrationOptions Options => options;

    public CrdtMigrationBridgeService(
        CrdtMigrationOptions options,
        DistributedCrdtScopeManager scopeManager,
        IServiceProvider rootServiceProvider,
        ICrdtSerializer serializer,
        ILogger<CrdtMigrationBridgeService<TSourceState, TTargetState>> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(scopeManager);
        ArgumentNullException.ThrowIfNull(rootServiceProvider);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);

        ArgumentException.ThrowIfNullOrWhiteSpace(options.MigrationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.SourceReplicaId);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.SourceTypeAlias);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.TargetReplicaId);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.TargetTypeAlias);

        this.options = options;
        this.scopeManager = scopeManager;
        this.rootServiceProvider = rootServiceProvider;
        this.serializer = serializer;
        this.logger = logger;

        dirtyDocumentChannel = Channel.CreateUnbounded<string>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false
        });
    }

    /// <inheritdoc />
    public CrdtMigrationStatusDto GetStatus()
    {
        var currentStage = (CrdtMigrationStage)Volatile.Read(ref stage);
        var pending = queuedDocuments.Count;
        var inFlight = Volatile.Read(ref inFlightTranslations);

        return new CrdtMigrationStatusDto(
            MigrationId: options.MigrationId,
            SourceReplicaId: options.SourceReplicaId,
            SourceTypeAlias: options.SourceTypeAlias,
            TargetReplicaId: options.TargetReplicaId,
            TargetTypeAlias: options.TargetTypeAlias,
            Stage: currentStage,
            BackfilledDocuments: Interlocked.Read(ref backfilledDocumentsCount),
            LiveEventsProcessed: Interlocked.Read(ref liveEventsProcessedCount),
            TombstonesProcessed: Interlocked.Read(ref tombstonesProcessedCount),
            PendingDocumentsInQueue: pending,
            InFlightTranslations: inFlight,
            FailedTranslations: Interlocked.Read(ref failedTranslationsCount));
    }

    /// <inheritdoc />
    public void EnqueueDocumentChange(string sourceDocumentId)
    {
        if (string.IsNullOrWhiteSpace(sourceDocumentId))
        {
            return;
        }

        if (string.Equals(sourceDocumentId, Constants.SystemRegistryDocumentId, StringComparison.Ordinal))
        {
            return;
        }

        Interlocked.Increment(ref liveEventsProcessedCount);
        EnqueueDirtyDocument(sourceDocumentId);
    }

    /// <inheritdoc />
    public void NotifyTopologyOrRegistryChanged()
    {
        Interlocked.Exchange(ref registryScanRequested, 1);
    }

    /// <inheritdoc />
    public async Task DrainAndFlushAsync(CancellationToken cancellationToken = default)
    {
        await drainLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var sourceScope = scopeManager.GetOrCreateScope(options.SourceReplicaId);
            var targetScope = scopeManager.GetOrCreateScope(options.TargetReplicaId);

            // 1. Flush the source orchestrator so all in-flight source commands/observers have fired
            await sourceScope.Orchestrator.FlushAsync(cancellationToken).ConfigureAwait(false);

            // 2. Run a full reconciliation scan of the source registry to catch anything not yet enqueued
            await ScanSourceRegistryAsync(sourceScope, isInitialBackfill: false, cancellationToken).ConfigureAwait(false);

            // 3. Process all queued items until the queue and in-flight workers are completely empty
            using var migratorScope = rootServiceProvider.CreateScope();
            var migrator = ResolveMigrator(migratorScope.ServiceProvider);

            while (true)
            {
                while (dirtyDocumentChannel.Reader.TryRead(out var docId))
                {
                    await ProcessSingleDocumentSafeAsync(docId, sourceScope, targetScope, migrator, cancellationToken).ConfigureAwait(false);
                }

                if (queuedDocuments.IsEmpty && Volatile.Read(ref inFlightTranslations) == 0)
                {
                    // Flush source one more time in case new commands arrived during processing
                    await sourceScope.Orchestrator.FlushAsync(cancellationToken).ConfigureAwait(false);
                    if (queuedDocuments.IsEmpty && Volatile.Read(ref inFlightTranslations) == 0 && !dirtyDocumentChannel.Reader.TryPeek(out _))
                    {
                        break;
                    }
                }

                await Task.Delay(TimeSpan.FromMilliseconds(Math.Max(1, options.BackfillBatchDelayMilliseconds)), cancellationToken).ConfigureAwait(false);
            }

            // 4. Flush the target orchestrator so all translated intents and patch broadcasts are durable/sent
            await targetScope.Orchestrator.FlushAsync(cancellationToken).ConfigureAwait(false);

            Volatile.Write(ref initialBackfillCompleted, true);
            Volatile.Write(ref stage, (int)CrdtMigrationStage.Drained);
        }
        finally
        {
            drainLock.Release();
        }
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Volatile.Write(ref stage, (int)CrdtMigrationStage.Initializing);

        IDistributedCrdtScope sourceScope;
        IDistributedCrdtScope targetScope;

        try
        {
            sourceScope = scopeManager.GetOrCreateScope(options.SourceReplicaId);
            targetScope = scopeManager.GetOrCreateScope(options.TargetReplicaId);

            await WaitForOrchestratorInitializedAsync(sourceScope.Orchestrator, stoppingToken).ConfigureAwait(false);
            await WaitForOrchestratorInitializedAsync(targetScope.Orchestrator, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to initialize CrdtMigrationBridgeService '{MigrationId}'.", options.MigrationId);
            return;
        }

        // Start concurrent translation worker loops
        var concurrency = Math.Max(1, options.MaxConcurrency);
        var workerTasks = new Task[concurrency];
        for (int i = 0; i < concurrency; i++)
        {
            workerTasks[i] = RunWorkerLoopAsync(sourceScope, targetScope, stoppingToken);
        }

        // Perform initial backfill scan
        Volatile.Write(ref stage, (int)CrdtMigrationStage.Backfilling);
        try
        {
            await ScanSourceRegistryAsync(sourceScope, isInitialBackfill: true, stoppingToken).ConfigureAwait(false);
            Volatile.Write(ref initialBackfillCompleted, true);
            UpdateDynamicStage();

            // Run periodic reconciliation scans until stopped
            var scanIntervalSeconds = Math.Max(1, options.ReconciliationScanIntervalSeconds);
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(scanIntervalSeconds));

            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                Interlocked.Exchange(ref registryScanRequested, 0);
                await ScanSourceRegistryAsync(sourceScope, isInitialBackfill: false, stoppingToken).ConfigureAwait(false);
                UpdateDynamicStage();
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error in reconciliation loop for CrdtMigrationBridgeService '{MigrationId}'.", options.MigrationId);
        }

        try
        {
            await Task.WhenAll(workerTasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private void EnqueueDirtyDocument(string sourceDocumentId)
    {
        if (queuedDocuments.TryAdd(sourceDocumentId, 0))
        {
            if (Volatile.Read(ref initialBackfillCompleted))
            {
                var current = (CrdtMigrationStage)Volatile.Read(ref stage);
                if (current is CrdtMigrationStage.Converged or CrdtMigrationStage.Drained)
                {
                    Volatile.Write(ref stage, (int)CrdtMigrationStage.StreamingLive);
                }
            }

            dirtyDocumentChannel.Writer.TryWrite(sourceDocumentId);
        }
    }

    private void UpdateDynamicStage()
    {
        if (!Volatile.Read(ref initialBackfillCompleted))
        {
            return;
        }

        var current = (CrdtMigrationStage)Volatile.Read(ref stage);
        if (current == CrdtMigrationStage.Drained && queuedDocuments.IsEmpty && Volatile.Read(ref inFlightTranslations) == 0)
        {
            return;
        }

        if (queuedDocuments.IsEmpty && Volatile.Read(ref inFlightTranslations) == 0)
        {
            Volatile.Write(ref stage, (int)CrdtMigrationStage.Converged);
        }
        else
        {
            Volatile.Write(ref stage, (int)CrdtMigrationStage.StreamingLive);
        }
    }

    private async Task WaitForOrchestratorInitializedAsync(ICrdtDocumentOrchestrator orchestrator, CancellationToken cancellationToken)
    {
        while (orchestrator.InitializationStage != OrchestratorInitializationStage.Initialized)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(Math.Max(1, options.BackfillBatchDelayMilliseconds)), cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task RunWorkerLoopAsync(
        IDistributedCrdtScope sourceScope,
        IDistributedCrdtScope targetScope,
        CancellationToken stoppingToken)
    {
        using var serviceScope = rootServiceProvider.CreateScope();
        var migrator = ResolveMigrator(serviceScope.ServiceProvider);

        while (!stoppingToken.IsCancellationRequested)
        {
            string docId;
            try
            {
                docId = await dirtyDocumentChannel.Reader.ReadAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ChannelClosedException)
            {
                break;
            }

            await ProcessSingleDocumentSafeAsync(docId, sourceScope, targetScope, migrator, stoppingToken).ConfigureAwait(false);
            UpdateDynamicStage();
        }
    }

    private async Task ScanSourceRegistryAsync(
        IDistributedCrdtScope sourceScope,
        bool isInitialBackfill,
        CancellationToken cancellationToken)
    {
        var registryDoc = sourceScope.Orchestrator.TryGetLocalDocument<CrdtRegistryState>(Constants.SystemRegistryDocumentId);
        if (registryDoc is null)
        {
            return;
        }

        var registrySnapshot = await registryDoc.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
        if (registrySnapshot.Data is null)
        {
            return;
        }

        var shardIdProvider = sourceScope.ServiceProvider.GetService<IShardIdProvider>();
        var shardDistributionManager = sourceScope.ServiceProvider.GetService<IShardDistributionManager>();
        var distributedOptions = sourceScope.ServiceProvider.GetService<DistributedCrdtOptions>();

        var activeKeyPrefix = $"{options.SourceTypeAlias}:";
        long discoveredCount = 0;

        // Scan active and tombstoned documents matching SourceTypeAlias
        foreach (var kvp in registrySnapshot.Data.Documents)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var entry = kvp.Value;
            if (!string.Equals(entry.TypeAlias, options.SourceTypeAlias, StringComparison.Ordinal))
            {
                if (!kvp.Key.StartsWith(activeKeyPrefix, StringComparison.Ordinal))
                {
                    continue;
                }
            }

            var docId = !string.IsNullOrWhiteSpace(entry.DocumentId)
                ? entry.DocumentId
                : ExtractDocumentIdFromCompositeKey(kvp.Key, activeKeyPrefix);

            if (string.IsNullOrWhiteSpace(docId) ||
                string.Equals(docId, Constants.SystemRegistryDocumentId, StringComparison.Ordinal))
            {
                continue;
            }

            if (entry.IsDeleted)
            {
                if (options.PropagateDocumentTombstones && !propagatedTombstonesSet.ContainsKey(docId))
                {
                    EnqueueDirtyDocument(docId);
                }

                continue;
            }

            // If this document was previously tombstoned and later recreated, clear the tombstone tracking
            propagatedTombstonesSet.TryRemove(docId, out _);

            if (!await ShouldProcessBackfillDocumentAsync(
                    docId,
                    shardIdProvider,
                    shardDistributionManager,
                    distributedOptions,
                    cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            discoveredCount++;

            if (isInitialBackfill || !backfilledDocumentsSet.ContainsKey(docId))
            {
                EnqueueDirtyDocument(docId);

                if (options.BackfillBatchDelayMilliseconds > 0 && isInitialBackfill)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(options.BackfillBatchDelayMilliseconds), cancellationToken).ConfigureAwait(false);
                }
            }
        }

        Interlocked.Exchange(ref totalSourceDocumentsDiscovered, discoveredCount);
    }

    private async ValueTask<bool> ShouldProcessBackfillDocumentAsync(
        string documentId,
        IShardIdProvider? shardIdProvider,
        IShardDistributionManager? shardDistributionManager,
        DistributedCrdtOptions? distributedOptions,
        CancellationToken cancellationToken)
    {
        if (!options.PartitionBackfillByPrimaryShardOwner ||
            distributedOptions is null ||
            !distributedOptions.EnableSharding ||
            shardDistributionManager is null)
        {
            return true;
        }

        var shardId = shardIdProvider is not null
            ? shardIdProvider.GetShardId<TSourceState>(documentId)
            : documentId;

        var targetNodes = await shardDistributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
        if (targetNodes.Count == 0)
        {
            return true;
        }

        return string.Equals(targetNodes[0], options.SourceReplicaId, StringComparison.Ordinal);
    }

    private async Task ProcessSingleDocumentSafeAsync(
        string sourceDocumentId,
        IDistributedCrdtScope sourceScope,
        IDistributedCrdtScope targetScope,
        ICrdtDocumentMigrator<TSourceState, TTargetState> migrator,
        CancellationToken cancellationToken)
    {
        var docLock = documentLocks.GetOrAdd(sourceDocumentId, static _ => new SemaphoreSlim(1, 1));
        Interlocked.Increment(ref inFlightTranslations);
        await docLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Remove from queued set AFTER acquiring the per-document lock and BEFORE reading state
            // so two workers never process the same document concurrently and any mutation occurring
            // while we are migrating will re-enqueue the documentId and never be lost.
            queuedDocuments.TryRemove(sourceDocumentId, out _);

            var existsInSource = await sourceScope.Orchestrator.DocumentExistsAsync(
                sourceDocumentId,
                options.SourceTypeAlias,
                cancellationToken).ConfigureAwait(false);

            var targetDocumentId = migrator.MapDocumentId(sourceDocumentId);
            if (string.IsNullOrWhiteSpace(targetDocumentId))
            {
                targetDocumentId = sourceDocumentId;
            }

            var targetStorage = targetScope.ServiceProvider.GetService<IDistributedCrdtStorage>();

            if (!existsInSource)
            {
                // Check if the document was explicitly tombstoned in the source registry
                if (options.PropagateDocumentTombstones &&
                    await IsSourceDocumentTombstonedAsync(sourceScope, sourceDocumentId, cancellationToken).ConfigureAwait(false))
                {
                    if (propagatedTombstonesSet.TryAdd(sourceDocumentId, 0))
                    {
                        var existsInTarget = await targetScope.Orchestrator.DocumentExistsAsync(
                            targetDocumentId,
                            options.TargetTypeAlias,
                            cancellationToken).ConfigureAwait(false);

                        if (existsInTarget)
                        {
                            await targetScope.Orchestrator.DeleteDocumentAsync(
                                targetDocumentId,
                                options.TargetTypeAlias,
                                options.TargetDeliveryMode,
                                cancellationToken).ConfigureAwait(false);
                        }

                        await ClearPreviousSourceCheckpointAsync(sourceDocumentId, targetStorage, cancellationToken).ConfigureAwait(false);
                        Interlocked.Increment(ref tombstonesProcessedCount);
                    }
                }

                return;
            }

            // Document is active in source, ensure it is not marked as tombstoned
            propagatedTombstonesSet.TryRemove(sourceDocumentId, out _);

            var sourceDoc = await sourceScope.Orchestrator.GetDocumentStateAsync<TSourceState>(sourceDocumentId, cancellationToken).ConfigureAwait(false);
            if (sourceDoc.Data is null)
            {
                return;
            }

            CrdtDocument<TSourceState> isolatedSourceSnapshot;
            byte[] sourceDocBytes;
            while (true)
            {
                try
                {
                    // Capture an isolated snapshot clone of the current source document before migrating
                    // so in-place mutations on the live source document do not mutate our stored 3-way merge baseline.
                    sourceDocBytes = serializer.SerializeToBytes(sourceDoc);
                    isolatedSourceSnapshot = serializer.DeserializeFromBytes<CrdtDocument<TSourceState>>(sourceDocBytes);
                    break;
                }
                catch (InvalidOperationException)
                {
                    // A concurrent mutation on the live source document modified an inner dictionary/collection
                    // while the serializer was enumerating it; re-fetch the state and retry snapshot capture.
                    cancellationToken.ThrowIfCancellationRequested();
                    sourceDoc = await sourceScope.Orchestrator.GetDocumentStateAsync<TSourceState>(sourceDocumentId, cancellationToken).ConfigureAwait(false);
                    if (sourceDoc.Data is null)
                    {
                        return;
                    }
                }
            }

            var previousSourceDoc = await LoadPreviousSourceCheckpointAsync(sourceDocumentId, targetStorage, cancellationToken).ConfigureAwait(false);

            var targetExists = await targetScope.Orchestrator.DocumentExistsAsync(
                targetDocumentId,
                options.TargetTypeAlias,
                cancellationToken).ConfigureAwait(false);

            if (!targetExists)
            {
                await targetScope.Orchestrator.CreateDocumentAsync(
                    targetDocumentId,
                    options.TargetTypeAlias,
                    options.TargetDeliveryMode,
                    cancellationToken).ConfigureAwait(false);
            }

            var targetDoc = await targetScope.Orchestrator.GetDocumentStateAsync<TTargetState>(targetDocumentId, cancellationToken).ConfigureAwait(false);
            if (targetDoc.Data is not null)
            {
                while (true)
                {
                    try
                    {
                        var targetDocBytes = serializer.SerializeToBytes(targetDoc);
                        targetDoc = serializer.DeserializeFromBytes<CrdtDocument<TTargetState>>(targetDocBytes);
                        break;
                    }
                    catch (InvalidOperationException)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        targetDoc = await targetScope.Orchestrator.GetDocumentStateAsync<TTargetState>(targetDocumentId, cancellationToken).ConfigureAwait(false);
                        if (targetDoc.Data is null)
                        {
                            break;
                        }
                    }
                }
            }

            await migrator.MigrateDocumentAsync(
                targetDocumentId,
                isolatedSourceSnapshot,
                previousSourceDoc,
                targetDoc,
                targetScope.Orchestrator,
                options.TargetDeliveryMode,
                cancellationToken).ConfigureAwait(false);

            await SavePreviousSourceCheckpointAsync(
                sourceDocumentId,
                isolatedSourceSnapshot,
                sourceDocBytes,
                targetStorage,
                cancellationToken).ConfigureAwait(false);

            if (backfilledDocumentsSet.TryAdd(sourceDocumentId, 0))
            {
                Interlocked.Increment(ref backfilledDocumentsCount);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Re-queue on cancellation so it is not lost if drained later
            queuedDocuments.TryAdd(sourceDocumentId, 0);
            dirtyDocumentChannel.Writer.TryWrite(sourceDocumentId);
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref failedTranslationsCount);
            logger.LogError(
                ex,
                "Failed to migrate document '{SourceDocumentId}' in migration bridge '{MigrationId}'.",
                sourceDocumentId,
                options.MigrationId);
        }
        finally
        {
            docLock.Release();
            Interlocked.Decrement(ref inFlightTranslations);
        }
    }

    private async ValueTask<CrdtDocument<TSourceState>?> LoadPreviousSourceCheckpointAsync(
        string sourceDocumentId,
        IDistributedCrdtStorage? targetStorage,
        CancellationToken cancellationToken)
    {
        if (previousSourceSnapshots.TryGetValue(sourceDocumentId, out var cachedSnapshot))
        {
            return cachedSnapshot;
        }

        if (targetStorage is null)
        {
            return null;
        }

        var checkpointKey = GetSourceCheckpointStorageKey(sourceDocumentId);
        var persistedSnapshot = await targetStorage.LoadDocumentAsync<TSourceState>(checkpointKey, checkpointKey, cancellationToken).ConfigureAwait(false);
        if (persistedSnapshot is not null && persistedSnapshot.Value.Data is not null)
        {
            previousSourceSnapshots[sourceDocumentId] = persistedSnapshot.Value;
            return persistedSnapshot.Value;
        }

        return null;
    }

    private async ValueTask SavePreviousSourceCheckpointAsync(
        string sourceDocumentId,
        CrdtDocument<TSourceState> isolatedSourceSnapshot,
        byte[] sourceDocBytes,
        IDistributedCrdtStorage? targetStorage,
        CancellationToken cancellationToken)
    {
        previousSourceSnapshots[sourceDocumentId] = isolatedSourceSnapshot;

        if (targetStorage is null)
        {
            return;
        }

        var checkpointKey = GetSourceCheckpointStorageKey(sourceDocumentId);
        await targetStorage.SaveSerializedDocumentAsync<TSourceState>(checkpointKey, checkpointKey, sourceDocBytes, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask ClearPreviousSourceCheckpointAsync(
        string sourceDocumentId,
        IDistributedCrdtStorage? targetStorage,
        CancellationToken cancellationToken)
    {
        previousSourceSnapshots.TryRemove(sourceDocumentId, out _);

        if (targetStorage is null)
        {
            return;
        }

        var checkpointKey = GetSourceCheckpointStorageKey(sourceDocumentId);
        await targetStorage.DeleteDocumentAsync(checkpointKey, checkpointKey, cancellationToken).ConfigureAwait(false);
    }

    private string GetSourceCheckpointStorageKey(string sourceDocumentId)
    {
        return $"__migration-checkpoint:{options.MigrationId}:{sourceDocumentId}";
    }

    private async ValueTask<bool> IsSourceDocumentTombstonedAsync(
        IDistributedCrdtScope sourceScope,
        string sourceDocumentId,
        CancellationToken cancellationToken)
    {
        var registryDoc = sourceScope.Orchestrator.TryGetLocalDocument<CrdtRegistryState>(Constants.SystemRegistryDocumentId);
        if (registryDoc is null)
        {
            return false;
        }

        var snapshot = await registryDoc.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
        if (snapshot.Data is null)
        {
            return false;
        }

        var compositeKey = CrdtRegistryEntry.GetRegistryKey(options.SourceTypeAlias, sourceDocumentId);
        return snapshot.Data.Documents.TryGetValue(compositeKey, out var entry) && entry.IsDeleted;
    }

    private ICrdtDocumentMigrator<TSourceState, TTargetState> ResolveMigrator(IServiceProvider scopedProvider)
    {
        var keyed = scopedProvider.GetKeyedService<ICrdtDocumentMigrator<TSourceState, TTargetState>>(options.MigrationId);
        if (keyed is not null)
        {
            return keyed;
        }

        return scopedProvider.GetRequiredService<ICrdtDocumentMigrator<TSourceState, TTargetState>>();
    }

    private static string ExtractDocumentIdFromCompositeKey(string compositeKey, string prefix)
    {
        if (compositeKey.StartsWith(prefix, StringComparison.Ordinal))
        {
            return compositeKey.Substring(prefix.Length);
        }

        var colonIndex = compositeKey.IndexOf(':');
        return colonIndex >= 0 && colonIndex + 1 < compositeKey.Length
            ? compositeKey.Substring(colonIndex + 1)
            : compositeKey;
    }
}