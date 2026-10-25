namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ama.CRDT.Services.Pooling;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// <summary>
/// Background hosted mediator service that executes a zero-downtime live migration from a source CRDT replica/model to a target CRDT replica/model.
/// Subscribes to live change events first, runs a throttled background backfill of existing documents, and drains all pending work before shutdown.
/// </summary>
/// <typeparam name="TSourceState">The source document state type.</typeparam>
/// <typeparam name="TTargetState">The target document state type.</typeparam>
public sealed class CrdtMigrationBridgeService<TSourceState, TTargetState> : BackgroundService, ICrdtMigrationBridge
    where TSourceState : class, new()
    where TTargetState : class, new()
{
    private readonly CrdtMigrationOptions options;
    private readonly IServiceProvider rootServiceProvider;
    private readonly ICrdtShutdownCoordinator shutdownCoordinator;
    private readonly ILogger<CrdtMigrationBridgeService<TSourceState, TTargetState>> logger;

    private readonly Channel<string> dirtyDocumentChannel;
    private readonly ConcurrentDictionary<string, byte> queuedDocuments = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> backfilledDocumentsSet = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> propagatedTombstonesSet = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim drainGate = new(1, 1);

    private int stage = (int)CrdtMigrationStage.NotStarted;
    private int registryScanRequested = 1;
    private int inFlightTranslations;
    private long backfilledDocumentsCount;
    private long liveEventsProcessedCount;
    private long tombstonesProcessedCount;
    private long failedTranslationsCount;
    private volatile bool initialBackfillCompleted;

    public CrdtMigrationBridgeService(
        CrdtMigrationOptions options,
        IServiceProvider rootServiceProvider,
        ICrdtShutdownCoordinator shutdownCoordinator,
        ILogger<CrdtMigrationBridgeService<TSourceState, TTargetState>> logger)
    {
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.rootServiceProvider = rootServiceProvider ?? throw new ArgumentNullException(nameof(rootServiceProvider));
        this.shutdownCoordinator = shutdownCoordinator ?? throw new ArgumentNullException(nameof(shutdownCoordinator));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        if (string.IsNullOrWhiteSpace(options.SourceReplicaId))
            throw new ArgumentException("SourceReplicaId must be specified.", nameof(options));
        if (string.IsNullOrWhiteSpace(options.SourceTypeAlias))
            throw new ArgumentException("SourceTypeAlias must be specified.", nameof(options));
        if (string.IsNullOrWhiteSpace(options.TargetReplicaId))
            throw new ArgumentException("TargetReplicaId must be specified.", nameof(options));
        if (string.IsNullOrWhiteSpace(options.TargetTypeAlias))
            throw new ArgumentException("TargetTypeAlias must be specified.", nameof(options));
        if (options.MaxConcurrency <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxConcurrency must be greater than zero.");

        dirtyDocumentChannel = Channel.CreateUnbounded<string>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });
    }

    /// <inheritdoc />
    public CrdtMigrationOptions Options => options;

    /// <inheritdoc />
    public CrdtMigrationStatusDto GetStatus()
    {
        var currentStage = (CrdtMigrationStage)Volatile.Read(ref stage);
        var pending = queuedDocuments.Count;
        var inFlight = Volatile.Read(ref inFlightTranslations);

        if (initialBackfillCompleted && currentStage != CrdtMigrationStage.Drained)
        {
            currentStage = (pending == 0 && inFlight == 0)
                ? CrdtMigrationStage.Converged
                : CrdtMigrationStage.StreamingLive;
        }

        return new CrdtMigrationStatusDto(
            options.MigrationId,
            options.SourceReplicaId,
            options.SourceTypeAlias,
            options.TargetReplicaId,
            options.TargetTypeAlias,
            currentStage,
            Interlocked.Read(ref backfilledDocumentsCount),
            Interlocked.Read(ref liveEventsProcessedCount),
            Interlocked.Read(ref tombstonesProcessedCount),
            pending,
            inFlight,
            Interlocked.Read(ref failedTranslationsCount));
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
            NotifyTopologyOrRegistryChanged();
            return;
        }

        sourceDocumentId = CrdtStringPool.Shared.GetOrAdd(sourceDocumentId);
        if (queuedDocuments.TryAdd(sourceDocumentId, 0))
        {
            dirtyDocumentChannel.Writer.TryWrite(sourceDocumentId);
        }
    }

    /// <inheritdoc />
    public void NotifyTopologyOrRegistryChanged()
    {
        Interlocked.Exchange(ref registryScanRequested, 1);
    }

    /// <inheritdoc />
    public async Task DrainAndFlushAsync(CancellationToken cancellationToken = default)
    {
        await drainGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var scopeManager = rootServiceProvider.GetRequiredService<DistributedCrdtScopeManager>();
            var sourceScope = scopeManager.GetOrCreateScope(options.SourceReplicaId);
            var targetScope = scopeManager.GetOrCreateScope(options.TargetReplicaId);

            var sourceOrchestrator = sourceScope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
            var targetOrchestrator = targetScope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
            var sourceShardManager = sourceScope.ServiceProvider.GetRequiredService<IShardDistributionManager>();
            var migrator = targetScope.ServiceProvider.GetService<ICrdtDocumentMigrator<TSourceState, TTargetState>>()
                ?? rootServiceProvider.GetRequiredService<ICrdtDocumentMigrator<TSourceState, TTargetState>>();

            // 1. Scan registry one more time to ensure all existing or tombstoned documents are queued
            await ScanSourceRegistryAsync(
                sourceOrchestrator,
                targetOrchestrator,
                sourceShardManager,
                migrator,
                cancellationToken).ConfigureAwait(false);

            // 2. Drain all queued documents synchronously
            while (dirtyDocumentChannel.Reader.TryRead(out var docId))
            {
                await ProcessSingleDocumentSafeAsync(
                    docId,
                    isBackfill: false,
                    sourceOrchestrator,
                    targetOrchestrator,
                    migrator,
                    cancellationToken).ConfigureAwait(false);
            }

            // 3. Wait for any concurrent background workers to finish in-flight items
            var pollDelay = TimeSpan.FromMilliseconds(Math.Max(1, options.BackfillBatchDelayMilliseconds));
            while (!queuedDocuments.IsEmpty || Volatile.Read(ref inFlightTranslations) > 0)
            {
                while (dirtyDocumentChannel.Reader.TryRead(out var docId))
                {
                    await ProcessSingleDocumentSafeAsync(
                        docId,
                        isBackfill: false,
                        sourceOrchestrator,
                        targetOrchestrator,
                        migrator,
                        cancellationToken).ConfigureAwait(false);
                }

                if (!queuedDocuments.IsEmpty || Volatile.Read(ref inFlightTranslations) > 0)
                {
                    await Task.Delay(pollDelay, cancellationToken).ConfigureAwait(false);
                }
            }

            // 4. Flush active target documents so active-sync broadcasters and journals are up-to-date
            var activeTargetDocs = targetOrchestrator.GetActiveDocuments();
            for (int i = 0; i < activeTargetDocs.Count; i++)
            {
                await activeTargetDocs[i].FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            drainGate.Release();
        }
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await DrainAndFlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Ignore cancellation on stop
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Error while draining CRDT migration bridge '{MigrationId}' during shutdown.", options.MigrationId);
        }

        dirtyDocumentChannel.Writer.TryComplete();
        Interlocked.Exchange(ref stage, (int)CrdtMigrationStage.Drained);

        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, shutdownCoordinator.ShutdownToken);
        var token = linkedCts.Token;

        Interlocked.Exchange(ref stage, (int)CrdtMigrationStage.Initializing);

        var scopeManager = rootServiceProvider.GetRequiredService<DistributedCrdtScopeManager>();
        var sourceScope = scopeManager.GetOrCreateScope(options.SourceReplicaId);
        var targetScope = scopeManager.GetOrCreateScope(options.TargetReplicaId);

        var sourceOrchestrator = sourceScope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var targetOrchestrator = targetScope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var sourceShardManager = sourceScope.ServiceProvider.GetRequiredService<IShardDistributionManager>();
        var migrator = targetScope.ServiceProvider.GetService<ICrdtDocumentMigrator<TSourceState, TTargetState>>()
            ?? rootServiceProvider.GetRequiredService<ICrdtDocumentMigrator<TSourceState, TTargetState>>();

        await WaitForOrchestratorInitializedAsync(sourceOrchestrator, token).ConfigureAwait(false);
        await WaitForOrchestratorInitializedAsync(targetOrchestrator, token).ConfigureAwait(false);

        Interlocked.Exchange(ref stage, (int)CrdtMigrationStage.Backfilling);
        logger.LogInformation(
            "Starting live CRDT migration bridge '{MigrationId}' from {SourceReplica}/{SourceType} to {TargetReplica}/{TargetType}.",
            options.MigrationId,
            options.SourceReplicaId,
            options.SourceTypeAlias,
            options.TargetReplicaId,
            options.TargetTypeAlias);

        // Start live consumer workers immediately so live changes are processed concurrently with backfill
        var workerCount = Math.Max(1, options.MaxConcurrency);
        var workers = new List<Task>(workerCount);
        for (int i = 0; i < workerCount; i++)
        {
            workers.Add(RunLiveQueueConsumerAsync(sourceOrchestrator, targetOrchestrator, migrator, token));
        }

        // Run initial background backfill and periodic reconciliation loop
        var reconciliationTask = RunBackfillAndReconciliationLoopAsync(
            sourceOrchestrator,
            targetOrchestrator,
            sourceShardManager,
            migrator,
            token);

        try
        {
            await Task.WhenAll(workers.Append(reconciliationTask)).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Normal shutdown
        }
    }

    private async Task WaitForOrchestratorInitializedAsync(ICrdtDocumentOrchestrator orchestrator, CancellationToken cancellationToken)
    {
        var delay = TimeSpan.FromMilliseconds(Math.Max(5, options.BackfillBatchDelayMilliseconds));
        while (orchestrator.InitializationStage != OrchestratorInitializationStage.Initialized)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task RunLiveQueueConsumerAsync(
        ICrdtDocumentOrchestrator sourceOrchestrator,
        ICrdtDocumentOrchestrator targetOrchestrator,
        ICrdtDocumentMigrator<TSourceState, TTargetState> migrator,
        CancellationToken cancellationToken)
    {
        var reader = dirtyDocumentChannel.Reader;
        while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (reader.TryRead(out var docId))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                var isFirstTimeBackfill = !backfilledDocumentsSet.ContainsKey(docId);
                await ProcessSingleDocumentSafeAsync(
                    docId,
                    isFirstTimeBackfill,
                    sourceOrchestrator,
                    targetOrchestrator,
                    migrator,
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task RunBackfillAndReconciliationLoopAsync(
        ICrdtDocumentOrchestrator sourceOrchestrator,
        ICrdtDocumentOrchestrator targetOrchestrator,
        IShardDistributionManager sourceShardManager,
        ICrdtDocumentMigrator<TSourceState, TTargetState> migrator,
        CancellationToken cancellationToken)
    {
        // Initial backfill pass
        await ScanSourceRegistryAsync(
            sourceOrchestrator,
            targetOrchestrator,
            sourceShardManager,
            migrator,
            cancellationToken).ConfigureAwait(false);

        initialBackfillCompleted = true;
        Interlocked.Exchange(
            ref stage,
            queuedDocuments.IsEmpty && Volatile.Read(ref inFlightTranslations) == 0
                ? (int)CrdtMigrationStage.Converged
                : (int)CrdtMigrationStage.StreamingLive);

        logger.LogInformation(
            "CRDT migration bridge '{MigrationId}' completed initial registry scan. Entering live streaming and reconciliation mode.",
            options.MigrationId);

        var scanInterval = TimeSpan.FromSeconds(Math.Max(1, options.ReconciliationScanIntervalSeconds));
        using var timer = new PeriodicTimer(scanInterval);

        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            if (Interlocked.Exchange(ref registryScanRequested, 0) == 1 || !initialBackfillCompleted)
            {
                await ScanSourceRegistryAsync(
                    sourceOrchestrator,
                    targetOrchestrator,
                    sourceShardManager,
                    migrator,
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task ScanSourceRegistryAsync(
        ICrdtDocumentOrchestrator sourceOrchestrator,
        ICrdtDocumentOrchestrator targetOrchestrator,
        IShardDistributionManager sourceShardManager,
        ICrdtDocumentMigrator<TSourceState, TTargetState> migrator,
        CancellationToken cancellationToken)
    {
        var registryDoc = await sourceOrchestrator.Registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
        if (registryDoc.Data?.Documents is null)
        {
            return;
        }

        var batchDelay = TimeSpan.FromMilliseconds(Math.Max(0, options.BackfillBatchDelayMilliseconds));
        var processedInBatch = 0;

        foreach (var kvp in registryDoc.Data.Documents)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var entry = kvp.Value;
            if (!string.Equals(entry.TypeAlias, options.SourceTypeAlias, StringComparison.Ordinal))
            {
                continue;
            }

            var sourceDocId = entry.DocumentId;

            if (entry.IsDeleted)
            {
                if (options.PropagateDocumentTombstones && propagatedTombstonesSet.TryAdd(sourceDocId, 0))
                {
                    var targetDocId = CrdtStringPool.Shared.GetOrAdd(migrator.MapDocumentId(sourceDocId));
                    var existsInTarget = await targetOrchestrator.DocumentExistsAsync(
                        targetDocId,
                        options.TargetTypeAlias,
                        cancellationToken).ConfigureAwait(false);

                    if (existsInTarget)
                    {
                        await targetOrchestrator.DeleteDocumentAsync(
                            targetDocId,
                            options.TargetTypeAlias,
                            options.TargetDeliveryMode,
                            cancellationToken).ConfigureAwait(false);

                        Interlocked.Increment(ref tombstonesProcessedCount);
                    }
                }

                continue;
            }

            if (backfilledDocumentsSet.ContainsKey(sourceDocId))
            {
                continue;
            }

            var shouldMigrateOnThisNode = await ShouldNodeBackfillDocumentAsync(
                sourceDocId,
                sourceOrchestrator,
                sourceShardManager,
                cancellationToken).ConfigureAwait(false);

            if (!shouldMigrateOnThisNode)
            {
                continue;
            }

            EnqueueDocumentChange(sourceDocId);
            processedInBatch++;

            if (batchDelay > TimeSpan.Zero && processedInBatch >= options.MaxConcurrency)
            {
                processedInBatch = 0;
                await Task.Delay(batchDelay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask<bool> ShouldNodeBackfillDocumentAsync(
        string sourceDocumentId,
        ICrdtDocumentOrchestrator sourceOrchestrator,
        IShardDistributionManager sourceShardManager,
        CancellationToken cancellationToken)
    {
        if (!options.PartitionBackfillByPrimaryShardOwner)
        {
            return true;
        }

        var shardId = await sourceOrchestrator.TryGetShardIdAsync(sourceDocumentId, options.SourceTypeAlias, cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(shardId))
        {
            return true;
        }

        var targetNodes = await sourceShardManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
        if (targetNodes.Count == 0)
        {
            return true;
        }

        // The primary owner (index 0 on the consistent hash ring) executes the initial backfill
        return string.Equals(targetNodes[0], options.SourceReplicaId, StringComparison.Ordinal);
    }

    private async Task ProcessSingleDocumentSafeAsync(
        string sourceDocumentId,
        bool isBackfill,
        ICrdtDocumentOrchestrator sourceOrchestrator,
        ICrdtDocumentOrchestrator targetOrchestrator,
        ICrdtDocumentMigrator<TSourceState, TTargetState> migrator,
        CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref inFlightTranslations);
        try
        {
            // Remove from queued set BEFORE reading state so any concurrent mutation occurring
            // while we are migrating will re-enqueue the documentId and never be lost.
            queuedDocuments.TryRemove(sourceDocumentId, out _);

            var existsInSource = await sourceOrchestrator.DocumentExistsAsync(
                sourceDocumentId,
                options.SourceTypeAlias,
                cancellationToken).ConfigureAwait(false);

            var targetDocumentId = CrdtStringPool.Shared.GetOrAdd(migrator.MapDocumentId(sourceDocumentId));

            if (!existsInSource)
            {
                if (options.PropagateDocumentTombstones && propagatedTombstonesSet.TryAdd(sourceDocumentId, 0))
                {
                    var existsInTarget = await targetOrchestrator.DocumentExistsAsync(
                        targetDocumentId,
                        options.TargetTypeAlias,
                        cancellationToken).ConfigureAwait(false);

                    if (existsInTarget)
                    {
                        await targetOrchestrator.DeleteDocumentAsync(
                            targetDocumentId,
                            options.TargetTypeAlias,
                            options.TargetDeliveryMode,
                            cancellationToken).ConfigureAwait(false);

                        Interlocked.Increment(ref tombstonesProcessedCount);
                    }
                }

                return;
            }

            // If the document was previously deleted and has now been recreated in source,
            // clear the tombstone marker so any subsequent deletion will be propagated again.
            propagatedTombstonesSet.TryRemove(sourceDocumentId, out _);

            var sourceDoc = await sourceOrchestrator.GetDocumentStateAsync<TSourceState>(sourceDocumentId, cancellationToken).ConfigureAwait(false);
            if (sourceDoc.Data is null)
            {
                return;
            }

            var targetExists = await targetOrchestrator.DocumentExistsAsync(
                targetDocumentId,
                options.TargetTypeAlias,
                cancellationToken).ConfigureAwait(false);

            if (!targetExists)
            {
                await targetOrchestrator.CreateDocumentAsync(
                    targetDocumentId,
                    options.TargetTypeAlias,
                    options.TargetDeliveryMode,
                    cancellationToken).ConfigureAwait(false);
            }

            var targetDoc = await targetOrchestrator.GetDocumentStateAsync<TTargetState>(targetDocumentId, cancellationToken).ConfigureAwait(false);

            await migrator.MigrateDocumentAsync(
                targetDocumentId,
                sourceDoc,
                targetDoc,
                targetOrchestrator,
                options.TargetDeliveryMode,
                cancellationToken).ConfigureAwait(false);

            if (backfilledDocumentsSet.TryAdd(sourceDocumentId, 0))
            {
                Interlocked.Increment(ref backfilledDocumentsCount);
            }

            if (!isBackfill)
            {
                Interlocked.Increment(ref liveEventsProcessedCount);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref failedTranslationsCount);
            logger.LogError(
                ex,
                "Failed to migrate document '{DocumentId}' in migration bridge '{MigrationId}'.",
                sourceDocumentId,
                options.MigrationId);
        }
        finally
        {
            Interlocked.Decrement(ref inFlightTranslations);
        }
    }
}