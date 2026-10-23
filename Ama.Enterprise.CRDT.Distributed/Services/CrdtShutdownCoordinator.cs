namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Default implementation of <see cref="ICrdtShutdownCoordinator"/> that orchestrates an idempotent, ordered flush of all active CRDT scopes.
/// </summary>
public sealed class CrdtShutdownCoordinator : ICrdtShutdownCoordinator, IDisposable
{
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtShutdownCoordinator> logger;
    private readonly IHostApplicationLifetime? hostApplicationLifetime;
    private readonly CancellationTokenSource shutdownCts = new();
    private readonly object syncRoot = new();

    private Task? flushTask;
    private int isShuttingDown;
    private int isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="CrdtShutdownCoordinator"/> class.
    /// </summary>
    /// <param name="scopeManager">The replica scope manager.</param>
    /// <param name="options">The distributed CRDT options.</param>
    /// <param name="logger">The logger instance.</param>
    /// <param name="hostApplicationLifetime">The optional host application lifetime to trigger application exit.</param>
    public CrdtShutdownCoordinator(
        DistributedCrdtScopeManager scopeManager,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtShutdownCoordinator> logger,
        IHostApplicationLifetime? hostApplicationLifetime = null)
    {
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.hostApplicationLifetime = hostApplicationLifetime;
    }

    /// <inheritdoc />
    public bool IsShuttingDown => Volatile.Read(ref isShuttingDown) == 1;

    /// <inheritdoc />
    public CancellationToken ShutdownToken => shutdownCts.Token;

    /// <inheritdoc />
    public Task FlushAllScopesAsync(string reason, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(reason))
        {
            throw new ArgumentException("Shutdown reason cannot be null or empty.", nameof(reason));
        }

        if (Interlocked.Exchange(ref isShuttingDown, 1) == 0)
        {
            try
            {
                shutdownCts.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        lock (syncRoot)
        {
            flushTask ??= ExecuteFlushPipelineAsync(reason, cancellationToken);
            return flushTask;
        }
    }

    /// <inheritdoc />
    public async Task RequestShutdownAsync(string reason, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(reason))
        {
            throw new ArgumentException("Shutdown reason cannot be null or empty.", nameof(reason));
        }

        await FlushAllScopesAsync(reason, cancellationToken).ConfigureAwait(false);
        hostApplicationLifetime?.StopApplication();
    }

    private async Task ExecuteFlushPipelineAsync(string reason, CancellationToken externalCancellationToken)
    {
        var opts = options.Value;
        var timeoutMs = Math.Max(1, opts.ShutdownTimeoutMilliseconds);

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(timeoutMs));
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken, timeoutCts.Token);
        var token = linkedCts.Token;

        var scopes = scopeManager.GetActiveScopes();
        logger.LogInformation(
            "Initiating distributed CRDT shutdown flush across {ScopeCount} replica scope(s). Reason: {Reason}, Mode: {FlushMode}, TimeoutMs: {TimeoutMs}.",
            scopes.Count,
            reason,
            opts.FlushModeOnShutdown,
            timeoutMs);

        foreach (var scope in scopes)
        {
            try
            {
                await FlushSingleScopeAsync(scope, opts, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                logger.LogWarning("Shutdown flush timed out or was cancelled while flushing replica '{ReplicaId}'.", scope.ReplicaId);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred while flushing replica '{ReplicaId}' during shutdown.", scope.ReplicaId);
            }
        }

        logger.LogInformation("Completed distributed CRDT shutdown flush across all replica scopes.");
    }

    private async Task FlushSingleScopeAsync(
        IDistributedCrdtScope scope,
        DistributedCrdtOptions opts,
        CancellationToken cancellationToken)
    {
        var orchestrator = scope.Orchestrator;

        // 1. Drain queued orchestrator commands (promotions, creations, topology evaluations)
        await orchestrator.FlushAsync(cancellationToken).ConfigureAwait(false);

        // 2. Drain shared document command pipeline so all in-flight intents/patches are applied and journaled
        var documentPipeline = scope.ServiceProvider.GetService<IDocumentCommandPipeline>();
        if (documentPipeline != null)
        {
            await documentPipeline.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        // 3. Flush debounced active-sync patches from all active documents
        IReadOnlyList<IDistributedCrdtDocument> documents = [];
        if (orchestrator.InitializationStage == OrchestratorInitializationStage.Initialized)
        {
            try
            {
                documents = orchestrator.GetActiveDocuments();
            }
            catch (InvalidOperationException)
            {
                documents = [];
            }
        }

        foreach (var document in documents)
        {
            await document.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        // 4. Drain outbound patch broadcaster channels so patches are pushed to P2P before transports close
        var patchObservers = scope.ServiceProvider.GetServices<IDocumentPatchObserver>();
        foreach (var observer in patchObservers)
        {
            await observer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        // 5. Optional final anti-entropy broadcast
        if (opts.FlushModeOnShutdown == CrdtShutdownFlushMode.FullSyncAndCheckpoint &&
            orchestrator.InitializationStage == OrchestratorInitializationStage.Initialized)
        {
            await orchestrator.DispatchAntiEntropyStateAsync(cancellationToken).ConfigureAwait(false);
        }

        // 6. Drain Write-Ahead Buffer (InMemoryBuffer or DiskWal) to underlying storage
        if (scope.Storage is BufferedDistributedCrdtStorageDecorator bufferedDecorator)
        {
            var maxBatchSize = opts.WalMaxBatchSize > 0 ? opts.WalMaxBatchSize : 500;
            await bufferedDecorator.DrainAsync(maxBatchSize, cancellationToken).ConfigureAwait(false);
        }

        // 7. Persist checkpoints, shard version vectors, and cluster states if configured
        if (opts.FlushModeOnShutdown is CrdtShutdownFlushMode.JournalAndCheckpoints or CrdtShutdownFlushMode.FullSyncAndCheckpoint &&
            orchestrator.InitializationStage == OrchestratorInitializationStage.Initialized)
        {
            var versionVectorManager = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();

            foreach (var document in documents)
            {
                await document.CheckpointAsync(cancellationToken).ConfigureAwait(false);
            }

            var safelyPersistedMap = versionVectorManager.GetFullSnapshot();
            await scope.Storage.SaveShardVersionVectorMapAsync(scope.ReplicaId, safelyPersistedMap, cancellationToken).ConfigureAwait(false);

            var exportedClusterState = scope.ClusterTracker.ExportState();
            await scope.Storage.SaveClusterStateAsync(scope.ReplicaId, exportedClusterState, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref isDisposed, 1) == 1)
        {
            return;
        }

        shutdownCts.Dispose();
    }
}