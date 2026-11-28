namespace Ama.Enterprise.CRDT.Distributed.Services.Lifecycle;

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.Storage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Background hosted service that multiplexes WAL flushing across all active replica scopes with pending
/// <see cref="BufferedDistributedCrdtStorageDecorator"/> operations, avoiding one persistent background loop per idle scope.
/// </summary>
public sealed class CrdtJournalFlushService : BackgroundService
{
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtJournalFlushService> logger;
    private readonly ConcurrentDictionary<string, Task> activeScopeFlushes = new(StringComparer.Ordinal);

    /// <summary>
    /// Initializes a new instance of the <see cref="CrdtJournalFlushService"/> class.
    /// </summary>
    /// <param name="scopeManager">The replica scope manager.</param>
    /// <param name="options">The distributed CRDT options.</param>
    /// <param name="logger">The logger instance.</param>
    public CrdtJournalFlushService(
        DistributedCrdtScopeManager scopeManager,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtJournalFlushService> logger)
    {
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken).ConfigureAwait(false);

        if (!this.activeScopeFlushes.IsEmpty)
        {
            try
            {
                await Task.WhenAll(this.activeScopeFlushes.Values).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
        }

        var maxBatchSize = this.options.Value.WalMaxBatchSize > 0 ? this.options.Value.WalMaxBatchSize : 500;
        foreach (var scope in this.scopeManager.GetActiveScopes())
        {
            if (scope.Storage is BufferedDistributedCrdtStorageDecorator decorator)
            {
                try
                {
                    await decorator.DrainAsync(maxBatchSize, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    this.logger.LogError(ex, "Failed to drain remaining WAL buffer operations during shutdown for replica '{ReplicaId}'.", scope.ReplicaId);
                }
            }
        }
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (this.options.Value.JournalMode == CrdtJournalMode.Direct)
        {
            return;
        }

        var maxBatchSize = this.options.Value.WalMaxBatchSize > 0 ? this.options.Value.WalMaxBatchSize : 500;
        var flushInterval = TimeSpan.FromMilliseconds(Math.Max(1, this.options.Value.WalFlushIntervalMilliseconds));

        while (!stoppingToken.IsCancellationRequested)
        {
            var scopes = this.scopeManager.GetActiveScopes();
            for (int i = 0; i < scopes.Count; i++)
            {
                var scope = scopes[i];
                if (scope.Storage is BufferedDistributedCrdtStorageDecorator decorator &&
                    decorator.WriteAheadBuffer.PendingCount > 0 &&
                    !this.activeScopeFlushes.ContainsKey(scope.ReplicaId))
                {
                    this.StartScopeDrainTask(scope.ReplicaId, decorator, maxBatchSize, flushInterval, stoppingToken);
                }
            }

            try
            {
                await Task.Delay(flushInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private void StartScopeDrainTask(
        string replicaId,
        BufferedDistributedCrdtStorageDecorator decorator,
        int maxBatchSize,
        TimeSpan flushInterval,
        CancellationToken stoppingToken)
    {
        this.activeScopeFlushes.GetOrAdd(
            replicaId,
            id => this.DrainPendingScopeBatchesAsync(id, decorator, maxBatchSize, flushInterval, stoppingToken));
    }

    private async Task DrainPendingScopeBatchesAsync(
        string replicaId,
        BufferedDistributedCrdtStorageDecorator decorator,
        int maxBatchSize,
        TimeSpan flushInterval,
        CancellationToken stoppingToken)
    {
        await Task.Yield();

        try
        {
            while (!stoppingToken.IsCancellationRequested && decorator.WriteAheadBuffer.PendingCount > 0)
            {
                var flushed = await decorator.FlushNextBatchAsync(maxBatchSize, flushInterval, stoppingToken).ConfigureAwait(false);
                if (flushed == 0)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Graceful stop
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Error flushing WAL batch to storage for replica '{ReplicaId}'.", replicaId);
        }
        finally
        {
            this.activeScopeFlushes.TryRemove(replicaId, out _);
        }
    }
}