namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Background hosted service that drains buffered operations from <see cref="BufferedDistributedCrdtStorageDecorator"/>
/// across all active replica scopes and flushes them in batches to the underlying <see cref="IDistributedCrdtStorage"/>.
/// </summary>
public sealed class CrdtJournalFlushService : BackgroundService
{
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtJournalFlushService> logger;
    private readonly ConcurrentDictionary<string, Task> scopeLoops = new(StringComparer.Ordinal);

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

        if (this.scopeLoops.Count > 0)
        {
            try
            {
                await Task.WhenAll(this.scopeLoops.Values).ConfigureAwait(false);
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

        var pollInterval = TimeSpan.FromMilliseconds(Math.Max(10, this.options.Value.WalFlushIntervalMilliseconds));

        while (!stoppingToken.IsCancellationRequested)
        {
            foreach (var scope in this.scopeManager.GetActiveScopes())
            {
                if (scope.Storage is BufferedDistributedCrdtStorageDecorator decorator)
                {
                    this.scopeLoops.GetOrAdd(scope.ReplicaId, _ => this.RunScopeFlushLoopAsync(scope.ReplicaId, decorator, stoppingToken));
                }
            }

            try
            {
                await Task.Delay(pollInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task RunScopeFlushLoopAsync(
        string replicaId,
        BufferedDistributedCrdtStorageDecorator decorator,
        CancellationToken stoppingToken)
    {
        var maxBatchSize = this.options.Value.WalMaxBatchSize > 0 ? this.options.Value.WalMaxBatchSize : 500;
        var flushInterval = TimeSpan.FromMilliseconds(Math.Max(1, this.options.Value.WalFlushIntervalMilliseconds));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var flushed = await decorator.FlushNextBatchAsync(maxBatchSize, flushInterval, stoppingToken).ConfigureAwait(false);
                if (flushed == 0)
                {
                    await Task.Delay(flushInterval, stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Error flushing WAL batch to storage for replica '{ReplicaId}'.", replicaId);
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
    }
}