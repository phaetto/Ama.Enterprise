namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Background service responsible for repeatedly broadcasting the local synchronization state of all registered CRDTs to trigger convergence.
/// </summary>
public sealed class CrdtAntiEntropyService : BackgroundService
{
    private readonly DistributedCrdtScopeProvider scopeProvider;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtAntiEntropyService> logger;

    public CrdtAntiEntropyService(
        DistributedCrdtScopeProvider scopeProvider,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtAntiEntropyService> logger)
    {
        this.scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var initialDelay = TimeSpan.FromSeconds(Math.Max(0, options.Value.AntiEntropyInitialDelaySeconds));
        var interval = TimeSpan.FromSeconds(Math.Max(1, options.Value.AntiEntropyIntervalSeconds));

        if (initialDelay > TimeSpan.Zero)
        {
            // Initial delay to allow network nodes to discover each other
            await Task.Delay(initialDelay, stoppingToken).ConfigureAwait(false);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
                var documents = orchestrator.GetActiveDocuments();

                foreach (var document in documents)
                {
                    await document.BroadcastStateAsync(stoppingToken).ConfigureAwait(false);

                    try
                    {
                        // Network smoothing logic: Introduces a 50ms structural jitter preventing catastrophic "Thundering Herd" packet overflow storms smoothly naturally across dense document collections.
                        await Task.Delay(50, stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred during CRDT distributed anti-entropy structural broadcast.");
            }

            try
            {
                // Delay between synchronization rounds natively
                await Task.Delay(interval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}