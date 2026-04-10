namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// <summary>
/// Background service responsible for repeatedly broadcasting the local synchronization state of all registered CRDTs to trigger convergence.
/// </summary>
public sealed class CrdtAntiEntropyService : BackgroundService
{
    private readonly DistributedCrdtScopeProvider scopeProvider;
    private readonly ILogger<CrdtAntiEntropyService> logger;

    public CrdtAntiEntropyService(
        DistributedCrdtScopeProvider scopeProvider,
        ILogger<CrdtAntiEntropyService> logger)
    {
        this.scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Initial delay to allow network nodes to discover each other
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken).ConfigureAwait(false);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var documents = scopeProvider.Scope.ServiceProvider.GetRequiredService<IEnumerable<IDistributedCrdtDocument>>();

                foreach (var document in documents)
                {
                    await document.BroadcastStateAsync(stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred during CRDT distributed anti-entropy broadcast.");
            }

            try
            {
                // Delay between synchronization rounds
                await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}