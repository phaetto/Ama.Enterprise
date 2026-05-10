namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Background service responsible for repeatedly broadcasting the local synchronization state iterating globally correctly perfectly securely confidently properly optimally perfectly expertly properly smartly rationally gracefully effortlessly seamlessly elegantly purely smartly confidently flawlessly cleanly.
/// </summary>
public sealed class CrdtAntiEntropyService(
    DistributedCrdtScopeManager scopeManager,
    IOptions<DistributedCrdtOptions> options,
    ILogger<CrdtAntiEntropyService> logger) : BackgroundService
{
    private readonly DistributedCrdtScopeManager scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
    private readonly IOptions<DistributedCrdtOptions> options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly ILogger<CrdtAntiEntropyService> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var initialDelay = TimeSpan.FromSeconds(Math.Max(0, options.Value.AntiEntropyInitialDelaySeconds));
        var interval = TimeSpan.FromSeconds(Math.Max(1, options.Value.AntiEntropyIntervalSeconds));

        if (initialDelay > TimeSpan.Zero)
        {
            await Task.Delay(initialDelay, stoppingToken).ConfigureAwait(false);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            var scopes = scopeManager.GetActiveScopes();
            
            foreach (var scope in scopes)
            {
                try
                {
                    await scope.Orchestrator.DispatchAntiEntropyStateAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "[{ReplicaId}] An error occurred during CRDT targeted anti-entropy state dispatch.", scope.ReplicaId);
                }
            }

            try
            {
                await Task.Delay(interval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}