namespace Ama.Enterprise.FeatureFlags.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// <summary>
/// Background service eagerly bootstrapping the singleton feature flags global state.
/// </summary>
internal sealed class FeatureFlagBootstrapper : IHostedService
{
    private readonly DistributedCrdtScopeProvider scopeProvider;
    private readonly ILogger<FeatureFlagBootstrapper> logger;

    public FeatureFlagBootstrapper(
        DistributedCrdtScopeProvider scopeProvider,
        ILogger<FeatureFlagBootstrapper> logger)
    {
        this.scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
            
            // Eagerly inject the global feature flags singleton into the document orchestrator pool
            await orchestrator.CreateDocumentAsync("feature-flags-singleton", "feature-flag", cancellationToken).ConfigureAwait(false);
            
            // Explicitly force map synchronization here at the bootstrapper edge initializing structures before host routing begins.
            await orchestrator.SyncDocumentsAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to bootstrap global feature flag document during initialization natively.");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}