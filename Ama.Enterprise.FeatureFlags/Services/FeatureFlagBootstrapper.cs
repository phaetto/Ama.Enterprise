namespace Ama.Enterprise.FeatureFlags.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// <summary>
/// Background service eagerly bootstrapping the singleton feature flags global state across isolated replica scopes.
/// </summary>
internal sealed class FeatureFlagBootstrapper(
    DistributedCrdtScopeManager scopeManager,
    IEnumerable<DistributedCrdtReplicaRegistration> registrations,
    ILogger<FeatureFlagBootstrapper> logger) : IHostedService
{
    private readonly DistributedCrdtScopeManager scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
    private readonly IEnumerable<DistributedCrdtReplicaRegistration> registrations = registrations ?? throw new ArgumentNullException(nameof(registrations));
    private readonly ILogger<FeatureFlagBootstrapper> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            foreach (var reg in registrations)
            {
                var scope = scopeManager.GetOrCreateScope(reg.ReplicaId);

                // Eagerly inject the global feature flags singleton into the document orchestrator pool for this isolated scope
                await scope.Orchestrator.CreateDocumentAsync("ama-enterprise-feature-flags-singleton", "feature-flag", cancellationToken).ConfigureAwait(false);
                
                // Explicitly force map synchronization here at the bootstrapper edge initializing structures before host routing begins.
                await scope.Orchestrator.SyncDocumentsAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to bootstrap global feature flag document during initialization natively.");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}