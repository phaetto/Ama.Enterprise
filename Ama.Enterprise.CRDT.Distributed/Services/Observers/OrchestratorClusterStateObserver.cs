namespace Ama.Enterprise.CRDT.Distributed.Services.Observers;

using System;
using System.Threading;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

/// <summary>
/// Observes cluster state topologies explicitly triggering orchestrator re-evaluations dynamically promoting or demoting documents.
/// </summary>
public sealed class OrchestratorClusterStateObserver : IClusterStateObserver
{
    private readonly IServiceProvider serviceProvider;
    private readonly ILogger<OrchestratorClusterStateObserver> logger;

    public OrchestratorClusterStateObserver(
        IServiceProvider serviceProvider,
        ILogger<OrchestratorClusterStateObserver> logger)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public void OnClusterTopologyChanged()
    {
        logger.LogInformation("Cluster state topology change detected. Triggering orchestrator topology re-evaluation.");
        
        try
        {
            // Resolve dynamically to prevent circular dependencies at startup
            // Orchestrator -> ShardDistributionManager -> ClusterStateTracker -> OrchestratorClusterStateObserver -> Orchestrator
            var orchestrator = serviceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
            _ = orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.FireAndForget, CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to re-evaluate orchestrator topology.");
        }
    }
}