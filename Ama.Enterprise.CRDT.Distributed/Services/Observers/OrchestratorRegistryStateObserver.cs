namespace Ama.Enterprise.CRDT.Distributed.Services.Observers;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

/// <summary>
/// Observes state changes on the system registry document and triggers orchestrator topology re-evaluation to promote or clean up documents.
/// </summary>
public sealed class OrchestratorRegistryStateObserver : IDocumentStateObserver<CrdtRegistryState>
{
    private readonly IServiceProvider serviceProvider;
    private readonly ILogger<OrchestratorRegistryStateObserver> logger;

    public OrchestratorRegistryStateObserver(
        IServiceProvider serviceProvider,
        ILogger<OrchestratorRegistryStateObserver> logger)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public Task OnStateChangedAsync(string documentId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId))
        {
            return Task.CompletedTask;
        }

        try
        {
            var orchestrator = serviceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
            return orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.FireAndForget, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to trigger orchestrator topology re-evaluation after registry state change on {DocumentId}.", documentId);
            return Task.CompletedTask;
        }
    }
}