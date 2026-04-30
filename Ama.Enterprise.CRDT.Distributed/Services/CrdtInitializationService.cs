namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Hosted service responsible for initializing all registered distributed CRDT documents upon application startup.
/// This ensures that persistent storage providers load saved states directly into securely maintained memory scopes seamlessly.
/// </summary>
public sealed class CrdtInitializationService(
    DistributedCrdtScopeProvider scopeProvider,
    IServiceProvider rootServiceProvider,
    ILogger<CrdtInitializationService> logger) : IHostedService
{
    private readonly DistributedCrdtScopeProvider scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
    private readonly IServiceProvider rootServiceProvider = rootServiceProvider ?? throw new ArgumentNullException(nameof(rootServiceProvider));
    private readonly ILogger<CrdtInitializationService> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Initializing distributed CRDT global state and dynamically orchestrated documents...");

        try
        {
            var options = rootServiceProvider.GetRequiredService<IOptions<DistributedCrdtOptions>>().Value;
            var globalStorage = rootServiceProvider.GetService<IDistributedCrdtStorage>();

            // 1. Initialize the global Dotted Version Vector scope tracking mechanism properly in-place
            if (globalStorage != null)
            {
                var savedDvv = await globalStorage.LoadGlobalVersionVectorAsync(options.ReplicaId, cancellationToken).ConfigureAwait(false);
                if (savedDvv != null)
                {
                    var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();
                    
                    // Safely mutate the existing context natively explicitly avoiding catastrophic scope replacement anomalies
                    lock (replicaContext.GlobalVersionVector)
                    {
                        replicaContext.GlobalVersionVector.Versions.Clear();
                        foreach (var kvp in savedDvv.Versions)
                        {
                            replicaContext.GlobalVersionVector.Versions[kvp.Key] = kvp.Value;
                        }

                        replicaContext.GlobalVersionVector.Dots.Clear();
                        foreach (var kvp in savedDvv.Dots)
                        {
                            replicaContext.GlobalVersionVector.Dots[kvp.Key] = new HashSet<long>(kvp.Value);
                        }
                    }
                    
                    logger.LogInformation("Successfully re-initialized in-place CRDT global Dotted Version Vector for replica {ReplicaId}.", options.ReplicaId);
                }
            }

            // 2. Initialize the dynamic document orchestrator
            var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
            await orchestrator.InitializeAsync(cancellationToken).ConfigureAwait(false);

            // 3. Replay uncheckpointed WAL operations mapping local state back
            if (globalStorage != null)
            {
                IDictionary<string, List<CrdtOperation>> operationsByDoc = new Dictionary<string, List<CrdtOperation>>();
                
                await foreach (var jOp in globalStorage.GetAllJournaledOperationsAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (!operationsByDoc.TryGetValue(jOp.DocumentId, out var opList))
                    {
                        opList = new List<CrdtOperation>();
                        operationsByDoc[jOp.DocumentId] = opList;
                    }
                    opList.Add(jOp.Operation);
                }

                if (operationsByDoc.Count > 0)
                {
                    logger.LogInformation("Replaying {Count} journaled operations.", operationsByDoc.Values.Sum(l => l.Count));
                    
                    // CRITICAL: Replay registry operations FIRST so the orchestrator correctly creates new generic local docs before routing their patches.
                    if (operationsByDoc.TryGetValue(orchestrator.Registry.DocumentId, out var registryOps))
                    {
                        await orchestrator.Registry.ApplyOperationsAsync(registryOps, cancellationToken).ConfigureAwait(false);
                        await orchestrator.SyncDocumentsAsync(cancellationToken).ConfigureAwait(false);
                    }

                    var documents = orchestrator.GetActiveDocuments();
                    
                    foreach (var document in documents)
                    {
                        if (document.DocumentId != orchestrator.Registry.DocumentId && operationsByDoc.TryGetValue(document.DocumentId, out var docOps))
                        {
                            await document.ApplyOperationsAsync(docOps, cancellationToken).ConfigureAwait(false);
                        }
                    }
                }
            }

            logger.LogInformation("Distributed CRDT documents successfully initialized.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An error occurred while initializing distributed CRDT documents.");
        }
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}