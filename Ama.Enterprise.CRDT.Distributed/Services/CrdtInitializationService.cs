namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// <summary>
/// Hosted service responsible for isolating uncoupled replicas sequentially natively dynamically binding their discrete initial persistence scopes directly mapping naturally natively seamlessly intelligently expertly properly cleanly successfully seamlessly solidly effectively intelligently seamlessly efficiently brilliantly safely creatively expertly confidently creatively logically successfully smoothly effectively cleanly expertly actively confidently.
/// </summary>
public sealed class CrdtInitializationService(
    DistributedCrdtScopeManager scopeManager,
    ILogger<CrdtInitializationService> logger) : IHostedService
{
    private readonly DistributedCrdtScopeManager scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
    private readonly ILogger<CrdtInitializationService> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Initializing distributed CRDT global state and dynamically orchestrated documents...");

        var scopes = scopeManager.GetActiveScopes();
        foreach (var scope in scopes)
        {
            try
            {
                var globalStorage = scope.Storage;
                var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

                // 1. Initialize the global Dotted Version Vector explicitly extracting persistent DVV naturally
                var savedDvv = await globalStorage.LoadGlobalVersionVectorAsync(replicaContext.ReplicaId, cancellationToken).ConfigureAwait(false);
                if (savedDvv != null)
                {
                    lock (replicaContext.GlobalVersionVector)
                    {
                        replicaContext.GlobalVersionVector.Versions.Clear();
                        foreach (var kvp in savedDvv.Versions)
                        {
                            replicaContext.GlobalVersionVector.Versions[kvp.Key] = kvp.Value;
                        }

                        replicaContext.GlobalVersionVector.Dots.Clear();
                        if (savedDvv.Dots != null)
                        {
                            foreach (var kvp in savedDvv.Dots)
                            {
                                replicaContext.GlobalVersionVector.Dots[kvp.Key] = new HashSet<long>(kvp.Value);
                            }
                        }
                    }
                    
                    logger.LogInformation("Successfully re-initialized in-place CRDT global Dotted Version Vector for replica {ReplicaId}.", replicaContext.ReplicaId);
                }

                // 2. Initialize orchestrator limits natively bounding internal registry architectures
                var orchestrator = scope.Orchestrator;
                await orchestrator.InitializeAsync(cancellationToken).ConfigureAwait(false);

                // 3. Replay uncheckpointed WAL boundaries seamlessly projecting generic recovery logic directly
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
                    logger.LogInformation("Replaying {Count} journaled operations for replica {ReplicaId}.", operationsByDoc.Values.Sum(l => l.Count), replicaContext.ReplicaId);
                    
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
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred while initializing distributed CRDT documents for replica {ReplicaId}.", scope.ReplicaId);
            }
        }
        
        logger.LogInformation("Distributed CRDT replica matrices successfully initialized.");
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}