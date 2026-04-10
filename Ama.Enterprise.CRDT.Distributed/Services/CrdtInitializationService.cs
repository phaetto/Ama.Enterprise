namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
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
public sealed class CrdtInitializationService : IHostedService
{
    private readonly DistributedCrdtScopeProvider scopeProvider;
    private readonly IServiceProvider rootServiceProvider;
    private readonly ILogger<CrdtInitializationService> logger;

    public CrdtInitializationService(
        DistributedCrdtScopeProvider scopeProvider,
        IServiceProvider rootServiceProvider,
        ILogger<CrdtInitializationService> logger)
    {
        this.scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
        this.rootServiceProvider = rootServiceProvider ?? throw new ArgumentNullException(nameof(rootServiceProvider));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Initializing distributed CRDT global state and documents...");

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
                    
                    logger.LogInformation("Successfully re-initialized in-place CRDT global Dotted Version Vector bounds natively for replica {ReplicaId}.", options.ReplicaId);
                }
            }

            // 2. Initialize the specifically tracked individual document data pipelines
            var documents = scopeProvider.Scope.ServiceProvider.GetRequiredService<IEnumerable<IDistributedCrdtDocument>>();

            foreach (var document in documents)
            {
                await document.InitializeAsync(cancellationToken).ConfigureAwait(false);
            }

            logger.LogInformation("Distributed CRDT documents successfully initialized structurally from persistent storage providers.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An error occurred while seamlessly initializing distributed CRDT documents and global state.");
        }
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}