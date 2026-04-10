namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Hosted service responsible for initializing all registered distributed CRDT documents upon application startup.
/// This ensures that persistent storage providers are given the opportunity to load saved state before the network meshes sync.
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

            // 1. Initialize the global Dotted Version Vector scope tracking mechanism properly
            if (globalStorage != null)
            {
                var savedDvv = await globalStorage.LoadGlobalVersionVectorAsync(options.ReplicaId, cancellationToken).ConfigureAwait(false);
                if (savedDvv != null)
                {
                    var scopeFactory = rootServiceProvider.GetRequiredService<ICrdtScopeFactory>();
                    
                    // Re-instantiate directly via the scoped factory mechanism mapping the populated payload dynamically
                    var newScope = scopeFactory.CreateScope(options.ReplicaId, savedDvv);
                    scopeProvider.ReplaceScope(newScope);
                    
                    logger.LogInformation("Successfully re-initialized CRDT scope with restored global Dotted Version Vector for replica {ReplicaId}.", options.ReplicaId);
                }
            }

            // 2. Initialize the specifically tracked individual document data pipelines
            var documents = scopeProvider.Scope.ServiceProvider.GetRequiredService<IEnumerable<IDistributedCrdtDocument>>();

            foreach (var document in documents)
            {
                await document.InitializeAsync(cancellationToken).ConfigureAwait(false);
            }

            logger.LogInformation("Distributed CRDT documents successfully initialized from persistent storage providers.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An error occurred while initializing distributed CRDT documents and global state.");
        }
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}