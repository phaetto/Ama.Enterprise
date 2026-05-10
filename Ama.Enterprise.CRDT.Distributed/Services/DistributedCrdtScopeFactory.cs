namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using Ama.CRDT.Services;
using Microsoft.Extensions.DependencyInjection;

internal sealed class DistributedCrdtScopeFactory(ICrdtScopeFactory coreScopeFactory) : IDistributedCrdtScopeFactory
{
    private readonly ICrdtScopeFactory coreScopeFactory = coreScopeFactory ?? throw new ArgumentNullException(nameof(coreScopeFactory));

    public IDistributedCrdtScope CreateScope(string replicaId)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));
        
        var scope = coreScopeFactory.CreateScope(replicaId);
        return new DistributedCrdtScope(scope, replicaId);
    }
}

internal sealed class DistributedCrdtScope : IDistributedCrdtScope
{
    private readonly IServiceScope innerScope;

    public string ReplicaId { get; }
    public ICrdtDocumentOrchestrator Orchestrator { get; }
    public IClusterStateTracker ClusterTracker { get; }
    public IDistributedCrdtStorage Storage { get; }
    public ICrdtEvictionService EvictionService { get; }
    public IServiceProvider ServiceProvider => innerScope.ServiceProvider;

    public DistributedCrdtScope(IServiceScope innerScope, string replicaId)
    {
        this.innerScope = innerScope ?? throw new ArgumentNullException(nameof(innerScope));
        ReplicaId = replicaId;
        Orchestrator = innerScope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        ClusterTracker = innerScope.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        Storage = innerScope.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        EvictionService = innerScope.ServiceProvider.GetRequiredService<ICrdtEvictionService>();
    }

    public void Dispose()
    {
        innerScope.Dispose();
    }
}