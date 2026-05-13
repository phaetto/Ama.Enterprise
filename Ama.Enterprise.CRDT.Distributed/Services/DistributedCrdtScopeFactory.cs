namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Diagnostics.Metrics;
using Ama.CRDT.Services;
using Microsoft.Extensions.DependencyInjection;

internal sealed class DistributedCrdtScopeFactory : IDistributedCrdtScopeFactory, IDisposable
{
    private readonly ICrdtScopeFactory coreScopeFactory;
    private readonly Meter meter;
    private readonly Counter<long> scopesGeneratedCounter;

    public DistributedCrdtScopeFactory(ICrdtScopeFactory coreScopeFactory, IMeterFactory? meterFactory = null)
    {
        this.coreScopeFactory = coreScopeFactory ?? throw new ArgumentNullException(nameof(coreScopeFactory));
        
        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.DistributedCrdtScopeFactory") ?? new Meter("Ama.Enterprise.CRDT.Distributed.DistributedCrdtScopeFactory");
        this.scopesGeneratedCounter = this.meter.CreateCounter<long>("crdt.scope.generated", "scopes", "Total explicitly distinct internal generic bounds generated dynamically");
    }

    public IDistributedCrdtScope CreateScope(string replicaId)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));
        
        var scope = coreScopeFactory.CreateScope(replicaId);
        
        scopesGeneratedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaId));
        return new DistributedCrdtScope(scope, replicaId);
    }

    public void Dispose()
    {
        meter.Dispose();
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