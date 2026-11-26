namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Pooling;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;

internal sealed class DistributedCrdtScopeFactory : IDistributedCrdtScopeFactory
{
    private readonly ICrdtScopeFactory coreScopeFactory;
    private readonly IDistributedCrdtTelemetry telemetry;

    public DistributedCrdtScopeFactory(ICrdtScopeFactory coreScopeFactory, IDistributedCrdtTelemetry telemetry)
    {
        this.coreScopeFactory = coreScopeFactory ?? throw new ArgumentNullException(nameof(coreScopeFactory));
        this.telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
    }

    public IDistributedCrdtScope CreateScope(string replicaId)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));
        return CreateScope(replicaId, replicaId, isPinnedHostScope: false);
    }

    public IDistributedCrdtScope CreateScope(string scopeId, string hostReplicaId, bool isPinnedHostScope = false)
    {
        if (string.IsNullOrWhiteSpace(scopeId)) throw new ArgumentException("Scope ID cannot be null or empty.", nameof(scopeId));
        if (string.IsNullOrWhiteSpace(hostReplicaId)) throw new ArgumentException("Host Replica ID cannot be null or empty.", nameof(hostReplicaId));

        scopeId = CrdtStringPool.Shared.GetOrAdd(scopeId);
        hostReplicaId = CrdtStringPool.Shared.GetOrAdd(hostReplicaId);

        string compositeReplicaId;
        if (string.Equals(scopeId, hostReplicaId, StringComparison.Ordinal))
        {
            compositeReplicaId = scopeId;
        }
        else if (scopeId.Contains('@', StringComparison.Ordinal))
        {
            compositeReplicaId = scopeId;
            var atIndex = scopeId.IndexOf('@', StringComparison.Ordinal);
            if (atIndex > 0 && atIndex < scopeId.Length - 1)
            {
                hostReplicaId = CrdtStringPool.Shared.GetOrAdd(scopeId[..atIndex]);
                scopeId = CrdtStringPool.Shared.GetOrAdd(scopeId[(atIndex + 1)..]);
            }
        }
        else
        {
            compositeReplicaId = CrdtStringPool.Shared.GetOrAdd($"{hostReplicaId}@{scopeId}");
        }

        var scope = coreScopeFactory.CreateScope(compositeReplicaId);
        var scopeContext = scope.ServiceProvider.GetService<CrdtScopeContext>();
        if (scopeContext != null)
        {
            scopeContext.ScopeId = scopeId;
            scopeContext.HostReplicaId = hostReplicaId;
            scopeContext.ReplicaId = compositeReplicaId;
        }

        telemetry.RecordScopeGenerated(compositeReplicaId);
        return new DistributedCrdtScope(scope, compositeReplicaId, scopeId, hostReplicaId, isPinnedHostScope);
    }
}

internal sealed class DistributedCrdtScope : IDistributedCrdtScope
{
    private readonly IServiceScope innerScope;
    private readonly SemaphoreSlim initializationLock = new(1, 1);
    private long lastActivityTimestamp;

    public string ReplicaId { get; }
    public string ScopeId { get; }
    public string HostReplicaId { get; }
    public bool IsPinnedHostScope { get; }
    public long LastActivityTimestamp => Volatile.Read(ref lastActivityTimestamp);
    public ICrdtDocumentOrchestrator Orchestrator { get; }
    public IClusterStateTracker ClusterTracker { get; }
    public IDistributedCrdtStorage Storage { get; }
    public ICrdtEvictionService EvictionService { get; }
    public IServiceProvider ServiceProvider => innerScope.ServiceProvider;

    public DistributedCrdtScope(
        IServiceScope innerScope,
        string replicaId,
        string scopeId,
        string hostReplicaId,
        bool isPinnedHostScope)
    {
        this.innerScope = innerScope ?? throw new ArgumentNullException(nameof(innerScope));
        ReplicaId = replicaId;
        ScopeId = scopeId;
        HostReplicaId = hostReplicaId;
        IsPinnedHostScope = isPinnedHostScope;
        lastActivityTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();
        Orchestrator = innerScope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        ClusterTracker = innerScope.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        Storage = innerScope.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        EvictionService = innerScope.ServiceProvider.GetRequiredService<ICrdtEvictionService>();
    }

    public void TouchActivity()
    {
        Volatile.Write(ref lastActivityTimestamp, System.Diagnostics.Stopwatch.GetTimestamp());
    }

    public async ValueTask EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        TouchActivity();

        if (Orchestrator.InitializationStage == OrchestratorInitializationStage.Initialized)
        {
            return;
        }

        await initializationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Orchestrator.InitializationStage != OrchestratorInitializationStage.Initialized)
            {
                await Orchestrator.InitializeAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            initializationLock.Release();
        }
    }

    public void Dispose()
    {
        initializationLock.Dispose();
        innerScope.Dispose();
    }
}