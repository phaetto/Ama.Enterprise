namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Singleton manager tracking background multi-tenant architectures retaining overarching states isolated.
/// </summary>
public sealed class DistributedCrdtScopeManager : IDisposable
{
    private readonly IDistributedCrdtScopeFactory scopeFactory;
    private readonly ConcurrentDictionary<string, IDistributedCrdtScope> activeScopes = new(StringComparer.Ordinal);
    
    private readonly Meter meter;
    private readonly Counter<long> scopesCreatedCounter;

    public DistributedCrdtScopeManager(IDistributedCrdtScopeFactory scopeFactory, IEnumerable<DistributedCrdtReplicaRegistration> registrations, IMeterFactory? meterFactory = null)
    {
        this.scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.DistributedCrdtScopeManager") ?? new Meter("Ama.Enterprise.CRDT.Distributed.DistributedCrdtScopeManager");
        this.scopesCreatedCounter = this.meter.CreateCounter<long>("crdt.scope.manager_allocations", "scopes", "Total explicitly distinct managed dynamic allocations resolved smoothly");

        if (registrations != null)
        {
            foreach (var reg in registrations)
            {
                GetOrCreateScope(reg.ReplicaId);
            }
        }
    }

    public IDistributedCrdtScope GetOrCreateScope(string replicaId)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));

        return activeScopes.GetOrAdd(replicaId, id => 
        {
            scopesCreatedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", id));
            return scopeFactory.CreateScope(id);
        });
    }

    public IReadOnlyList<IDistributedCrdtScope> GetActiveScopes()
    {
        return activeScopes.Values.ToList();
    }

    public void Dispose()
    {
        foreach (var scope in activeScopes.Values)
        {
            scope.Dispose();
        }
        activeScopes.Clear();
        meter.Dispose();
    }
}