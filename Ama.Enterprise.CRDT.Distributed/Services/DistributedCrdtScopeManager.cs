namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Singleton manager tracking background multi-tenant architectures retaining overarching states isolated.
/// </summary>
public sealed class DistributedCrdtScopeManager : IDisposable
{
    private readonly IDistributedCrdtScopeFactory scopeFactory;
    private readonly ConcurrentDictionary<string, IDistributedCrdtScope> activeScopes = new(StringComparer.Ordinal);

    public DistributedCrdtScopeManager(IDistributedCrdtScopeFactory scopeFactory, IEnumerable<DistributedCrdtReplicaRegistration> registrations)
    {
        this.scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));

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

        return activeScopes.GetOrAdd(replicaId, id => scopeFactory.CreateScope(id));
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
    }
}