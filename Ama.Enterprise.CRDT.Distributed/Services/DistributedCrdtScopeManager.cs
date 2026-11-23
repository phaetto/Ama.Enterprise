namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Pooling;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Singleton manager tracking active CRDT replica scopes and dynamically creating isolated causal compartments on demand.
/// </summary>
public sealed class DistributedCrdtScopeManager : IDisposable
{
    private readonly IDistributedCrdtScopeFactory scopeFactory;
    private readonly string defaultHostReplicaId;
    private readonly HashSet<string> registeredReplicaIds = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, IDistributedCrdtScope> activeScopes = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, IDistributedCrdtScope> scopesByCompositeId = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim initLock = new(1, 1);

    private readonly Meter meter;
    private readonly Counter<long> scopesCreatedCounter;

    /// <summary>
    /// Gets the primary host replica identifier registered at startup.
    /// </summary>
    public string DefaultHostReplicaId => defaultHostReplicaId;

    public DistributedCrdtScopeManager(
        IDistributedCrdtScopeFactory scopeFactory,
        IEnumerable<DistributedCrdtReplicaRegistration> registrations,
        IMeterFactory? meterFactory = null)
    {
        this.scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.DistributedCrdtScopeManager") ?? new Meter("Ama.Enterprise.CRDT.Distributed.DistributedCrdtScopeManager");
        this.scopesCreatedCounter = this.meter.CreateCounter<long>("crdt.scope.manager_allocations", "scopes", "Total managed CRDT scope allocations");

        var firstHost = string.Empty;
        if (registrations != null)
        {
            foreach (var reg in registrations)
            {
                if (!string.IsNullOrWhiteSpace(reg.ReplicaId))
                {
                    var pooled = CrdtStringPool.Shared.GetOrAdd(reg.ReplicaId);
                    registeredReplicaIds.Add(pooled);
                    if (string.IsNullOrEmpty(firstHost))
                    {
                        firstHost = pooled;
                    }
                }
            }
        }

        this.defaultHostReplicaId = firstHost;

        foreach (var regId in registeredReplicaIds)
        {
            GetOrCreateScope(regId);
        }
    }

    /// <summary>
    /// Gets an existing scope or creates a new scope synchronously for the specified scope or replica identifier.
    /// </summary>
    /// <param name="scopeOrReplicaId">The logical scope ID or registered replica ID.</param>
    /// <param name="hostReplicaId">The optional physical host replica ID.</param>
    /// <returns>The resolved <see cref="IDistributedCrdtScope"/>.</returns>
    public IDistributedCrdtScope GetOrCreateScope(string scopeOrReplicaId, string? hostReplicaId = null)
    {
        if (string.IsNullOrWhiteSpace(scopeOrReplicaId))
        {
            throw new ArgumentException("Scope or Replica ID cannot be null or empty.", nameof(scopeOrReplicaId));
        }

        scopeOrReplicaId = CrdtStringPool.Shared.GetOrAdd(scopeOrReplicaId);

        if (activeScopes.TryGetValue(scopeOrReplicaId, out var existing))
        {
            return existing;
        }

        if (scopesByCompositeId.TryGetValue(scopeOrReplicaId, out var existingComposite))
        {
            return existingComposite;
        }

        var resolvedHost = ResolveHostReplicaId(scopeOrReplicaId, hostReplicaId);
        var normalizedScopeId = NormalizeScopeKey(scopeOrReplicaId, resolvedHost);

        var scope = activeScopes.GetOrAdd(normalizedScopeId, id =>
        {
            scopesCreatedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", id));
            return scopeFactory.CreateScope(id, resolvedHost);
        });

        scopesByCompositeId.TryAdd(scope.ReplicaId, scope);
        return scope;
    }

    /// <summary>
    /// Gets an existing scope or creates and initializes a new scope asynchronously for the specified scope identifier.
    /// </summary>
    /// <param name="scopeOrReplicaId">The logical scope ID or registered replica ID.</param>
    /// <param name="hostReplicaId">The optional physical host replica ID.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The initialized <see cref="IDistributedCrdtScope"/>.</returns>
    public async ValueTask<IDistributedCrdtScope> GetOrCreateScopeAsync(
        string scopeOrReplicaId,
        string? hostReplicaId = null,
        CancellationToken cancellationToken = default)
    {
        var scope = GetOrCreateScope(scopeOrReplicaId, hostReplicaId);
        if (scope.Orchestrator.InitializationStage == OrchestratorInitializationStage.Initialized)
        {
            return scope;
        }

        await initLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (scope.Orchestrator.InitializationStage != OrchestratorInitializationStage.Initialized)
            {
                await scope.Orchestrator.InitializeAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            initLock.Release();
        }

        return scope;
    }

    /// <summary>
    /// Attempts to retrieve an already active scope by its scope ID or composite replica ID without creating a new one.
    /// </summary>
    /// <param name="scopeOrReplicaId">The scope ID or composite replica ID.</param>
    /// <param name="scope">When this method returns, contains the active scope if found.</param>
    /// <returns>True if the scope is active; otherwise, false.</returns>
    public bool TryGetScope(string scopeOrReplicaId, out IDistributedCrdtScope? scope)
    {
        if (string.IsNullOrWhiteSpace(scopeOrReplicaId))
        {
            scope = null;
            return false;
        }

        if (activeScopes.TryGetValue(scopeOrReplicaId, out scope))
        {
            return true;
        }

        if (scopesByCompositeId.TryGetValue(scopeOrReplicaId, out scope))
        {
            return true;
        }

        var atIdx = scopeOrReplicaId.IndexOf('@', StringComparison.Ordinal);
        if (atIdx > 0 && atIdx < scopeOrReplicaId.Length - 1)
        {
            var extractedScopeId = scopeOrReplicaId[(atIdx + 1)..];
            if (activeScopes.TryGetValue(extractedScopeId, out scope))
            {
                return true;
            }
        }

        scope = null;
        return false;
    }

    /// <summary>
    /// Returns a snapshot list of all currently active CRDT scopes on this node.
    /// </summary>
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
        scopesByCompositeId.Clear();
        initLock.Dispose();
        meter.Dispose();
    }

    private string ResolveHostReplicaId(string scopeOrReplicaId, string? explicitHostReplicaId)
    {
        if (!string.IsNullOrWhiteSpace(explicitHostReplicaId))
        {
            return CrdtStringPool.Shared.GetOrAdd(explicitHostReplicaId);
        }

        if (registeredReplicaIds.Contains(scopeOrReplicaId))
        {
            return scopeOrReplicaId;
        }

        var atIdx = scopeOrReplicaId.IndexOf('@', StringComparison.Ordinal);
        if (atIdx > 0 && atIdx < scopeOrReplicaId.Length - 1)
        {
            return CrdtStringPool.Shared.GetOrAdd(scopeOrReplicaId[..atIdx]);
        }

        if (!string.IsNullOrWhiteSpace(defaultHostReplicaId))
        {
            return defaultHostReplicaId;
        }

        return scopeOrReplicaId;
    }

    private static string NormalizeScopeKey(string scopeOrReplicaId, string resolvedHost)
    {
        var atIdx = scopeOrReplicaId.IndexOf('@', StringComparison.Ordinal);
        if (atIdx > 0 && atIdx < scopeOrReplicaId.Length - 1)
        {
            var prefix = scopeOrReplicaId[..atIdx];
            if (string.Equals(prefix, resolvedHost, StringComparison.Ordinal))
            {
                return CrdtStringPool.Shared.GetOrAdd(scopeOrReplicaId[(atIdx + 1)..]);
            }
        }

        return scopeOrReplicaId;
    }
}