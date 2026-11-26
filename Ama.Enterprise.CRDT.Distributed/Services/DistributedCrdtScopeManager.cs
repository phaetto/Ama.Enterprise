namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Pooling;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;

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
    private readonly ConcurrentDictionary<string, SemaphoreSlim> fallbackInitLocks = new(StringComparer.Ordinal);

    private readonly Meter meter;
    private readonly Counter<long> scopesCreatedCounter;
    private readonly Counter<long> scopesEvictedCounter;

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
        this.scopesEvictedCounter = this.meter.CreateCounter<long>("crdt.scope.manager_evictions", "scopes", "Total idle dynamic CRDT scopes evicted");

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
            existing.TouchActivity();
            return existing;
        }

        if (scopesByCompositeId.TryGetValue(scopeOrReplicaId, out var existingComposite))
        {
            existingComposite.TouchActivity();
            return existingComposite;
        }

        var resolvedHost = ResolveHostReplicaId(scopeOrReplicaId, hostReplicaId);
        var normalizedScopeId = NormalizeScopeKey(scopeOrReplicaId, resolvedHost);

        var scope = activeScopes.GetOrAdd(normalizedScopeId, id =>
        {
            scopesCreatedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", id));
            var isPinned = registeredReplicaIds.Contains(id) && string.Equals(id, resolvedHost, StringComparison.Ordinal);
            return scopeFactory.CreateScope(id, resolvedHost, isPinned);
        });

        scopesByCompositeId.TryAdd(scope.ReplicaId, scope);
        scope.TouchActivity();
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

        if (scope is DistributedCrdtScope concreteScope)
        {
            await concreteScope.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
            return scope;
        }

        var scopeLock = fallbackInitLocks.GetOrAdd(scope.ReplicaId, _ => new SemaphoreSlim(1, 1));
        await scopeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (scope.Orchestrator.InitializationStage != OrchestratorInitializationStage.Initialized)
            {
                await scope.Orchestrator.InitializeAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            scopeLock.Release();
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
            scope.TouchActivity();
            return true;
        }

        if (scopesByCompositeId.TryGetValue(scopeOrReplicaId, out scope))
        {
            scope.TouchActivity();
            return true;
        }

        var atIdx = scopeOrReplicaId.IndexOf('@', StringComparison.Ordinal);
        if (atIdx > 0 && atIdx < scopeOrReplicaId.Length - 1)
        {
            var extractedScopeId = scopeOrReplicaId[(atIdx + 1)..];
            if (activeScopes.TryGetValue(extractedScopeId, out scope))
            {
                scope.TouchActivity();
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

    /// <summary>
    /// Flushes, checkpoints, removes, and disposes dynamic (non-pinned) CRDT scopes whose last activity timestamp is older than <paramref name="idleTtl"/>.
    /// </summary>
    /// <param name="idleTtl">The minimum idle duration before a dynamic scope is eligible for eviction.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The number of dynamic scopes evicted during this pass.</returns>
    public async ValueTask<int> EvictIdleDynamicScopesAsync(TimeSpan idleTtl, CancellationToken cancellationToken = default)
    {
        if (idleTtl <= TimeSpan.Zero)
        {
            return 0;
        }

        int evictedCount = 0;

        foreach (var kvp in activeScopes)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var scopeKey = kvp.Key;
            var scope = kvp.Value;

            if (scope.IsPinnedHostScope)
            {
                continue;
            }

            var lastActivity = scope.LastActivityTimestamp;
            if (Stopwatch.GetElapsedTime(lastActivity) < idleTtl)
            {
                continue;
            }

            if (scope.Orchestrator is CrdtDocumentOrchestrator concreteOrchestrator &&
                (concreteOrchestrator.PendingCommandCount > 0 || concreteOrchestrator.IsAntiEntropyBusy))
            {
                continue;
            }

            var docPipeline = scope.ServiceProvider.GetService<IDocumentCommandPipeline>();
            if (docPipeline != null && docPipeline.PendingCommandCount > 0)
            {
                continue;
            }

            try
            {
                if (scope.Orchestrator.InitializationStage == OrchestratorInitializationStage.Initialized)
                {
                    await scope.Orchestrator.FlushAsync(cancellationToken).ConfigureAwait(false);

                    var documents = scope.Orchestrator.GetActiveDocuments();
                    for (int i = 0; i < documents.Count; i++)
                    {
                        await documents[i].CheckpointAsync(cancellationToken).ConfigureAwait(false);
                    }

                    var isMemoryStorage = scope.Storage is MemoryCrdtStorage
                        || (scope.Storage is BufferedDistributedCrdtStorageDecorator decorator && decorator.InnerStorage is MemoryCrdtStorage);

                    if (!isMemoryStorage)
                    {
                        var versionVectorManager = scope.ServiceProvider.GetService<IShardVersionVectorManager>();
                        if (versionVectorManager != null)
                        {
                            var safelyPersistedMap = versionVectorManager.GetFullSnapshot();
                            await scope.Storage.SaveShardVersionVectorMapAsync(scope.ReplicaId, safelyPersistedMap, cancellationToken).ConfigureAwait(false);
                        }

                        var exportedClusterState = scope.ClusterTracker.ExportState();
                        await scope.Storage.SaveClusterStateAsync(scope.ReplicaId, exportedClusterState, cancellationToken).ConfigureAwait(false);
                    }

                    if (scope.Storage is BufferedDistributedCrdtStorageDecorator bufferedStorage)
                    {
                        await bufferedStorage.DrainAsync(500, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch
            {
                continue;
            }

            // Re-verify that the scope remained idle while the asynchronous flush and checkpoint completed.
            if (Stopwatch.GetElapsedTime(scope.LastActivityTimestamp) < idleTtl)
            {
                continue;
            }

            if (!activeScopes.TryRemove(new KeyValuePair<string, IDistributedCrdtScope>(scopeKey, scope)))
            {
                continue;
            }

            scopesByCompositeId.TryRemove(new KeyValuePair<string, IDistributedCrdtScope>(scope.ReplicaId, scope));
            if (fallbackInitLocks.TryRemove(scope.ReplicaId, out var initSem))
            {
                initSem.Dispose();
            }

            scope.Dispose();
            scopesEvictedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
            evictedCount++;
        }

        return evictedCount;
    }

    public void Dispose()
    {
        foreach (var scope in activeScopes.Values)
        {
            scope.Dispose();
        }
        activeScopes.Clear();
        scopesByCompositeId.Clear();

        foreach (var sem in fallbackInitLocks.Values)
        {
            sem.Dispose();
        }
        fallbackInitLocks.Clear();

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