namespace Ama.Enterprise.CRDT.Distributed.Services.Scopes;

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Pooling;
using Ama.CRDT.Services.Providers;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services.Documents;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.CRDT.Distributed.Services.Sharding;
using Ama.Enterprise.CRDT.Distributed.Services.Storage;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Default implementation of <see cref="IDistributedCrdtScopeFactory"/> using <see cref="ICrdtScopeFactory"/>
/// to spawn isolated dependency injection scopes per replica and dynamic scope identifier.
/// </summary>
public sealed class DistributedCrdtScopeFactory : IDistributedCrdtScopeFactory
{
    private readonly IServiceProvider rootProvider;
    private readonly ICrdtScopeFactory crdtScopeFactory;
    private readonly ICrdtStringPool stringPool;

    public DistributedCrdtScopeFactory(
        IServiceProvider rootProvider,
        ICrdtScopeFactory crdtScopeFactory,
        ICrdtStringPool? stringPool = null)
    {
        this.rootProvider = rootProvider ?? throw new ArgumentNullException(nameof(rootProvider));
        this.crdtScopeFactory = crdtScopeFactory ?? throw new ArgumentNullException(nameof(crdtScopeFactory));
        this.stringPool = stringPool ?? CrdtStringPool.Shared;
    }

    /// <inheritdoc />
    public IDistributedCrdtScope CreateScope(string replicaId)
    {
        if (string.IsNullOrWhiteSpace(replicaId))
        {
            throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));
        }

        replicaId = stringPool.GetOrAdd(replicaId);
        return CreateScope(replicaId, replicaId, isPinnedHostScope: true);
    }

    /// <inheritdoc />
    public IDistributedCrdtScope CreateScope(string scopeId, string hostReplicaId, bool isPinnedHostScope = false)
    {
        if (string.IsNullOrWhiteSpace(scopeId))
        {
            throw new ArgumentException("Scope ID cannot be null or empty.", nameof(scopeId));
        }

        if (string.IsNullOrWhiteSpace(hostReplicaId))
        {
            throw new ArgumentException("Host replica ID cannot be null or empty.", nameof(hostReplicaId));
        }

        scopeId = stringPool.GetOrAdd(scopeId);
        hostReplicaId = stringPool.GetOrAdd(hostReplicaId);

        var compositeReplicaId = string.Equals(scopeId, hostReplicaId, StringComparison.Ordinal)
            ? hostReplicaId
            : stringPool.GetOrAdd($"{hostReplicaId}@{scopeId}");

        var serviceScope = crdtScopeFactory.CreateScope(compositeReplicaId);
        try
        {
            var sp = serviceScope.ServiceProvider;

            var scopeContext = sp.GetRequiredService<CrdtScopeContext>();
            scopeContext.ScopeId = scopeId;
            scopeContext.HostReplicaId = hostReplicaId;
            scopeContext.ReplicaId = compositeReplicaId;

            DecorateScopeSystemRegistryProviders(sp);

            var storage = sp.GetRequiredService<IDistributedCrdtStorage>();
            var clusterTracker = sp.GetRequiredService<IClusterStateTracker>();
            var evictionService = sp.GetRequiredService<ICrdtEvictionService>();
            var orchestrator = sp.GetRequiredService<ICrdtDocumentOrchestrator>();

            // Force early instantiation of observers so they can register and receive callbacks within this scope
            _ = sp.GetServices<IClusterStateObserver>();
            _ = sp.GetServices<ICrdtOrchestratorObserver>();
            _ = sp.GetServices<IDocumentPatchObserver>();

            return new DistributedCrdtScope(
                scopeId,
                hostReplicaId,
                compositeReplicaId,
                isPinnedHostScope,
                serviceScope,
                storage,
                clusterTracker,
                evictionService,
                orchestrator);
        }
        catch
        {
            serviceScope.Dispose();
            throw;
        }
    }

    private static void DecorateScopeSystemRegistryProviders(IServiceProvider sp)
    {
        var shardIdProvider = sp.GetService<IShardIdProvider>();
        if (shardIdProvider is SystemRegistryShardIdProviderDecorator shardDecorator)
        {
            shardDecorator.EnsureInnerResolved(sp);
        }

        var docIdProvider = sp.GetService<IDocumentIdProvider>();
        if (docIdProvider is SystemRegistryDocumentIdProviderDecorator docDecorator)
        {
            docDecorator.EnsureInnerResolved(sp);
        }
    }

    internal sealed class DistributedCrdtScope : IDistributedCrdtScope, IAsyncDisposable
    {
        private readonly IServiceScope serviceScope;
        private readonly SemaphoreSlim initializationLock = new(1, 1);
        private long lastActivityTimestamp;
        private int disposed;

        public string ScopeId { get; }

        public string HostReplicaId { get; }

        public string ReplicaId { get; }

        public bool IsPinnedHostScope { get; }

        public long LastActivityTimestamp => Volatile.Read(ref lastActivityTimestamp);

        public IServiceProvider ServiceProvider => serviceScope.ServiceProvider;

        public IDistributedCrdtStorage Storage { get; }

        public IClusterStateTracker ClusterTracker { get; }

        public ICrdtEvictionService EvictionService { get; }

        public ICrdtDocumentOrchestrator Orchestrator { get; }

        public DistributedCrdtScope(
            string scopeId,
            string hostReplicaId,
            string replicaId,
            bool isPinnedHostScope,
            IServiceScope serviceScope,
            IDistributedCrdtStorage storage,
            IClusterStateTracker clusterTracker,
            ICrdtEvictionService evictionService,
            ICrdtDocumentOrchestrator orchestrator)
        {
            ScopeId = scopeId;
            HostReplicaId = hostReplicaId;
            ReplicaId = replicaId;
            IsPinnedHostScope = isPinnedHostScope;
            this.serviceScope = serviceScope;
            Storage = storage;
            ClusterTracker = clusterTracker;
            EvictionService = evictionService;
            Orchestrator = orchestrator;
            lastActivityTimestamp = Stopwatch.GetTimestamp();
        }

        public void TouchActivity()
        {
            Volatile.Write(ref lastActivityTimestamp, Stopwatch.GetTimestamp());
        }

        public async ValueTask EnsureInitializedAsync(CancellationToken cancellationToken = default)
        {
            TouchActivity();

            if (Orchestrator.InitializationStage == OrchestratorInitializationStage.Initialized)
            {
                return;
            }

            if (Orchestrator is CrdtDocumentOrchestrator concreteOrchestrator &&
                concreteOrchestrator.IsCurrentThreadOrchestratorProcessor)
            {
                await Orchestrator.InitializeAsync(cancellationToken).ConfigureAwait(false);
                return;
            }

            await initializationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (Orchestrator.InitializationStage == OrchestratorInitializationStage.Initialized)
                {
                    return;
                }

                await Orchestrator.InitializeAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                initializationLock.Release();
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
            {
                return;
            }

            DisposePatchObservers();
            serviceScope.Dispose();
            initializationLock.Dispose();
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
            {
                return;
            }

            DisposePatchObservers();

            if (serviceScope is IAsyncDisposable asyncDisposableScope)
            {
                await asyncDisposableScope.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                serviceScope.Dispose();
            }

            initializationLock.Dispose();
        }

        private void DisposePatchObservers()
        {
            try
            {
                var observers = serviceScope.ServiceProvider.GetServices<IDocumentPatchObserver>();
                foreach (var observer in observers)
                {
                    if (observer is IDisposable disposable)
                    {
                        try
                        {
                            disposable.Dispose();
                        }
                        catch (ObjectDisposedException)
                        {
                        }
                    }
                }
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }
}