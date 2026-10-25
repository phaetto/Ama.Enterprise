namespace Ama.Enterprise.CRDT.Distributed.Services.Observers;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Scoped observer attached to a source CRDT replica that forwards live document mutations, registry updates, and topology shifts to matching <see cref="ICrdtMigrationBridge"/> instances.
/// </summary>
/// <typeparam name="TSourceState">The source document state type being observed.</typeparam>
public sealed class MigrationSourceStateObserver<TSourceState> :
    IDocumentStateObserver<TSourceState>,
    ICrdtOrchestratorObserver
    where TSourceState : class, new()
{
    private readonly string currentReplicaId;
    private readonly IServiceProvider serviceProvider;
    private readonly RegistryStateObserverAdapter registryObserver;
    private IReadOnlyList<ICrdtMigrationBridge>? matchingBridges;

    /// <summary>
    /// Gets the observer implementation for the internal <see cref="CrdtRegistryState"/> document, avoiding CS0695 generic interface unification when <typeparamref name="TSourceState"/> is unconstrained.
    /// </summary>
    public RegistryStateObserverAdapter RegistryObserver => registryObserver;

    public MigrationSourceStateObserver(
        ReplicaContext replicaContext,
        IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(replicaContext);
        ArgumentNullException.ThrowIfNull(serviceProvider);

        currentReplicaId = replicaContext.ReplicaId;
        this.serviceProvider = serviceProvider;
        registryObserver = new RegistryStateObserverAdapter(this);
    }

    /// <inheritdoc />
    public Task OnStateChangedAsync(string documentId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId))
        {
            return Task.CompletedTask;
        }

        var bridges = GetMatchingBridges();
        if (bridges.Count == 0)
        {
            return Task.CompletedTask;
        }

        for (int i = 0; i < bridges.Count; i++)
        {
            bridges[i].EnqueueDocumentChange(documentId);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task OnTopologyChangedAsync(CancellationToken cancellationToken = default)
    {
        var bridges = GetMatchingBridges();
        for (int i = 0; i < bridges.Count; i++)
        {
            bridges[i].NotifyTopologyOrRegistryChanged();
        }

        return Task.CompletedTask;
    }

    private IReadOnlyList<ICrdtMigrationBridge> GetMatchingBridges()
    {
        var resolved = Volatile.Read(ref matchingBridges);
        if (resolved is not null)
        {
            return resolved;
        }

        var bridges = serviceProvider
            .GetServices<ICrdtMigrationBridge>()
            .Where(b => string.Equals(b.Options.SourceReplicaId, currentReplicaId, StringComparison.Ordinal))
            .ToList();

        Interlocked.CompareExchange(ref matchingBridges, bridges, null);
        return Volatile.Read(ref matchingBridges)!;
    }

    /// <summary>
    /// Adapter forwarding <see cref="CrdtRegistryState"/> state change notifications to the parent <see cref="MigrationSourceStateObserver{TSourceState}"/>.
    /// </summary>
    public sealed class RegistryStateObserverAdapter(MigrationSourceStateObserver<TSourceState> parent) : IDocumentStateObserver<CrdtRegistryState>
    {
        /// <inheritdoc />
        public Task OnStateChangedAsync(string documentId, CancellationToken cancellationToken = default)
        {
            return parent.OnTopologyChangedAsync(cancellationToken);
        }
    }
}