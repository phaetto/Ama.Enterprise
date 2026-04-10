namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Singleton provider that maintains the long-lived CRDT scope for the local replica.
/// Ensures that all components (UI loops, background services, incoming network handlers) 
/// interact with the exact same in-memory state and ReplicaContext.
/// </summary>
public sealed class DistributedCrdtScopeProvider : IDisposable
{
    private IServiceScope scope;
    private readonly object syncRoot = new();

    /// <summary>
    /// Gets the long-lived CRDT scope.
    /// </summary>
    public IServiceScope Scope 
    {
        get
        {
            lock (syncRoot)
            {
                return scope;
            }
        }
    }

    public DistributedCrdtScopeProvider(ICrdtScopeFactory crdtScopeFactory, IOptions<DistributedCrdtOptions> options)
    {
        if (crdtScopeFactory == null)
        {
            throw new ArgumentNullException(nameof(crdtScopeFactory));
        }

        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        scope = crdtScopeFactory.CreateScope(options.Value.ReplicaId);
    }

    /// <summary>
    /// Replaces the current active scope securely. Used inherently by the initialization routines 
    /// when restoring global states explicitly via the scope factory.
    /// </summary>
    public void ReplaceScope(IServiceScope newScope)
    {
        if (newScope == null) throw new ArgumentNullException(nameof(newScope));

        lock (syncRoot)
        {
            scope.Dispose();
            scope = newScope;
        }
    }

    public void Dispose()
    {
        lock (syncRoot)
        {
            scope?.Dispose();
        }
    }
}