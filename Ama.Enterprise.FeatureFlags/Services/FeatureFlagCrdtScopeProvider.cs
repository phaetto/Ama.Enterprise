namespace Ama.Enterprise.FeatureFlags.Services;

using System;
using Ama.CRDT.Services;
using Ama.Enterprise.FeatureFlags.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

/// <summary>
/// Singleton provider that maintains the long-lived CRDT scope for the local replica.
/// Ensures that all components (UI loops, background services, incoming network handlers) 
/// interact with the exact same in-memory state and ReplicaContext.
/// </summary>
public sealed class FeatureFlagCrdtScopeProvider : IDisposable
{
    /// <summary>
    /// Gets the long-lived CRDT scope.
    /// </summary>
    public IServiceScope Scope { get; }

    public FeatureFlagCrdtScopeProvider(ICrdtScopeFactory crdtScopeFactory, IOptions<FeatureFlagOptions> options)
    {
        if (crdtScopeFactory == null)
        {
            throw new ArgumentNullException(nameof(crdtScopeFactory));
        }

        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        Scope = crdtScopeFactory.CreateScope(options.Value.ReplicaId);
    }

    public void Dispose()
    {
        Scope.Dispose();
    }
}