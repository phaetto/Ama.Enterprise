namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;

/// <summary>
/// Represents a discrete, long-lived isolated scope for a specific CRDT replica or causal compartment.
/// </summary>
/// <example>
/// <code>
/// var scope = await scopeManager.GetOrCreateScopeAsync("eu-user", cancellationToken);
/// await scope.Orchestrator.CreateDocumentAsync("doc-1", "task-list", cancellationToken);
/// </code>
/// </example>
public interface IDistributedCrdtScope : IDisposable
{
    /// <summary>
    /// Gets the unique CRDT replica identifier for this isolated execution context.
    /// </summary>
    string ReplicaId { get; }

    /// <summary>
    /// Gets the logical scope or compartment identifier routed across P2P envelopes.
    /// </summary>
    string ScopeId { get; }

    /// <summary>
    /// Gets the physical host node replica identifier used to resolve P2P mesh bindings.
    /// </summary>
    string HostReplicaId { get; }

    /// <summary>
    /// Gets the document orchestrator scoped to this replica.
    /// </summary>
    ICrdtDocumentOrchestrator Orchestrator { get; }

    /// <summary>
    /// Gets the cluster state tracker scoped to this replica.
    /// </summary>
    IClusterStateTracker ClusterTracker { get; }

    /// <summary>
    /// Gets the unified storage mechanism isolated to this replica scope.
    /// </summary>
    IDistributedCrdtStorage Storage { get; }

    /// <summary>
    /// Gets the eviction handler configured for this replica scope.
    /// </summary>
    ICrdtEvictionService EvictionService { get; }

    /// <summary>
    /// Gets the inner scoped dependency injection service provider for this replica scope.
    /// </summary>
    IServiceProvider ServiceProvider { get; }
}