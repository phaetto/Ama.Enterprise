namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;

/// <summary>
/// Represents a discrete, long-lived isolated scope for a specific explicit CRDT replica decoupling structural tracking boundaries.
/// </summary>
public interface IDistributedCrdtScope : IDisposable
{
    /// <summary>
    /// The unique replica identifier matching this isolated execution context.
    /// </summary>
    string ReplicaId { get; }

    /// <summary>
    /// The document orchestrator scoped to this replica.
    /// </summary>
    ICrdtDocumentOrchestrator Orchestrator { get; }

    /// <summary>
    /// The cluster tracker scoped to this replica.
    /// </summary>
    IClusterStateTracker ClusterTracker { get; }

    /// <summary>
    /// The localized unified storage mechanism explicitly isolated matching this node.
    /// </summary>
    IDistributedCrdtStorage Storage { get; }

    /// <summary>
    /// The dedicated eviction handler configured correctly evaluating this target context.
    /// </summary>
    ICrdtEvictionService EvictionService { get; }

    /// <summary>
    /// Exposes the strictly bounded inner dependency container encapsulating replica dependencies.
    /// </summary>
    IServiceProvider ServiceProvider { get; }
}