namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Coordinates anti-entropy workload evaluation, shard concurrency locks, topology announcements, and outbound snapshot dispatches.
/// </summary>
/// <example>
/// <code>
/// var antiEntropy = serviceProvider.GetRequiredService&lt;ICrdtAntiEntropyCoordinator&gt;();
/// var workload = await antiEntropy.EvaluateAndDispatchAntiEntropyStateAsync(orchestrator, cancellationToken);
/// </code>
/// </example>
public interface ICrdtAntiEntropyCoordinator
{
    /// <summary>
    /// Gets a value indicating whether an anti-entropy dispatch or incoming state sync response is currently active.
    /// </summary>
    bool IsAntiEntropyBusy { get; }

    /// <summary>
    /// Evaluates current pipeline queue depth and shard reception state, and dispatches an anti-entropy state sync if shards are available.
    /// </summary>
    /// <param name="orchestrator">The local document orchestrator.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The evaluated <see cref="CrdtAntiEntropyWorkLoadDto"/>.</returns>
    Task<CrdtAntiEntropyWorkLoadDto> EvaluateAndDispatchAntiEntropyStateAsync(
        ICrdtDocumentOrchestrator orchestrator,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Dispatches a full state sync message to a random active expected peer.
    /// </summary>
    /// <param name="orchestrator">The local document orchestrator.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DispatchAntiEntropyStateAsync(
        ICrdtDocumentOrchestrator orchestrator,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Dispatches a full state sync message to a specific target peer.
    /// </summary>
    /// <param name="orchestrator">The local document orchestrator.</param>
    /// <param name="targetPeerId">The target network peer identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DispatchAntiEntropyStateAsync(
        ICrdtDocumentOrchestrator orchestrator,
        PeerId targetPeerId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Dispatches a lightweight topology announcement and system registry state sync to a specific target peer.
    /// </summary>
    /// <param name="orchestrator">The local document orchestrator.</param>
    /// <param name="targetPeerId">The target network peer identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DispatchTopologyAnnouncementAsync(
        ICrdtDocumentOrchestrator orchestrator,
        PeerId targetPeerId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Serializes and sends a full document snapshot to the specified target peer.
    /// </summary>
    /// <param name="orchestrator">The local document orchestrator.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <param name="typeAlias">The optional document type alias.</param>
    /// <param name="targetReplicaId">The target replica identifier.</param>
    /// <param name="targetPeerId">The target network peer identifier.</param>
    /// <param name="shardDocumentIds">The optional batch list of expected document keys in the shard.</param>
    /// <param name="consistentGlobalState">The optional pre-captured shard version vector snapshot.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ProvideSnapshotAsync(
        ICrdtDocumentOrchestrator orchestrator,
        string documentId,
        string? typeAlias,
        string targetReplicaId,
        PeerId targetPeerId,
        IReadOnlyList<string>? shardDocumentIds = null,
        ShardVersionVectorMap? consistentGlobalState = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to acquire a global anti-entropy work scope if the node is not overloaded or already busy.
    /// </summary>
    /// <param name="orchestratorPendingCommandCount">The number of pending commands in the orchestrator channel.</param>
    /// <returns>An <see cref="IDisposable"/> releasing the work scope, or <see langword="null"/> if busy.</returns>
    IDisposable? TryAcquireAntiEntropyWorkScope(int orchestratorPendingCommandCount);

    /// <summary>
    /// Attempts to acquire a per-shard anti-entropy work scope if the shard and node queues are not busy.
    /// </summary>
    /// <param name="shardId">The shard identifier to lock.</param>
    /// <param name="orchestratorPendingCommandCount">The number of pending commands in the orchestrator channel.</param>
    /// <returns>An <see cref="IDisposable"/> releasing the shard work scope, or <see langword="null"/> if busy.</returns>
    IDisposable? TryAcquireAntiEntropyWorkScope(string shardId, int orchestratorPendingCommandCount);
}