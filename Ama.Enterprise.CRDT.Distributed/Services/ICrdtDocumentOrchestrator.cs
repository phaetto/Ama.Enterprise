namespace Ama.Enterprise.CRDT.Distributed.Services;

using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Centralized stateless gateway orchestrating multi-document runtime allocations and managing P2P CRDT topology routing while eliminating proxy memory allocations.
/// </summary>
/// <example>
/// <code>
/// await orchestrator.InitializeAsync(cancellationToken);
/// await orchestrator.CreateDocumentAsync("doc-1", nameof(MyState), cancellationToken);
/// var docState = await orchestrator.GetDocumentStateAsync&lt;MyState&gt;("doc-1", cancellationToken);
/// </code>
/// </example>
public interface ICrdtDocumentOrchestrator
{
    /// <summary>
    /// Exposes the global decentralized map managing P2P CRDT topology logic across the cluster.
    /// </summary>
    IDistributedCrdtDocument<CrdtRegistryState> Registry { get; }

    /// <summary>
    /// Bootstraps overarching mapping bounds.
    /// </summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Iterates across the active owned local document matrix.
    /// </summary>
    IReadOnlyList<IDistributedCrdtDocument> GetActiveDocuments();

    /// <summary>
    /// Retrieves a typed managed generic instance yielding active states if locally mapped.
    /// </summary>
    IDistributedCrdtDocument<TState>? TryGetLocalDocument<TState>(string documentId) where TState : class, new();

    /// <summary>
    /// Resolves the shard identifier for a document ID using the registry and cached mappings.
    /// </summary>
    ValueTask<string?> TryGetShardIdAsync(string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the materialized document state routing across local memory channels or out-of-bound RPC network fetches.
    /// </summary>
    ValueTask<CrdtDocument<TState>> GetDocumentStateAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new();

    /// <summary>
    /// Forwards an intent directly bounding execution on the remote authoritative shard mapped across the decentralized mesh topology.
    /// </summary>
    Task<bool> ApplyIntentAsync<TState>(string documentId, IOperationIntent intent, string targetProperty, CancellationToken cancellationToken = default) where TState : class, new();

    /// <summary>
    /// Forwards an intent directly bounding execution on the remote authoritative shard with the specified delivery mode.
    /// </summary>
    Task<bool> ApplyIntentAsync<TState>(string documentId, IOperationIntent intent, string targetProperty, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default) where TState : class, new();

    /// <summary>
    /// Applies a locally generated patch intention resolving structural mutations traversing the local memory channel or remote proxy.
    /// </summary>
    Task ApplyPatchAsync<TState>(string documentId, CrdtPatch patch, CancellationToken cancellationToken = default) where TState : class, new();

    /// <summary>
    /// Manually creates mapped distributed state models across the P2P network.
    /// </summary>
    Task CreateDocumentAsync(string documentId, string typeAlias, CancellationToken cancellationToken = default);

    /// <summary>
    /// Issues a permanent cluster tombstone.
    /// </summary>
    Task DeleteDocumentAsync(string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Processes localized matrix modifications dynamically promoting assigned structural bounds.
    /// </summary>
    Task SyncDocumentsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Manually triggers a re-evaluation of the document topology, promoting or demoting dynamically based on the current cluster map.
    /// </summary>
    Task ReevaluateTopologyAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Serializes the current local state and dispatches it directly as a targeted snapshot following an unrecoverable journal truncation gap.
    /// </summary>
    Task ProvideSnapshotAsync(string documentId, string targetReplicaId, PeerId targetPeerId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Dispatches a targeted point-to-point synchronization request mapping peer topologies extracting random active peers internally.
    /// </summary>
    Task DispatchAntiEntropyStateAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Dispatches a targeted point-to-point synchronization request directly to a specific peer.
    /// </summary>
    Task DispatchAntiEntropyStateAsync(PeerId targetPeerId, CancellationToken cancellationToken = default);
}