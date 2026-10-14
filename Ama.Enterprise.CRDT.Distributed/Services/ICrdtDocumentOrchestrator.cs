namespace Ama.Enterprise.CRDT.Distributed.Services;

using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Centralized stateless gateway orchestrating multi-document runtime allocations, gracefully managing P2P CRDT topology routing while completely eliminating proxy memory allocations.
/// </summary>
public interface ICrdtDocumentOrchestrator
{
    /// <summary>
    /// Exposes the global decentralized map managing P2P CRDT topology logic across the cluster natively.
    /// </summary>
    IDistributedCrdtDocument<CrdtRegistryState> Registry { get; }

    /// <summary>
    /// Bootstraps explicit overarching mapping bounds.
    /// </summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Iterates across the active strictly owned local document matrix.
    /// </summary>
    IReadOnlyList<IDistributedCrdtDocument> GetActiveDocuments();

    /// <summary>
    /// Retrieves a typed managed generic instance seamlessly yielding active native states if strictly locally mapped.
    /// </summary>
    IDistributedCrdtDocument<TState>? TryGetLocalDocument<TState>(string documentId) where TState : class, new();

    /// <summary>
    /// Retrieves the materialized document state efficiently routing across local memory channels or out-of-bound RPC network fetches implicitly.
    /// </summary>
    ValueTask<CrdtDocument<TState>> GetDocumentStateAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new();

    /// <summary>
    /// Forwards an intent directly bounding execution securely on the remote authoritative shard mapped across the decentralized mesh topology.
    /// </summary>
    Task<bool> ForwardIntentAsync<TState>(string documentId, IOperationIntent intent, string targetProperty, CancellationToken cancellationToken = default) where TState : class, new();

    /// <summary>
    /// Applies a locally generated patch intention resolving structural mutations traversing the local memory channel or remote proxy seamlessly.
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
    /// Processes localized matrix modifications dynamically promoting strictly assigned structural bounds.
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
    /// Dispatches a targeted point-to-point synchronization request mapping peer topologies extracting random active peers internally automatically.
    /// </summary>
    Task DispatchAntiEntropyStateAsync(CancellationToken cancellationToken = default);
}