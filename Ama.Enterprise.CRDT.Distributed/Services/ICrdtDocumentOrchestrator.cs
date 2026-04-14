namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Generic manager facilitating multi-document runtime allocations.
/// </summary>
public interface ICrdtDocumentOrchestrator
{
    /// <summary>
    /// Fired asynchronously when active document registries change.
    /// </summary>
    event EventHandler? DocumentsChanged;

    /// <summary>
    /// Exposes the global decentralized map managing P2P CRDT topology logic across the cluster.
    /// </summary>
    IDistributedCrdtDocument<CrdtRegistryState> Registry { get; }

    /// <summary>
    /// Bootstraps explicit overarching mapping bounds.
    /// </summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Iterates across the active document matrix.
    /// </summary>
    IReadOnlyList<IDistributedCrdtDocument> GetActiveDocuments();

    /// <summary>
    /// Retrieves a typed managed generic instance.
    /// </summary>
    IDistributedCrdtDocument<TState>? GetDocument<TState>(string documentId) where TState : class, IDistributedCrdtState, new();

    /// <summary>
    /// Manually creates mapped distributed state models across the P2P network.
    /// </summary>
    Task CreateDocumentAsync(string documentId, string typeAlias, CancellationToken cancellationToken = default);

    /// <summary>
    /// Issues a permanent cluster tombstone.
    /// </summary>
    Task DeleteDocumentAsync(string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Processes localized matrix modifications.
    /// </summary>
    Task SyncDocumentsAsync(CancellationToken cancellationToken = default);
}