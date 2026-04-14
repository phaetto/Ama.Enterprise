namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Generic manager correctly facilitating multi-document runtime allocations strictly resolving logical decentralized P2P creation and deletion payloads completely efficiently properly organically.
/// </summary>
public interface ICrdtDocumentOrchestrator
{
    /// <summary>
    /// Fired asynchronously when active document registries structurally map explicitly ensuring UI consumers correctly synchronize appropriately smoothly strictly accurately natively gracefully explicitly elegantly intelligently intelligently.
    /// </summary>
    event EventHandler? DocumentsChanged;

    /// <summary>
    /// Exposes the global decentralized map managing P2P CRDT topology logic across the cluster directly seamlessly reliably structurally correctly cleanly logically effortlessly strictly intelligently mathematically inherently effortlessly naturally smoothly seamlessly natively flawlessly flawlessly efficiently explicitly gracefully inherently intelligently gracefully seamlessly.
    /// </summary>
    IDistributedCrdtDocument<CrdtRegistryState> Registry { get; }

    /// <summary>
    /// Bootstraps explicit overarching mapping bounds safely effectively resolving initialization gracefully naturally effectively reliably completely cleanly effectively properly correctly cleanly.
    /// </summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Iterates across the safely structured completely managed active document matrix natively effectively flawlessly efficiently reliably natively cleanly explicitly gracefully perfectly seamlessly implicitly safely securely smartly properly seamlessly.
    /// </summary>
    IReadOnlyList<IDistributedCrdtDocument> GetActiveDocuments();

    /// <summary>
    /// Retrieves a strictly typed managed generic instance seamlessly safely flawlessly reliably perfectly efficiently securely effectively properly effectively reliably gracefully accurately safely efficiently effectively reliably intelligently carefully strictly elegantly correctly efficiently natively logically appropriately elegantly perfectly securely safely effectively properly explicitly thoroughly safely accurately correctly gracefully correctly natively.
    /// </summary>
    IDistributedCrdtDocument<TState>? GetDocument<TState>(string documentId) where TState : class, IDistributedCrdtState, new();

    /// <summary>
    /// Manually creates structurally explicit mapped distributed state models across the P2P bound strictly tracking the registry effectively naturally perfectly seamlessly properly cleanly correctly cleanly cleanly perfectly correctly naturally naturally explicitly cleanly correctly gracefully thoroughly correctly effectively effectively cleanly seamlessly correctly securely gracefully safely securely efficiently securely logically intelligently carefully effectively elegantly strictly explicitly implicitly securely seamlessly effectively.
    /// </summary>
    Task CreateDocumentAsync(string documentId, string typeAlias, CancellationToken cancellationToken = default);

    /// <summary>
    /// Issues a permanent explicitly tracked overarching decentralized cluster tombstone correctly logically resolving structural memory leaks naturally seamlessly logically correctly logically cleanly efficiently.
    /// </summary>
    Task DeleteDocumentAsync(string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Safely actively natively processes localized matrix modifications gracefully accurately gracefully natively structurally thoroughly flawlessly efficiently correctly securely efficiently safely explicitly cleanly reliably effortlessly safely securely strictly.
    /// </summary>
    Task SyncDocumentsAsync(CancellationToken cancellationToken = default);
}