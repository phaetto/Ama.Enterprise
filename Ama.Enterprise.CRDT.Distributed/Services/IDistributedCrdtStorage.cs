namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Journaling;

/// <summary>
/// Defines a unified persistence and journaling mechanism for distributed CRDT documents.
/// Combines document state, global DVV, and operation journaling to prevent gaps in reconstruction.
/// </summary>
public interface IDistributedCrdtStorage : ICrdtOperationJournal
{
    /// <summary>
    /// Loads the globally tracked version vector representing the entire replica context state.
    /// </summary>
    /// <param name="replicaId">The active node replica ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The stored DottedVersionVector, or null if it doesn't exist.</returns>
    Task<DottedVersionVector?> LoadGlobalVersionVectorAsync(string replicaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Persists the global replica version vector strictly after operations map appropriately across the bounds.
    /// </summary>
    /// <param name="replicaId">The active node replica ID.</param>
    /// <param name="globalVersionVector">The newly updated complete DottedVersionVector.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous save operation.</returns>
    Task SaveGlobalVersionVectorAsync(string replicaId, DottedVersionVector globalVersionVector, CancellationToken cancellationToken = default);

    /// <summary>
    /// Loads the stored CRDT document state including its Dotted Version Vectors and metadata.
    /// </summary>
    /// <typeparam name="TState">The application state bounded by the document.</typeparam>
    /// <param name="documentId">The globally unique identifier for this document type (Topic).</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The stored document, or null if no previous state exists.</returns>
    Task<CrdtDocument<TState>?> LoadDocumentAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new();

    /// <summary>
    /// Persists the updated CRDT document state after patches or remote operations are applied.
    /// </summary>
    /// <typeparam name="TState">The application state bounded by the document.</typeparam>
    /// <param name="documentId">The globally unique identifier for this document type (Topic).</param>
    /// <param name="document">The full document containing the latest state and metadata tracking.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous save operation.</returns>
    Task SaveDocumentAsync<TState>(string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default) where TState : class, new();

    /// <summary>
    /// Asynchronously trims the underlying operation journal securely to remove obsolete operations strictly mapped across the background synchronized version vector bounds.
    /// </summary>
    /// <param name="globalMinimumVersionVector">The mapped version vector boundaries resolving safe historic patch removal limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous trim operation.</returns>
    Task TrimAsync(IReadOnlyDictionary<string, long> globalMinimumVersionVector, CancellationToken cancellationToken = default);
}