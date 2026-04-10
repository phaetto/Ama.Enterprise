namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;

/// <summary>
/// Defines an optional persistence mechanism for distributed CRDT documents.
/// Implement this interface to load initial states and persist local changes over time into storage.
/// </summary>
/// <typeparam name="TState">The type of the application state handled by the document.</typeparam>
public interface IDistributedCrdtStorage<TState> where TState : class, new()
{
    /// <summary>
    /// Loads the stored CRDT document state including its Dotted Version Vectors and metadata.
    /// </summary>
    /// <param name="documentId">The globally unique identifier for this document type (Topic).</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The stored document, or null if no previous state exists.</returns>
    Task<CrdtDocument<TState>?> LoadAsync(string documentId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Persists the updated CRDT document state after patches or remote operations are applied.
    /// </summary>
    /// <param name="documentId">The globally unique identifier for this document type (Topic).</param>
    /// <param name="document">The full document containing the latest state and metadata tracking.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous save operation.</returns>
    Task SaveAsync(string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default);
}