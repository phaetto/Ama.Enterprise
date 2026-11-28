namespace Ama.Enterprise.CRDT.Distributed.Services.Documents;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;

/// <summary>
/// Internal factory interface for instantiating generic <see cref="IDistributedCrdtDocument"/> instances without reflection.
/// </summary>
/// <example>
/// <code>
/// public sealed class CustomDocumentFactory : IDocumentFactory
/// {
///     public string TypeAlias => "custom-doc";
///     public Type StateType => typeof(CustomState);
///     // Implementation details...
/// }
/// </code>
/// </example>
public interface IDocumentFactory
{
    /// <summary>
    /// Gets the unique type alias for the document state.
    /// </summary>
    string TypeAlias { get; }

    /// <summary>
    /// Gets the underlying CLR type of the document state.
    /// </summary>
    Type StateType { get; }

    /// <summary>
    /// Resolves the shard identifier for the specified document identifier.
    /// </summary>
    /// <param name="serviceProvider">The scoped service provider.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <returns>The resolved shard identifier.</returns>
    string GetShardId(IServiceProvider serviceProvider, string documentId);

    /// <summary>
    /// Creates a new distributed CRDT document instance for the specified document identifier.
    /// </summary>
    /// <param name="serviceProvider">The scoped service provider.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <returns>The created distributed CRDT document.</returns>
    IDistributedCrdtDocument CreateDocument(IServiceProvider serviceProvider, string documentId);

    /// <summary>
    /// Creates a new distributed CRDT document instance for a brand-new locally created document, skipping redundant initial storage load queries.
    /// </summary>
    /// <param name="serviceProvider">The scoped service provider.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <returns>The created distributed CRDT document marked as initialized.</returns>
    IDistributedCrdtDocument CreateNewlyCreatedLocalDocument(IServiceProvider serviceProvider, string documentId)
        => CreateDocument(serviceProvider, documentId);

    /// <summary>
    /// Attempts to load a document directly from storage and serialize it into a snapshot payload without promoting it into active memory.
    /// </summary>
    /// <param name="serviceProvider">The scoped service provider.</param>
    /// <param name="shardId">The target shard identifier.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The serialized <see cref="CrdtDocument{TState}"/> byte array, or an empty array if not found in storage.</returns>
    ValueTask<byte[]> TryLoadSerializedDocumentFromStorageAsync(
        IServiceProvider serviceProvider,
        string shardId,
        string documentId,
        CancellationToken cancellationToken = default);
}
