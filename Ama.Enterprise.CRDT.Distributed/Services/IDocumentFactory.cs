namespace Ama.Enterprise.CRDT.Distributed.Services;

using Ama.CRDT.Services.Pooling;
using Ama.CRDT.Services.Providers;
using Microsoft.Extensions.DependencyInjection;
using System;

/// <summary>
/// AOT-friendly generic factory interface for dynamically resolving distributed CRDT instances.
/// </summary>
/// <example>
/// <code>
/// var shardId = factory.GetShardId(serviceProvider, "doc-1");
/// var doc = factory.CreateDocument(serviceProvider, "doc-1");
/// </code>
/// </example>
public interface IDocumentFactory
{
    /// <summary>
    /// Gets the registered string alias for the document state type.
    /// </summary>
    string TypeAlias { get; }

    /// <summary>
    /// Gets the CLR type of the document state.
    /// </summary>
    Type StateType { get; }

    /// <summary>
    /// Resolves the shard identifier for a document without allocating or initializing the full document instance.
    /// </summary>
    /// <param name="serviceProvider">The scoped service provider.</param>
    /// <param name="documentId">The unique document identifier.</param>
    /// <returns>The resolved shard identifier.</returns>
    string GetShardId(IServiceProvider serviceProvider, string documentId);

    /// <summary>
    /// Creates an initialized distributed document.
    /// </summary>
    /// <param name="serviceProvider">The scoped service provider.</param>
    /// <param name="documentId">The unique document identifier.</param>
    /// <returns>The created distributed CRDT document instance.</returns>
    IDistributedCrdtDocument CreateDocument(IServiceProvider serviceProvider, string documentId);
}

/// <summary>
/// Strictly typed internal implementation for generating specific document generics.
/// </summary>
internal sealed class DocumentFactory<TState> : IDocumentFactory where TState : class, new()
{
    public DocumentFactory(string typeAlias)
    {
        if (string.IsNullOrWhiteSpace(typeAlias)) throw new ArgumentException("Type alias cannot be null or empty.", nameof(typeAlias));
        TypeAlias = CrdtStringPool.Shared.GetOrAdd(typeAlias);
    }

    /// <inheritdoc />
    public string TypeAlias { get; }

    /// <inheritdoc />
    public Type StateType => typeof(TState);

    /// <inheritdoc />
    public string GetShardId(IServiceProvider serviceProvider, string documentId)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        documentId = CrdtStringPool.Shared.GetOrAdd(documentId);
        var shardIdProvider = serviceProvider.GetRequiredService<IShardIdProvider>();
        var shardId = shardIdProvider.GetShardId<TState>(documentId) ?? documentId;
        return CrdtStringPool.Shared.GetOrAdd(shardId);
    }

    /// <inheritdoc />
    public IDistributedCrdtDocument CreateDocument(IServiceProvider serviceProvider, string documentId)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        documentId = CrdtStringPool.Shared.GetOrAdd(documentId);
        var documentIdProvider = serviceProvider.GetRequiredService<IDocumentIdProvider>();
        var state = documentIdProvider.CreateDocumentWithId<TState>(documentId);
        
        return ActivatorUtilities.CreateInstance<DistributedCrdtDocument<TState>>(serviceProvider, state, TypeAlias);
    }
}