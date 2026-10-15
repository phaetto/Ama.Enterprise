namespace Ama.Enterprise.CRDT.Distributed.Services;

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
    /// <inheritdoc />
    public string GetShardId(IServiceProvider serviceProvider, string documentId)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        var shardIdProvider = serviceProvider.GetRequiredService<IShardIdProvider>();
        return shardIdProvider.GetShardId<TState>(documentId) ?? documentId;
    }

    /// <inheritdoc />
    public IDistributedCrdtDocument CreateDocument(IServiceProvider serviceProvider, string documentId)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        var documentIdProvider = serviceProvider.GetRequiredService<IDocumentIdProvider>();
        var state = documentIdProvider.CreateDocumentWithId<TState>(documentId);
        
        return ActivatorUtilities.CreateInstance<DistributedCrdtDocument<TState>>(serviceProvider, state);
    }
}