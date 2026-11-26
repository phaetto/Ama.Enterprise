namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Pooling;
using Ama.CRDT.Services.Providers;
using Ama.CRDT.Services.Serialization;
using Ama.CRDT.Services.Versioning;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Internal factory interface enabling AOT-friendly document creation for registered types.
/// </summary>
/// <example>
/// <code>
/// var doc = factory.CreateDocument(serviceProvider, "doc-1");
/// </code>
/// </example>
public interface IDocumentFactory
{
    /// <summary>
    /// Gets the type alias that identifies the document state type.
    /// </summary>
    string TypeAlias { get; }

    /// <summary>
    /// Gets the CLR type of the underlying CRDT document state.
    /// </summary>
    Type StateType { get; }

    /// <summary>
    /// Creates an initialized distributed CRDT document instance for the specified document identifier.
    /// </summary>
    /// <param name="serviceProvider">The scoped service provider.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <returns>The created distributed CRDT document instance.</returns>
    IDistributedCrdtDocument CreateDocument(IServiceProvider serviceProvider, string documentId);

    /// <summary>
    /// Creates a newly created local distributed CRDT document instance without scheduling a storage load.
    /// </summary>
    /// <param name="serviceProvider">The scoped service provider.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <returns>The created distributed CRDT document instance.</returns>
    IDistributedCrdtDocument CreateNewlyCreatedLocalDocument(IServiceProvider serviceProvider, string documentId)
        => CreateDocument(serviceProvider, documentId);

    /// <summary>
    /// Computes the shard identifier for the specified document identifier without instantiating the full document pipeline.
    /// </summary>
    /// <param name="serviceProvider">The scoped service provider.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <returns>The resolved shard identifier.</returns>
    string GetShardId(IServiceProvider serviceProvider, string documentId);

    /// <summary>
    /// Attempts to load a persisted document from storage and returns its serialized snapshot bytes without promoting it into active memory.
    /// </summary>
    /// <param name="serviceProvider">The scoped service provider.</param>
    /// <param name="shardId">The shard identifier.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The serialized snapshot bytes if found in storage; otherwise an empty byte array.</returns>
    ValueTask<byte[]> TryLoadSerializedDocumentFromStorageAsync(
        IServiceProvider serviceProvider,
        string shardId,
        string documentId,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Generic implementation of <see cref="IDocumentFactory"/> for a specific <typeparamref name="TState"/>.
/// </summary>
/// <typeparam name="TState">The CRDT state model type.</typeparam>
public sealed class DocumentFactory<TState> : IDocumentFactory where TState : class, new()
{
    /// <inheritdoc />
    public string TypeAlias { get; }

    /// <inheritdoc />
    public Type StateType => typeof(TState);

    /// <summary>
    /// Initializes a new instance of the <see cref="DocumentFactory{TState}"/> class.
    /// </summary>
    /// <param name="typeAlias">The string alias mapped to <typeparamref name="TState"/>.</param>
    public DocumentFactory(string typeAlias)
    {
        if (string.IsNullOrWhiteSpace(typeAlias)) throw new ArgumentException("Type alias cannot be null or empty.", nameof(typeAlias));
        TypeAlias = CrdtStringPool.Shared.GetOrAdd(typeAlias);
    }

    /// <inheritdoc />
    public IDistributedCrdtDocument CreateDocument(IServiceProvider serviceProvider, string documentId)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        var stringPool = serviceProvider.GetService<ICrdtStringPool>() ?? CrdtStringPool.Shared;
        documentId = stringPool.GetOrAdd(documentId);

        var idProvider = serviceProvider.GetRequiredService<IDocumentIdProvider>();
        var state = idProvider.CreateDocumentWithId<TState>(documentId);
        return CreateDocumentInstance(serviceProvider, state, TypeAlias, stringPool);
    }

    /// <inheritdoc />
    public IDistributedCrdtDocument CreateNewlyCreatedLocalDocument(IServiceProvider serviceProvider, string documentId)
    {
        var doc = (DistributedCrdtDocument<TState>)CreateDocument(serviceProvider, documentId);
        doc.MarkAsNewlyCreatedLocalDocument();
        return doc;
    }

    /// <summary>
    /// Directly constructs a <see cref="DistributedCrdtDocument{TState}"/> using pre-registered services without reflection or <see cref="ActivatorUtilities"/> overhead.
    /// </summary>
    internal static DistributedCrdtDocument<TState> CreateDocumentInstance(
        IServiceProvider serviceProvider,
        TState state,
        string? typeAlias = null,
        ICrdtStringPool? stringPool = null)
    {
        return new DistributedCrdtDocument<TState>(
            state,
            serviceProvider,
            serviceProvider.GetRequiredService<IShardVersionVectorManager>(),
            serviceProvider.GetRequiredService<IAsyncCrdtApplicator>(),
            serviceProvider.GetRequiredService<ICrdtApplicator>(),
            serviceProvider.GetRequiredService<IAsyncCrdtMerger>(),
            serviceProvider.GetRequiredService<ICrdtMetadataManager>(),
            serviceProvider.GetRequiredService<IOptions<DistributedCrdtOptions>>(),
            serviceProvider.GetRequiredService<ICrdtSerializer>(),
            serviceProvider.GetRequiredService<IDistributedCrdtStorage>(),
            serviceProvider.GetRequiredService<ILogger<DistributedCrdtDocument<TState>>>(),
            serviceProvider.GetRequiredService<IDocumentIdProvider>(),
            serviceProvider.GetRequiredService<IShardIdProvider>(),
            serviceProvider.GetRequiredService<IShardDistributionManager>(),
            serviceProvider.GetRequiredService<IDocumentCommandPipeline>(),
            serviceProvider.GetRequiredService<IDistributedCrdtTelemetry>(),
            serviceProvider.GetServices<IDocumentStateObserver<TState>>(),
            serviceProvider.GetServices<IDocumentPatchObserver>(),
            typeAlias,
            stringPool);
    }

    /// <inheritdoc />
    public string GetShardId(IServiceProvider serviceProvider, string documentId)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        var stringPool = serviceProvider.GetService<ICrdtStringPool>() ?? CrdtStringPool.Shared;
        documentId = stringPool.GetOrAdd(documentId);

        if (typeof(TState) == typeof(CrdtRegistryState))
        {
            return documentId;
        }

        var shardIdProvider = serviceProvider.GetRequiredService<IShardIdProvider>();
        return stringPool.GetOrAdd(shardIdProvider.GetShardId<TState>(documentId) ?? string.Empty);
    }

    /// <inheritdoc />
    public async ValueTask<byte[]> TryLoadSerializedDocumentFromStorageAsync(
        IServiceProvider serviceProvider,
        string shardId,
        string documentId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        if (string.IsNullOrWhiteSpace(shardId) || string.IsNullOrWhiteSpace(documentId))
        {
            return Array.Empty<byte>();
        }

        var storage = serviceProvider.GetRequiredService<IDistributedCrdtStorage>();
        var compositeKey = typeof(TState) == typeof(CrdtRegistryState)
            ? documentId
            : CrdtRegistryEntry.GetRegistryKey(TypeAlias, documentId);

        var loaded = await storage.LoadDocumentAsync<TState>(shardId, compositeKey, cancellationToken).ConfigureAwait(false);
        if (loaded == null && !string.Equals(compositeKey, documentId, StringComparison.Ordinal))
        {
            loaded = await storage.LoadDocumentAsync<TState>(shardId, documentId, cancellationToken).ConfigureAwait(false);
        }

        if (loaded == null)
        {
            loaded = await storage.LoadDocumentFromAnyShardAsync<TState>(compositeKey, cancellationToken).ConfigureAwait(false);
            if (loaded == null && !string.Equals(compositeKey, documentId, StringComparison.Ordinal))
            {
                loaded = await storage.LoadDocumentFromAnyShardAsync<TState>(documentId, cancellationToken).ConfigureAwait(false);
            }
        }

        if (loaded == null)
        {
            loaded = await storage.LoadOrphanedDocumentAsync<TState>(shardId, compositeKey, cancellationToken).ConfigureAwait(false);
            if (loaded == null && !string.Equals(compositeKey, documentId, StringComparison.Ordinal))
            {
                loaded = await storage.LoadOrphanedDocumentAsync<TState>(shardId, documentId, cancellationToken).ConfigureAwait(false);
            }
        }

        if (loaded == null)
        {
            return Array.Empty<byte>();
        }

        var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();
        return serializer.SerializeToBytes(loaded.Value);
    }
}