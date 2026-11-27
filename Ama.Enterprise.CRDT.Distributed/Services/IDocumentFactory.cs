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
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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

/// <summary>
/// AOT-friendly generic implementation of <see cref="IDocumentFactory"/>.
/// </summary>
/// <typeparam name="TState">The CRDT document state type.</typeparam>
internal sealed class DocumentFactory<TState> : IDocumentFactory where TState : class, new()
{
    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<IServiceProvider, ScopedDocumentDependencies> ScopedCache = new();

    private readonly CrdtDocumentResidencyMode? residencyModeOverride;

    public string TypeAlias { get; }

    public Type StateType => typeof(TState);

    public DocumentFactory(string typeAlias, CrdtDocumentResidencyMode? residencyModeOverride = null)
    {
        if (string.IsNullOrWhiteSpace(typeAlias))
        {
            throw new ArgumentException("Type alias cannot be null or empty.", nameof(typeAlias));
        }

        TypeAlias = CrdtStringPool.Shared.GetOrAdd(typeAlias);
        this.residencyModeOverride = residencyModeOverride;
    }

    public string GetShardId(IServiceProvider serviceProvider, string documentId)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        if (string.IsNullOrWhiteSpace(documentId))
        {
            throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        }

        if (typeof(TState) == typeof(CrdtRegistryState) ||
            string.Equals(documentId, Constants.SystemRegistryDocumentId, StringComparison.Ordinal))
        {
            return Constants.SystemRegistryDocumentId;
        }

        var deps = GetOrCreateDependencies(serviceProvider);
        return deps.ShardIdProvider.GetShardId<TState>(documentId) ?? documentId;
    }

    public IDistributedCrdtDocument CreateDocument(IServiceProvider serviceProvider, string documentId)
    {
        return CreateTypedDocument(serviceProvider, documentId);
    }

    public IDistributedCrdtDocument CreateNewlyCreatedLocalDocument(IServiceProvider serviceProvider, string documentId)
    {
        var doc = CreateTypedDocument(serviceProvider, documentId);
        doc.MarkAsNewlyCreatedLocalDocument();
        return doc;
    }

    public async ValueTask<byte[]> TryLoadSerializedDocumentFromStorageAsync(
        IServiceProvider serviceProvider,
        string shardId,
        string documentId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        if (string.IsNullOrWhiteSpace(documentId))
        {
            return Array.Empty<byte>();
        }

        var deps = GetOrCreateDependencies(serviceProvider);
        var compositeKey = string.IsNullOrWhiteSpace(TypeAlias) || typeof(TState) == typeof(CrdtRegistryState)
            ? documentId
            : CrdtRegistryEntry.GetRegistryKey(TypeAlias, documentId);

        var rawBytes = await deps.Storage.LoadSerializedDocumentAsync(shardId, compositeKey, cancellationToken).ConfigureAwait(false);
        if ((rawBytes == null || rawBytes.Length == 0) && !string.Equals(compositeKey, documentId, StringComparison.Ordinal))
        {
            rawBytes = await deps.Storage.LoadSerializedDocumentAsync(shardId, documentId, cancellationToken).ConfigureAwait(false);
        }

        if (rawBytes == null || rawBytes.Length == 0)
        {
            rawBytes = await deps.Storage.LoadSerializedDocumentFromAnyShardAsync(compositeKey, cancellationToken).ConfigureAwait(false);
            if ((rawBytes == null || rawBytes.Length == 0) && !string.Equals(compositeKey, documentId, StringComparison.Ordinal))
            {
                rawBytes = await deps.Storage.LoadSerializedDocumentFromAnyShardAsync(documentId, cancellationToken).ConfigureAwait(false);
            }
        }

        if (rawBytes is { Length: > 0 })
        {
            return rawBytes;
        }

        var loaded = await deps.Storage.LoadDocumentAsync<TState>(shardId, compositeKey, cancellationToken).ConfigureAwait(false);
        if (loaded == null && !string.Equals(compositeKey, documentId, StringComparison.Ordinal))
        {
            loaded = await deps.Storage.LoadDocumentAsync<TState>(shardId, documentId, cancellationToken).ConfigureAwait(false);
        }

        if (loaded == null)
        {
            loaded = await deps.Storage.LoadDocumentFromAnyShardAsync<TState>(compositeKey, cancellationToken).ConfigureAwait(false);
            if (loaded == null && !string.Equals(compositeKey, documentId, StringComparison.Ordinal))
            {
                loaded = await deps.Storage.LoadDocumentFromAnyShardAsync<TState>(documentId, cancellationToken).ConfigureAwait(false);
            }
        }

        if (loaded == null)
        {
            return Array.Empty<byte>();
        }

        return deps.Serializer.SerializeToBytes(loaded.Value);
    }

    internal static DistributedCrdtDocument<TState> CreateDocumentInstance(
        IServiceProvider serviceProvider,
        TState initialState,
        string typeAlias,
        ICrdtStringPool? stringPool = null,
        CrdtDocumentResidencyMode? residencyMode = null)
    {
        var deps = GetOrCreateDependencies(serviceProvider);
        return new DistributedCrdtDocument<TState>(
            initialState,
            deps.DecoratedServiceProvider,
            deps.VersionVectorManager,
            deps.Applicator,
            deps.SyncApplicator,
            deps.Merger,
            deps.MetadataManager,
            deps.Options,
            deps.Serializer,
            deps.Storage,
            deps.Logger,
            deps.DocumentIdProvider,
            deps.ShardIdProvider,
            deps.DistributionManager,
            deps.CommandPipeline,
            deps.Telemetry,
            deps.StateObservers,
            deps.PatchObservers,
            typeAlias,
            stringPool ?? deps.StringPool,
            residencyMode);
    }

    private DistributedCrdtDocument<TState> CreateTypedDocument(IServiceProvider serviceProvider, string documentId)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        if (string.IsNullOrWhiteSpace(documentId))
        {
            throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        }

        var deps = GetOrCreateDependencies(serviceProvider);
        documentId = deps.StringPool.GetOrAdd(documentId);
        var state = deps.DocumentIdProvider.CreateDocumentWithId<TState>(documentId);

        return new DistributedCrdtDocument<TState>(
            state,
            deps.DecoratedServiceProvider,
            deps.VersionVectorManager,
            deps.Applicator,
            deps.SyncApplicator,
            deps.Merger,
            deps.MetadataManager,
            deps.Options,
            deps.Serializer,
            deps.Storage,
            deps.Logger,
            deps.DocumentIdProvider,
            deps.ShardIdProvider,
            deps.DistributionManager,
            deps.CommandPipeline,
            deps.Telemetry,
            deps.StateObservers,
            deps.PatchObservers,
            TypeAlias,
            deps.StringPool,
            residencyModeOverride);
    }

    private static ScopedDocumentDependencies GetOrCreateDependencies(IServiceProvider serviceProvider)
    {
        return ScopedCache.GetValue(serviceProvider, static sp => new ScopedDocumentDependencies(sp));
    }

    private sealed class ScopedDocumentDependencies
    {
        public IServiceProvider DecoratedServiceProvider { get; }
        public IShardVersionVectorManager VersionVectorManager { get; }
        public IAsyncCrdtApplicator Applicator { get; }
        public ICrdtApplicator SyncApplicator { get; }
        public IAsyncCrdtMerger Merger { get; }
        public ICrdtMetadataManager MetadataManager { get; }
        public IOptions<DistributedCrdtOptions> Options { get; }
        public ICrdtSerializer Serializer { get; }
        public IDistributedCrdtStorage Storage { get; }
        public ILogger<DistributedCrdtDocument<TState>> Logger { get; }
        public IDocumentIdProvider DocumentIdProvider { get; }
        public IShardIdProvider ShardIdProvider { get; }
        public IShardDistributionManager DistributionManager { get; }
        public IDocumentCommandPipeline CommandPipeline { get; }
        public IDistributedCrdtTelemetry Telemetry { get; }
        public IDocumentStateObserver<TState>[] StateObservers { get; }
        public IDocumentPatchObserver[] PatchObservers { get; }
        public ICrdtStringPool StringPool { get; }

        public ScopedDocumentDependencies(IServiceProvider sp)
        {
            DecoratedServiceProvider = sp is SystemRegistryServiceProvider ? sp : new SystemRegistryServiceProvider(sp);
            VersionVectorManager = DecoratedServiceProvider.GetRequiredService<IShardVersionVectorManager>();
            Applicator = DecoratedServiceProvider.GetRequiredService<IAsyncCrdtApplicator>();
            SyncApplicator = DecoratedServiceProvider.GetRequiredService<ICrdtApplicator>();
            Merger = DecoratedServiceProvider.GetRequiredService<IAsyncCrdtMerger>();
            MetadataManager = DecoratedServiceProvider.GetRequiredService<ICrdtMetadataManager>();
            Options = DecoratedServiceProvider.GetRequiredService<IOptions<DistributedCrdtOptions>>();
            Serializer = DecoratedServiceProvider.GetRequiredService<ICrdtSerializer>();
            Storage = DecoratedServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
            Logger = DecoratedServiceProvider.GetRequiredService<ILogger<DistributedCrdtDocument<TState>>>();

            var rawDocIdProvider = DecoratedServiceProvider.GetRequiredService<IDocumentIdProvider>();
            DocumentIdProvider = rawDocIdProvider as SystemRegistryDocumentIdProviderDecorator
                ?? new SystemRegistryDocumentIdProviderDecorator(rawDocIdProvider);

            var rawShardIdProvider = DecoratedServiceProvider.GetRequiredService<IShardIdProvider>();
            ShardIdProvider = rawShardIdProvider as SystemRegistryShardIdProviderDecorator
                ?? new SystemRegistryShardIdProviderDecorator(rawShardIdProvider);

            DistributionManager = DecoratedServiceProvider.GetRequiredService<IShardDistributionManager>();
            CommandPipeline = DecoratedServiceProvider.GetRequiredService<IDocumentCommandPipeline>();
            Telemetry = DecoratedServiceProvider.GetRequiredService<IDistributedCrdtTelemetry>();

            var stateObs = DecoratedServiceProvider.GetServices<IDocumentStateObserver<TState>>();
            StateObservers = stateObs as IDocumentStateObserver<TState>[] ?? new List<IDocumentStateObserver<TState>>(stateObs).ToArray();

            var patchObs = DecoratedServiceProvider.GetServices<IDocumentPatchObserver>();
            PatchObservers = patchObs as IDocumentPatchObserver[] ?? new List<IDocumentPatchObserver>(patchObs).ToArray();

            StringPool = DecoratedServiceProvider.GetService<ICrdtStringPool>() ?? CrdtStringPool.Shared;
        }
    }
}