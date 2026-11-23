namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Pooling;
using Ama.CRDT.Services.Providers;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Centralized gateway that manages distributed CRDT documents and coordinates topology, remote proxy calls, anti-entropy, and Map/Reduce jobs.
/// </summary>
public sealed class CrdtDocumentOrchestrator : ICrdtDocumentOrchestrator, IDisposable
{
    private static readonly AsyncLocal<CrdtDocumentOrchestrator?> CurrentProcessingOrchestrator = new();

    private readonly IServiceProvider serviceProvider;
    private readonly IDistributedCrdtStorage storage;
    private readonly IAsyncCrdtPatcher patcher;
    private readonly IShardDistributionManager distributionManager;
    private readonly ICrdtRemoteProxyClient remoteProxyClient;
    private readonly ICrdtMapReduceCoordinator mapReduceCoordinator;
    private readonly ICrdtAntiEntropyCoordinator antiEntropyCoordinator;
    private readonly ICrdtOrchestratorObserver[] orchestratorObservers;
    private readonly ILogger<CrdtDocumentOrchestrator> logger;
    private readonly IDistributedCrdtTelemetry telemetry;
    private readonly ICrdtStringPool stringPool;
    private readonly IDisposable telemetryRegistration;
    private readonly DistributedCrdtOptions options;

    private readonly ConcurrentDictionary<string, IDistributedCrdtDocument> activeDocuments = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string> shardIdCache = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Type, string> stateTypeAliases = new();

    private readonly Channel<PooledOrchestratorCommand> commandChannel;
    private readonly ConcurrentQueue<PooledOrchestratorCommand> commandPool = new();
    private readonly CancellationTokenSource disposeCts = new();
    private readonly Task processingTask;
    private int syncPending;
    private int activeOrchestratorCommandCount;
    private int initializationStage = (int)OrchestratorInitializationStage.NotStarted;

    /// <summary>
    /// Gets a value indicating whether the current execution context is already running inside this orchestrator's command processing loop.
    /// </summary>
    public bool IsCurrentThreadOrchestratorProcessor => ReferenceEquals(CurrentProcessingOrchestrator.Value, this);

    /// <summary>
    /// Gets the number of commands currently queued or executing in the orchestrator channel.
    /// </summary>
    public int PendingCommandCount =>
        (commandChannel.Reader.CanCount ? commandChannel.Reader.Count : 0) +
        Volatile.Read(ref activeOrchestratorCommandCount);

    /// <summary>
    /// Gets a value indicating whether an anti-entropy dispatch or incoming state sync response is currently active.
    /// </summary>
    public bool IsAntiEntropyBusy => antiEntropyCoordinator.IsAntiEntropyBusy;

    /// <inheritdoc />
    public OrchestratorInitializationStage InitializationStage => (OrchestratorInitializationStage)Volatile.Read(ref initializationStage);

    /// <inheritdoc />
    public IDistributedCrdtDocument<CrdtRegistryState> Registry { get; private set; } = null!;

    public CrdtDocumentOrchestrator(
        IServiceProvider serviceProvider,
        IDistributedCrdtStorage storage,
        IAsyncCrdtPatcher patcher,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtDocumentOrchestrator> logger,
        IShardDistributionManager distributionManager,
        ICrdtRemoteProxyClient remoteProxyClient,
        ICrdtMapReduceCoordinator mapReduceCoordinator,
        ICrdtAntiEntropyCoordinator antiEntropyCoordinator,
        IEnumerable<ICrdtOrchestratorObserver> orchestratorObservers,
        IDistributedCrdtTelemetry telemetry,
        ICrdtStringPool? stringPool = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));
        this.distributionManager = distributionManager ?? throw new ArgumentNullException(nameof(distributionManager));
        this.remoteProxyClient = remoteProxyClient ?? throw new ArgumentNullException(nameof(remoteProxyClient));
        this.mapReduceCoordinator = mapReduceCoordinator ?? throw new ArgumentNullException(nameof(mapReduceCoordinator));
        this.antiEntropyCoordinator = antiEntropyCoordinator ?? throw new ArgumentNullException(nameof(antiEntropyCoordinator));
        ArgumentNullException.ThrowIfNull(orchestratorObservers);
        this.orchestratorObservers = orchestratorObservers as ICrdtOrchestratorObserver[] ?? new List<ICrdtOrchestratorObserver>(orchestratorObservers).ToArray();
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        this.stringPool = stringPool ?? CrdtStringPool.Shared;
        this.options = options.Value;

        foreach (var factory in this.serviceProvider.GetServices<IDocumentFactory>())
        {
            this.stateTypeAliases.TryAdd(factory.StateType, this.stringPool.GetOrAdd(factory.TypeAlias));
        }

        var capacity = this.options.ChannelCapacity;
        if (capacity > 0)
        {
            this.commandChannel = Channel.CreateBounded<PooledOrchestratorCommand>(new BoundedChannelOptions(capacity)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });
        }
        else
        {
            this.commandChannel = Channel.CreateUnbounded<PooledOrchestratorCommand>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });
        }

        this.telemetryRegistration = this.telemetry.RegisterOrchestratorPipelineMetrics(
            () => this.commandChannel.Reader.CanCount ? this.commandChannel.Reader.Count : 0,
            () => this.commandPool.Count);

        this.processingTask = Task.Run(ProcessChannelAsync);
    }

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (IsCurrentThreadOrchestratorProcessor)
        {
            await ProcessInitializeInternalAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        var cmd = GetCommand();
        cmd.Type = OrchestratorCommandType.Initialize;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            telemetry.RecordOrchestratorCommandEnqueued();
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public Task ReevaluateTopologyAsync(CancellationToken cancellationToken = default)
    {
        return ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task ReevaluateTopologyAsync(IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (IsCurrentThreadOrchestratorProcessor && deliveryMode != IntentDeliveryMode.FireAndForget)
        {
            Interlocked.Exchange(ref syncPending, 0);
            await ProcessSyncDocumentsInternalAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            if (Interlocked.Exchange(ref syncPending, 1) == 1)
            {
                return;
            }

            var ffCmd = GetCommand();
            ffCmd.Type = OrchestratorCommandType.ReevaluateTopology;
            ffCmd.IsFireAndForget = true;
            ffCmd.CancellationToken = CancellationToken.None;

            if (commandChannel.Writer.TryWrite(ffCmd))
            {
                telemetry.RecordOrchestratorCommandEnqueued();
                return;
            }

            if (IsCurrentThreadOrchestratorProcessor)
            {
                Interlocked.Exchange(ref syncPending, 0);
                ReturnCommand(ffCmd);
                await ProcessSyncDocumentsInternalAsync(cancellationToken).ConfigureAwait(false);
                return;
            }

            await commandChannel.Writer.WriteAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            telemetry.RecordOrchestratorCommandEnqueued();
            return;
        }

        var cmd = GetCommand();
        cmd.Type = OrchestratorCommandType.ReevaluateTopology;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            telemetry.RecordOrchestratorCommandEnqueued();
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<IDistributedCrdtDocument> GetActiveDocuments()
    {
        var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");
        var docs = new List<IDistributedCrdtDocument>(activeDocuments.Count + 1) { registry };
        docs.AddRange(activeDocuments.Values);
        return docs;
    }

    /// <inheritdoc />
    public IDistributedCrdtDocument<TState>? TryGetLocalDocument<TState>(string documentId) where TState : class, new()
    {
        if (string.IsNullOrWhiteSpace(documentId)) return null;

        documentId = stringPool.GetOrAdd(documentId);

        if (Registry != null && Registry.DocumentId == documentId && Registry is IDistributedCrdtDocument<TState> registryTyped)
        {
            return registryTyped;
        }

        var typeAlias = GetTypeAlias<TState>();
        if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            var compositeKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
            if (activeDocuments.TryGetValue(compositeKey, out var docByComposite) && docByComposite is IDistributedCrdtDocument<TState> typedByComposite)
            {
                return typedByComposite;
            }
        }

        if (activeDocuments.TryGetValue(documentId, out var doc) && doc is IDistributedCrdtDocument<TState> typedDoc)
        {
            return typedDoc;
        }

        foreach (var activeDoc in activeDocuments.Values)
        {
            if (string.Equals(activeDoc.DocumentId, documentId, StringComparison.Ordinal) && activeDoc is IDistributedCrdtDocument<TState> matchedTypedDoc)
            {
                return matchedTypedDoc;
            }
        }

        return null;
    }

    /// <inheritdoc />
    public ValueTask<bool> DocumentExistsAsync(string documentId, CancellationToken cancellationToken = default)
    {
        return DocumentExistsAsync(documentId, null, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> DocumentExistsAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        if (string.IsNullOrWhiteSpace(documentId)) return new ValueTask<bool>(false);

        if (typeof(TState) == typeof(CrdtRegistryState) && Registry != null && string.Equals(Registry.DocumentId, documentId, StringComparison.Ordinal))
        {
            return new ValueTask<bool>(true);
        }

        var typeAlias = GetTypeAlias<TState>();
        return DocumentExistsAsync(documentId, typeAlias, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> DocumentExistsAsync(string documentId, string? typeAlias, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return new ValueTask<bool>(false);

        documentId = stringPool.GetOrAdd(documentId);
        if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            typeAlias = stringPool.GetOrAdd(typeAlias);
        }

        if (Registry != null && string.Equals(Registry.DocumentId, documentId, StringComparison.Ordinal))
        {
            var matchesRegistry = string.IsNullOrWhiteSpace(typeAlias) ||
                   string.Equals(typeAlias, Registry.TypeAlias, StringComparison.Ordinal) ||
                   string.Equals(typeAlias, Constants.SystemRegistryDocumentId, StringComparison.Ordinal);
            return new ValueTask<bool>(matchesRegistry);
        }

        if (Registry != null)
        {
            var registryDoc = Registry.CurrentDocument;
            if (TryFindRegistryEntry(registryDoc.Data.Documents, documentId, typeAlias, out var entry) && entry != null)
            {
                return new ValueTask<bool>(!entry.IsDeleted);
            }
        }

        if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            var compositeKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
            if (activeDocuments.ContainsKey(compositeKey))
            {
                return new ValueTask<bool>(true);
            }

            if (activeDocuments.TryGetValue(documentId, out var docById) && string.Equals(docById.TypeAlias, typeAlias, StringComparison.Ordinal))
            {
                return new ValueTask<bool>(true);
            }

            foreach (var doc in activeDocuments.Values)
            {
                if (string.Equals(doc.DocumentId, documentId, StringComparison.Ordinal) &&
                    string.Equals(doc.TypeAlias, typeAlias, StringComparison.Ordinal))
                {
                    return new ValueTask<bool>(true);
                }
            }

            return new ValueTask<bool>(false);
        }

        if (activeDocuments.ContainsKey(documentId))
        {
            return new ValueTask<bool>(true);
        }

        foreach (var doc in activeDocuments.Values)
        {
            if (string.Equals(doc.DocumentId, documentId, StringComparison.Ordinal))
            {
                return new ValueTask<bool>(true);
            }
        }

        return new ValueTask<bool>(false);
    }

    /// <inheritdoc />
    public ValueTask<string?> TryGetShardIdAsync(string documentId, CancellationToken cancellationToken = default)
    {
        return TryGetShardIdAsync(documentId, null, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> TryGetShardIdAsync(string documentId, string? typeAlias, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return new ValueTask<string?>((string?)null);

        documentId = stringPool.GetOrAdd(documentId);
        if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            typeAlias = stringPool.GetOrAdd(typeAlias);
        }

        if (Registry != null && string.Equals(Registry.DocumentId, documentId, StringComparison.Ordinal))
        {
            return new ValueTask<string?>(Registry.ShardId);
        }

        var cacheKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
        if (shardIdCache.TryGetValue(cacheKey, out var cachedShardId))
        {
            return new ValueTask<string?>(cachedShardId);
        }

        if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            if (activeDocuments.TryGetValue(cacheKey, out var typedActiveDoc))
            {
                shardIdCache.TryAdd(cacheKey, typedActiveDoc.ShardId);
                return new ValueTask<string?>(typedActiveDoc.ShardId);
            }
        }
        else if (activeDocuments.TryGetValue(documentId, out var activeDoc))
        {
            shardIdCache.TryAdd(cacheKey, activeDoc.ShardId);
            return new ValueTask<string?>(activeDoc.ShardId);
        }

        foreach (var doc in activeDocuments.Values)
        {
            if (string.Equals(doc.DocumentId, documentId, StringComparison.Ordinal) &&
                (string.IsNullOrWhiteSpace(typeAlias) || string.Equals(doc.TypeAlias, typeAlias, StringComparison.Ordinal)))
            {
                shardIdCache.TryAdd(cacheKey, doc.ShardId);
                return new ValueTask<string?>(doc.ShardId);
            }
        }

        if (Registry != null)
        {
            var registryDoc = Registry.CurrentDocument;
            if (!TryFindRegistryEntry(registryDoc.Data.Documents, documentId, typeAlias, out var entry) && !string.IsNullOrWhiteSpace(typeAlias))
            {
                var factory = serviceProvider.GetKeyedService<IDocumentFactory>(typeAlias);
                if (factory != null)
                {
                    var resolvedShardId = stringPool.GetOrAdd(factory.GetShardId(serviceProvider, documentId));
                    shardIdCache.TryAdd(cacheKey, resolvedShardId);
                    return new ValueTask<string?>(resolvedShardId);
                }
            }
            else if (entry != null && !entry.IsDeleted)
            {
                var factory = serviceProvider.GetKeyedService<IDocumentFactory>(entry.TypeAlias);
                if (factory != null)
                {
                    var resolvedShardId = stringPool.GetOrAdd(factory.GetShardId(serviceProvider, entry.DocumentId));
                    shardIdCache.TryAdd(CrdtRegistryEntry.GetRegistryKey(entry.TypeAlias, entry.DocumentId), resolvedShardId);
                    shardIdCache.TryAdd(cacheKey, resolvedShardId);
                    return new ValueTask<string?>(resolvedShardId);
                }
            }
        }

        return new ValueTask<string?>((string?)null);
    }

    /// <inheritdoc />
    public async ValueTask<IDistributedCrdtDocument?> ResolveOrPromoteLocalDocumentAsync(string documentId, string? typeAlias = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return null;

        documentId = stringPool.GetOrAdd(documentId);
        if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            typeAlias = stringPool.GetOrAdd(typeAlias);
        }

        if (Registry != null && string.Equals(Registry.DocumentId, documentId, StringComparison.Ordinal))
        {
            return Registry;
        }

        if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            var compositeKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
            if (activeDocuments.TryGetValue(compositeKey, out var existingTypedDoc))
            {
                return existingTypedDoc;
            }
        }
        else
        {
            if (activeDocuments.TryGetValue(documentId, out var existingDoc))
            {
                return existingDoc;
            }

            foreach (var doc in activeDocuments.Values)
            {
                if (string.Equals(doc.DocumentId, documentId, StringComparison.Ordinal))
                {
                    return doc;
                }
            }
        }

        var resolvedAlias = typeAlias;
        if (Registry != null)
        {
            var registryDoc = Registry.CurrentDocument;
            if (TryFindRegistryEntry(registryDoc.Data.Documents, documentId, resolvedAlias, out var entry) && entry != null)
            {
                if (entry.IsDeleted)
                {
                    return null;
                }

                if (string.IsNullOrWhiteSpace(resolvedAlias))
                {
                    resolvedAlias = entry.TypeAlias;
                }
            }
        }

        if (string.IsNullOrWhiteSpace(resolvedAlias))
        {
            return null;
        }

        resolvedAlias = stringPool.GetOrAdd(resolvedAlias);
        var resolvedKey = CrdtRegistryEntry.GetRegistryKey(resolvedAlias, documentId);
        if (activeDocuments.TryGetValue(resolvedKey, out var alreadyActiveDoc))
        {
            return alreadyActiveDoc;
        }

        var factory = serviceProvider.GetKeyedService<IDocumentFactory>(resolvedAlias);
        if (factory == null)
        {
            return null;
        }

        if (!shardIdCache.TryGetValue(resolvedKey, out var shardId))
        {
            shardId = stringPool.GetOrAdd(factory.GetShardId(serviceProvider, documentId));
            shardIdCache.TryAdd(resolvedKey, shardId);
        }

        if (!await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        if (IsCurrentThreadOrchestratorProcessor)
        {
            await ProcessPromoteDocumentInternalAsync(documentId, resolvedAlias, cancellationToken).ConfigureAwait(false);
            activeDocuments.TryGetValue(resolvedKey, out var inlinePromotedDoc);
            return inlinePromotedDoc;
        }

        var cmd = GetCommand();
        cmd.Type = OrchestratorCommandType.CreateDocument;
        cmd.DocumentId = documentId;
        cmd.TypeAlias = resolvedAlias;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            telemetry.RecordOrchestratorCommandEnqueued();
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }

        activeDocuments.TryGetValue(resolvedKey, out var promotedDoc);
        return promotedDoc;
    }

    /// <inheritdoc />
    public async ValueTask<byte[]> TryLoadStoredDocumentSnapshotBytesAsync(string documentId, string? typeAlias = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId))
        {
            return Array.Empty<byte>();
        }

        documentId = stringPool.GetOrAdd(documentId);
        if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            typeAlias = stringPool.GetOrAdd(typeAlias);
        }

        if (Registry != null && string.Equals(Registry.DocumentId, documentId, StringComparison.Ordinal))
        {
            var registrySnapshot = await Registry.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
            return registrySnapshot.SnapshotData;
        }

        var resolvedAlias = typeAlias;
        if (Registry != null)
        {
            var registryDoc = Registry.CurrentDocument;
            if (TryFindRegistryEntry(registryDoc.Data.Documents, documentId, resolvedAlias, out var entry) && entry != null)
            {
                if (entry.IsDeleted)
                {
                    return Array.Empty<byte>();
                }

                if (string.IsNullOrWhiteSpace(resolvedAlias))
                {
                    resolvedAlias = entry.TypeAlias;
                }
            }
        }

        if (string.IsNullOrWhiteSpace(resolvedAlias))
        {
            return Array.Empty<byte>();
        }

        resolvedAlias = stringPool.GetOrAdd(resolvedAlias);
        var factory = serviceProvider.GetKeyedService<IDocumentFactory>(resolvedAlias);
        if (factory == null)
        {
            return Array.Empty<byte>();
        }

        var compositeKey = CrdtRegistryEntry.GetRegistryKey(resolvedAlias, documentId);
        if (!shardIdCache.TryGetValue(compositeKey, out var shardId))
        {
            shardId = stringPool.GetOrAdd(factory.GetShardId(serviceProvider, documentId));
            shardIdCache.TryAdd(compositeKey, shardId);
        }

        return await factory.TryLoadSerializedDocumentFromStorageAsync(serviceProvider, shardId, documentId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<CrdtDocument<TState>> GetDocumentStateAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        documentId = stringPool.GetOrAdd(documentId);

        var localTypedDoc = TryGetLocalDocument<TState>(documentId);
        if (localTypedDoc != null)
        {
            return localTypedDoc.CurrentDocument;
        }

        var documentTypeAlias = GetTypeAlias<TState>();
        var shardId = await GetShardIdAsync<TState>(documentId, cancellationToken).ConfigureAwait(false);
        var isLocal = await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false);

        if (isLocal)
        {
            var promotedDoc = await ResolveOrPromoteLocalDocumentAsync(documentId, documentTypeAlias, cancellationToken).ConfigureAwait(false);
            if (promotedDoc is IDistributedCrdtDocument<TState> promotedTypedDoc)
            {
                return promotedTypedDoc.CurrentDocument;
            }
        }

        return await remoteProxyClient.GetRemoteDocumentStateAsync<TState>(documentId, shardId, documentTypeAlias, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<bool> ApplyIntentAsync<TState>(string documentId, IOperationIntent intent, string targetProperty, CancellationToken cancellationToken = default) where TState : class, new()
    {
        return ApplyIntentAsync<TState>(documentId, intent, targetProperty, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> ApplyIntentAsync<TState>(string documentId, IOperationIntent intent, string targetProperty, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default) where TState : class, new()
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        ArgumentNullException.ThrowIfNull(intent);

        documentId = stringPool.GetOrAdd(documentId);
        if (!string.IsNullOrEmpty(targetProperty))
        {
            targetProperty = stringPool.GetOrAdd(targetProperty);
        }

        if (Registry != null && string.Equals(Registry.DocumentId, documentId, StringComparison.Ordinal) && Registry is IDistributedCrdtDocument<TState> registryTyped)
        {
            var applied = await registryTyped.ApplyRemoteIntentAsync(intent, targetProperty, patcher, deliveryMode, cancellationToken).ConfigureAwait(false);
            if (applied && deliveryMode == IntentDeliveryMode.Acknowledged)
            {
                await ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, cancellationToken).ConfigureAwait(false);
            }
            return applied;
        }

        var fastTypedDoc = TryGetLocalDocument<TState>(documentId);
        if (fastTypedDoc != null)
        {
            return await fastTypedDoc.ApplyRemoteIntentAsync(intent, targetProperty, patcher, deliveryMode, cancellationToken).ConfigureAwait(false);
        }

        var documentTypeAlias = GetTypeAlias<TState>();
        var shardId = await GetShardIdAsync<TState>(documentId, cancellationToken).ConfigureAwait(false);
        var isLocal = await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false);

        if (isLocal)
        {
            var activeDoc = await ResolveOrPromoteLocalDocumentAsync(documentId, documentTypeAlias, cancellationToken).ConfigureAwait(false);
            if (activeDoc is IDistributedCrdtDocument<TState> typedDoc)
            {
                return await typedDoc.ApplyRemoteIntentAsync(intent, targetProperty, patcher, deliveryMode, cancellationToken).ConfigureAwait(false);
            }
        }

        return await remoteProxyClient.ForwardIntentAsync(
            documentId,
            shardId,
            documentTypeAlias,
            intent,
            targetProperty,
            deliveryMode,
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task ApplyPatchAsync<TState>(string documentId, CrdtPatch patch, CancellationToken cancellationToken = default) where TState : class, new()
    {
        return ApplyPatchAsync<TState>(documentId, patch, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task ApplyPatchAsync<TState>(string documentId, CrdtPatch patch, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default) where TState : class, new()
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        if (patch.Operations == null || patch.Operations.Count == 0)
        {
            return;
        }

        documentId = stringPool.GetOrAdd(documentId);

        if (Registry != null && string.Equals(Registry.DocumentId, documentId, StringComparison.Ordinal) && Registry is IDistributedCrdtDocument<TState> registryTyped)
        {
            await registryTyped.ApplyPatchAsync(patch, deliveryMode, cancellationToken).ConfigureAwait(false);
            if (deliveryMode == IntentDeliveryMode.Acknowledged)
            {
                await ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, cancellationToken).ConfigureAwait(false);
            }
            return;
        }

        var fastTypedDoc = TryGetLocalDocument<TState>(documentId);
        if (fastTypedDoc != null)
        {
            await fastTypedDoc.ApplyPatchAsync(patch, deliveryMode, cancellationToken).ConfigureAwait(false);
            return;
        }

        var documentTypeAlias = GetTypeAlias<TState>();
        var shardId = await GetShardIdAsync<TState>(documentId, cancellationToken).ConfigureAwait(false);
        var isLocal = await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false);

        if (isLocal)
        {
            var activeDoc = await ResolveOrPromoteLocalDocumentAsync(documentId, documentTypeAlias, cancellationToken).ConfigureAwait(false);
            if (activeDoc is IDistributedCrdtDocument<TState> typedDoc)
            {
                await typedDoc.ApplyPatchAsync(patch, deliveryMode, cancellationToken).ConfigureAwait(false);
                return;
            }
        }

        await remoteProxyClient.ForwardPatchAsync(documentId, shardId, documentTypeAlias, patch, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task CreateDocumentAsync(string documentId, string typeAlias, CancellationToken cancellationToken = default)
    {
        return CreateDocumentAsync(documentId, typeAlias, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task CreateDocumentAsync(string documentId, string typeAlias, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(typeAlias)) throw new ArgumentException("Type alias cannot be null or empty.", nameof(typeAlias));

        documentId = stringPool.GetOrAdd(documentId);
        typeAlias = stringPool.GetOrAdd(typeAlias);

        var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");
        var registryKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
        var intent = new MapSetIntent(registryKey, new CrdtRegistryEntry(documentId, typeAlias, false));

        if (Registry is IDistributedCrdtDocument<CrdtRegistryState> registryTyped)
        {
            var applied = await registryTyped.ApplyRemoteIntentAsync(intent, nameof(CrdtRegistryState.Documents), patcher, deliveryMode, cancellationToken).ConfigureAwait(false);
            if (applied)
            {
                await PromoteSingleDocumentAsync(documentId, typeAlias, deliveryMode, cancellationToken).ConfigureAwait(false);
                logger.LogDebug("Forwarded creation intent for document {DocumentId} ({TypeAlias}).", documentId, typeAlias);
            }
            else
            {
                logger.LogWarning("Failed to forward creation intent for document {DocumentId} ({TypeAlias}).", documentId, typeAlias);
            }

            return;
        }

        var success = await ApplyIntentAsync<CrdtRegistryState>(registry.DocumentId, intent, nameof(CrdtRegistryState.Documents), deliveryMode, cancellationToken).ConfigureAwait(false);

        if (success)
        {
            logger.LogDebug("Forwarded creation intent for document {DocumentId} ({TypeAlias}).", documentId, typeAlias);
        }
        else
        {
            logger.LogWarning("Failed to forward creation intent for document {DocumentId} ({TypeAlias}).", documentId, typeAlias);
        }
    }

    /// <inheritdoc />
    public Task DeleteDocumentAsync(string documentId, CancellationToken cancellationToken = default)
    {
        return DeleteDocumentAsync(documentId, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public Task DeleteDocumentAsync(string documentId, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        return DeleteDocumentInternalAsync(documentId, null, deliveryMode, cancellationToken);
    }

    /// <inheritdoc />
    public Task DeleteDocumentAsync(string documentId, string typeAlias, CancellationToken cancellationToken = default)
    {
        return DeleteDocumentAsync(documentId, typeAlias, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public Task DeleteDocumentAsync(string documentId, string typeAlias, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(typeAlias)) throw new ArgumentException("Type alias cannot be null or empty.", nameof(typeAlias));
        return DeleteDocumentInternalAsync(documentId, typeAlias, deliveryMode, cancellationToken);
    }

    /// <inheritdoc />
    public Task ProvideSnapshotAsync(string documentId, string targetReplicaId, PeerId targetPeerId, CancellationToken cancellationToken = default)
    {
        return antiEntropyCoordinator.ProvideSnapshotAsync(this, documentId, null, targetReplicaId, targetPeerId, null, null, cancellationToken);
    }

    /// <inheritdoc />
    public Task ProvideSnapshotAsync(string documentId, string? typeAlias, string targetReplicaId, PeerId targetPeerId, CancellationToken cancellationToken = default)
    {
        return antiEntropyCoordinator.ProvideSnapshotAsync(this, documentId, typeAlias, targetReplicaId, targetPeerId, null, null, cancellationToken);
    }

    /// <inheritdoc />
    public Task ProvideSnapshotAsync(
        string documentId,
        string? typeAlias,
        string targetReplicaId,
        PeerId targetPeerId,
        IReadOnlyList<string>? shardDocumentIds,
        ShardVersionVectorMap? consistentGlobalState = null,
        CancellationToken cancellationToken = default)
    {
        return antiEntropyCoordinator.ProvideSnapshotAsync(
            this,
            documentId,
            typeAlias,
            targetReplicaId,
            targetPeerId,
            shardDocumentIds,
            consistentGlobalState,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<CrdtAntiEntropyWorkLoadDto> EvaluateAndDispatchAntiEntropyStateAsync(CancellationToken cancellationToken = default)
    {
        return antiEntropyCoordinator.EvaluateAndDispatchAntiEntropyStateAsync(this, cancellationToken);
    }

    /// <inheritdoc />
    public Task DispatchAntiEntropyStateAsync(CancellationToken cancellationToken = default)
    {
        return antiEntropyCoordinator.DispatchAntiEntropyStateAsync(this, cancellationToken);
    }

    /// <inheritdoc />
    public Task DispatchAntiEntropyStateAsync(PeerId targetPeerId, CancellationToken cancellationToken = default)
    {
        return antiEntropyCoordinator.DispatchAntiEntropyStateAsync(this, targetPeerId, cancellationToken);
    }

    /// <inheritdoc />
    public Task DispatchTopologyAnnouncementAsync(PeerId targetPeerId, CancellationToken cancellationToken = default)
    {
        return antiEntropyCoordinator.DispatchTopologyAnnouncementAsync(this, targetPeerId, cancellationToken);
    }

    /// <summary>
    /// Attempts to acquire a global anti-entropy work scope if the node is not overloaded or already busy.
    /// </summary>
    public IDisposable? TryAcquireAntiEntropyWorkScope()
    {
        return antiEntropyCoordinator.TryAcquireAntiEntropyWorkScope(PendingCommandCount);
    }

    /// <summary>
    /// Attempts to acquire a per-shard anti-entropy work scope if the shard and node queues are not busy.
    /// </summary>
    public IDisposable? TryAcquireAntiEntropyWorkScope(string shardId)
    {
        return antiEntropyCoordinator.TryAcquireAntiEntropyWorkScope(shardId, PendingCommandCount);
    }

    /// <inheritdoc />
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        var commandPipeline = serviceProvider.GetRequiredService<IDocumentCommandPipeline>();
        await commandPipeline.FlushAsync(cancellationToken).ConfigureAwait(false);

        if (!IsCurrentThreadOrchestratorProcessor)
        {
            var cmd = GetCommand();
            cmd.Type = OrchestratorCommandType.FlushBarrier;
            cmd.CancellationToken = cancellationToken;

            try
            {
                await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
                telemetry.RecordOrchestratorCommandEnqueued();
                await cmd.ExecuteAsync().ConfigureAwait(false);
            }
            finally
            {
                ReturnCommand(cmd);
            }
        }

        await ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, cancellationToken).ConfigureAwait(false);
        await commandPipeline.FlushAsync(cancellationToken).ConfigureAwait(false);

        foreach (var doc in activeDocuments.Values)
        {
            await doc.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<TResult> ExecuteMapReduceAsync<TState, TInput, TAccumulate, TResult>(
        TInput input,
        CancellationToken cancellationToken = default)
        where TState : class, new()
    {
        var resultDto = await ExecuteMapReduceAsync<TState, TInput, TAccumulate, TResult>(
            input,
            default,
            cancellationToken).ConfigureAwait(false);
        return resultDto.Result;
    }

    /// <inheritdoc />
    public Task<TResult> QueryClusterAsync<TState, TQuery, TResult>(
        TQuery query,
        CancellationToken cancellationToken = default)
        where TState : class, new()
    {
        return ExecuteMapReduceAsync<TState, TQuery, TResult, TResult>(query, cancellationToken);
    }

    /// <inheritdoc />
    public Task<CrdtMapReduceResultDto<TResult>> QueryClusterAsync<TState, TQuery, TResult>(
        TQuery query,
        CrdtMapReduceScopeDto scope,
        CancellationToken cancellationToken = default)
        where TState : class, new()
    {
        return ExecuteMapReduceAsync<TState, TQuery, TResult, TResult>(query, scope, cancellationToken);
    }

    /// <inheritdoc />
    public Task<CrdtMapReduceResultDto<TResult>> ExecuteMapReduceAsync<TState, TInput, TAccumulate, TResult>(
        TInput input,
        CrdtMapReduceScopeDto scope,
        CancellationToken cancellationToken = default)
        where TState : class, new()
    {
        return mapReduceCoordinator.ExecuteMapReduceAsync<TState, TInput, TAccumulate, TResult>(
            this,
            GetTypeAlias<TState>(),
            input,
            scope,
            cancellationToken);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        telemetryRegistration.Dispose();
        disposeCts.Cancel();
        commandChannel?.Writer.TryComplete();
        disposeCts.Dispose();

        if (Registry is IDisposable rd)
        {
            rd.Dispose();
        }

        foreach (var doc in activeDocuments.Values)
        {
            if (doc is IDisposable d) d.Dispose();
        }

        activeDocuments.Clear();
        shardIdCache.Clear();
    }

    private async Task NotifyTopologyChangedAsync(CancellationToken cancellationToken)
    {
        for (int i = 0; i < orchestratorObservers.Length; i++)
        {
            try
            {
                await orchestratorObservers[i].OnTopologyChangedAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error notifying orchestrator observer.");
            }
        }
    }

    private PooledOrchestratorCommand GetCommand() => commandPool.TryDequeue(out var cmd) ? cmd : new PooledOrchestratorCommand();

    private void ReturnCommand(PooledOrchestratorCommand cmd)
    {
        cmd.Reset();
        commandPool.Enqueue(cmd);
    }

    private async Task ProcessChannelAsync()
    {
        try
        {
            await foreach (var cmd in commandChannel.Reader.ReadAllAsync(disposeCts.Token).ConfigureAwait(false))
            {
                Interlocked.Increment(ref activeOrchestratorCommandCount);
                try
                {
                    cmd.CancellationToken.ThrowIfCancellationRequested();

                    CurrentProcessingOrchestrator.Value = this;
                    try
                    {
                        switch (cmd.Type)
                        {
                            case OrchestratorCommandType.Initialize:
                                await ProcessInitializeInternalAsync(cmd.CancellationToken).ConfigureAwait(false);
                                break;
                            case OrchestratorCommandType.CreateDocument:
                                await ProcessPromoteDocumentInternalAsync(cmd.DocumentId!, cmd.TypeAlias!, cmd.CancellationToken).ConfigureAwait(false);
                                break;
                            case OrchestratorCommandType.ReevaluateTopology:
                                Interlocked.Exchange(ref syncPending, 0);
                                await ProcessSyncDocumentsInternalAsync(cmd.CancellationToken).ConfigureAwait(false);
                                break;
                            case OrchestratorCommandType.FlushBarrier:
                                break;
                        }
                    }
                    finally
                    {
                        CurrentProcessingOrchestrator.Value = null;
                    }

                    telemetry.RecordOrchestratorCommandProcessed();

                    if (cmd.IsFireAndForget)
                    {
                        ReturnCommand(cmd);
                    }
                    else
                    {
                        cmd.SetResult();
                    }
                }
                catch (OperationCanceledException)
                {
                    if (cmd.IsFireAndForget)
                    {
                        ReturnCommand(cmd);
                    }
                    else
                    {
                        cmd.SetException(new OperationCanceledException("Orchestrator operation cancelled."));
                    }
                }
                catch (Exception ex)
                {
                    telemetry.RecordOrchestratorCommandFailure();
                    logger.LogError(ex, "Error executing orchestrator command {CommandType} for document {DocumentId}.", cmd.Type, cmd.DocumentId);
                    if (cmd.IsFireAndForget)
                    {
                        ReturnCommand(cmd);
                    }
                    else
                    {
                        cmd.SetException(ex);
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref activeOrchestratorCommandCount);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Orchestrator background channel failed.");
        }
    }

    private async Task ProcessInitializeInternalAsync(CancellationToken cancellationToken)
    {
        if (InitializationStage == OrchestratorInitializationStage.Initialized && Registry != null)
        {
            await ProcessSyncDocumentsInternalAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        Volatile.Write(ref initializationStage, (int)OrchestratorInitializationStage.Initializing);

        try
        {
            if (Registry is IDisposable existingRegistryDisposable)
            {
                try
                {
                    existingRegistryDisposable.Dispose();
                }
                catch (ObjectDisposedException) { }
            }

            var registryState = new CrdtRegistryState();
            Registry = ActivatorUtilities.CreateInstance<DistributedCrdtDocument<CrdtRegistryState>>(serviceProvider, registryState);
            shardIdCache.TryAdd(Registry.DocumentId, Registry.ShardId);

            await Registry.InitializeAsync(cancellationToken).ConfigureAwait(false);

            var versionVectorManager = serviceProvider.GetRequiredService<IShardVersionVectorManager>();
            versionVectorManager.EnsureShardRegistered(Registry.ShardId);

            await ProcessSyncDocumentsInternalAsync(cancellationToken).ConfigureAwait(false);

            Volatile.Write(ref initializationStage, (int)OrchestratorInitializationStage.Initialized);
        }
        catch
        {
            Volatile.Write(ref initializationStage, (int)OrchestratorInitializationStage.NotStarted);
            throw;
        }
    }

    private async Task ProcessPromoteDocumentInternalAsync(string documentId, string typeAlias, CancellationToken cancellationToken)
    {
        documentId = stringPool.GetOrAdd(documentId);
        typeAlias = stringPool.GetOrAdd(typeAlias);

        var compositeKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
        if (activeDocuments.ContainsKey(compositeKey))
        {
            return;
        }

        var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");
        var registryDoc = await registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
        var registryMap = registryDoc.Data.Documents;
        if ((registryMap.TryGetValue(compositeKey, out var existingEntry) || registryMap.TryGetValue(documentId, out existingEntry))
            && existingEntry != null
            && existingEntry.IsDeleted)
        {
            return;
        }

        var factory = serviceProvider.GetKeyedService<IDocumentFactory>(typeAlias);
        if (factory == null)
        {
            logger.LogWarning("Cannot find document factory for type alias: {TypeAlias}", typeAlias);
            return;
        }

        if (!shardIdCache.TryGetValue(compositeKey, out var shardId))
        {
            shardId = stringPool.GetOrAdd(factory.GetShardId(serviceProvider, documentId));
            shardIdCache.TryAdd(compositeKey, shardId);
        }

        if (!await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false))
        {
            return;
        }

        var doc = factory.CreateDocument(serviceProvider, documentId);
        await doc.InitializeAsync(cancellationToken).ConfigureAwait(false);

        if (activeDocuments.TryAdd(compositeKey, doc))
        {
            telemetry.RecordOrchestratorDocumentCreated(documentId);
            logger.LogDebug("Mapped new active CRDT document: {DocumentId} ({TypeAlias})", documentId, typeAlias);

            var versionVectorManager = serviceProvider.GetRequiredService<IShardVersionVectorManager>();
            versionVectorManager.EnsureShardRegistered(doc.ShardId);

            await NotifyTopologyChangedAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            try
            {
                if (doc is IDisposable d) d.Dispose();
            }
            catch (ObjectDisposedException) { }
        }
    }

    private async Task PromoteSingleDocumentAsync(string documentId, string typeAlias, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken)
    {
        var compositeKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
        if (activeDocuments.ContainsKey(compositeKey))
        {
            return;
        }

        var factory = serviceProvider.GetKeyedService<IDocumentFactory>(typeAlias);
        if (factory == null)
        {
            return;
        }

        if (!shardIdCache.TryGetValue(compositeKey, out var shardId))
        {
            shardId = stringPool.GetOrAdd(factory.GetShardId(serviceProvider, documentId));
            shardIdCache.TryAdd(compositeKey, shardId);
        }

        if (!await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false))
        {
            return;
        }

        if (IsCurrentThreadOrchestratorProcessor)
        {
            await ProcessPromoteDocumentInternalAsync(documentId, typeAlias, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            var ffCmd = GetCommand();
            ffCmd.Type = OrchestratorCommandType.CreateDocument;
            ffCmd.DocumentId = documentId;
            ffCmd.TypeAlias = typeAlias;
            ffCmd.IsFireAndForget = true;
            ffCmd.CancellationToken = CancellationToken.None;

            if (commandChannel.Writer.TryWrite(ffCmd))
            {
                telemetry.RecordOrchestratorCommandEnqueued();
                return;
            }

            var pipeline = serviceProvider.GetService<IDocumentCommandPipeline>();
            if (pipeline != null && pipeline.IsCurrentThreadPipelineProcessor)
            {
                ReturnCommand(ffCmd);
                _ = ReevaluateTopologyAsync(IntentDeliveryMode.FireAndForget, CancellationToken.None);
                return;
            }

            await commandChannel.Writer.WriteAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            telemetry.RecordOrchestratorCommandEnqueued();
            return;
        }

        var cmd = GetCommand();
        cmd.Type = OrchestratorCommandType.CreateDocument;
        cmd.DocumentId = documentId;
        cmd.TypeAlias = typeAlias;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            telemetry.RecordOrchestratorCommandEnqueued();
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    private async Task ProcessSyncDocumentsInternalAsync(CancellationToken cancellationToken)
    {
        try
        {
            var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");
            var registryDoc = await registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            var registryMap = registryDoc.Data.Documents;
            bool changed = false;

            var versionVectorManager = serviceProvider.GetRequiredService<IShardVersionVectorManager>();
            versionVectorManager.EnsureShardRegistered(registry.ShardId);

            foreach (var kvp in registryMap)
            {
                var entry = kvp.Value;
                var compositeKey = CrdtRegistryEntry.GetRegistryKey(entry.TypeAlias, entry.DocumentId);

                if (!entry.IsDeleted)
                {
                    if (!activeDocuments.ContainsKey(compositeKey))
                    {
                        var factory = serviceProvider.GetKeyedService<IDocumentFactory>(entry.TypeAlias);
                        if (factory != null)
                        {
                            if (!shardIdCache.TryGetValue(compositeKey, out var shardId))
                            {
                                shardId = stringPool.GetOrAdd(factory.GetShardId(serviceProvider, entry.DocumentId));
                                shardIdCache.TryAdd(compositeKey, shardId);
                            }

                            if (await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false))
                            {
                                var doc = factory.CreateDocument(serviceProvider, entry.DocumentId);
                                await doc.InitializeAsync(cancellationToken).ConfigureAwait(false);

                                if (activeDocuments.TryAdd(compositeKey, doc))
                                {
                                    changed = true;
                                    telemetry.RecordOrchestratorDocumentCreated(entry.DocumentId);
                                    logger.LogDebug("Mapped new active CRDT document: {DocumentId} ({TypeAlias})", entry.DocumentId, entry.TypeAlias);

                                    versionVectorManager.EnsureShardRegistered(doc.ShardId);
                                }
                                else
                                {
                                    try
                                    {
                                        if (doc is IDisposable d) d.Dispose();
                                    }
                                    catch (ObjectDisposedException) { }
                                }
                            }
                        }
                        else
                        {
                            logger.LogWarning("Cannot find document factory for type alias: {TypeAlias}", entry.TypeAlias);
                        }
                    }
                    else if (activeDocuments.TryGetValue(compositeKey, out var activeDoc))
                    {
                        if (!await distributionManager.IsShardLocalAsync(activeDoc.ShardId, cancellationToken).ConfigureAwait(false))
                        {
                            if (activeDocuments.TryRemove(compositeKey, out _))
                            {
                                await DemoteActiveDocumentSafelyAsync(activeDoc, cancellationToken).ConfigureAwait(false);

                                changed = true;
                                logger.LogDebug("Removed CRDT document footprint: {DocumentId} ({TypeAlias})", entry.DocumentId, entry.TypeAlias);
                            }
                        }
                    }
                }
            }

            var toDelete = new List<string>();
            var toDemote = new List<string>();

            foreach (var active in activeDocuments)
            {
                if (TryFindRegistryEntry(registryMap, active.Value.DocumentId, active.Value.TypeAlias, out var entry) && entry != null)
                {
                    if (entry.IsDeleted)
                    {
                        toDelete.Add(active.Key);
                    }
                }
                else if (!await distributionManager.IsShardLocalAsync(active.Value.ShardId, cancellationToken).ConfigureAwait(false))
                {
                    toDemote.Add(active.Key);
                }
            }

            foreach (var compositeKey in toDemote)
            {
                if (activeDocuments.TryRemove(compositeKey, out var doc))
                {
                    await DemoteActiveDocumentSafelyAsync(doc, cancellationToken).ConfigureAwait(false);

                    changed = true;
                    logger.LogDebug("Removed CRDT document footprint: {DocumentId} ({TypeAlias})", doc.DocumentId, doc.TypeAlias);
                }
            }

            foreach (var compositeKey in toDelete)
            {
                shardIdCache.TryRemove(compositeKey, out _);

                if (activeDocuments.TryRemove(compositeKey, out var doc))
                {
                    shardIdCache.TryRemove(doc.DocumentId, out _);
                    var shardId = doc.ShardId;
                    try
                    {
                        if (doc is IDisposable d) d.Dispose();
                    }
                    catch (ObjectDisposedException) { }

                    await storage.DeleteDocumentAsync(shardId, compositeKey, cancellationToken).ConfigureAwait(false);
                    if (!string.Equals(compositeKey, doc.DocumentId, StringComparison.Ordinal))
                    {
                        await storage.DeleteDocumentAsync(shardId, doc.DocumentId, cancellationToken).ConfigureAwait(false);
                    }

                    bool shardStillInUse = string.Equals(registry.ShardId, shardId, StringComparison.Ordinal);
                    if (!shardStillInUse)
                    {
                        foreach (var remainingDoc in activeDocuments.Values)
                        {
                            if (string.Equals(remainingDoc.ShardId, shardId, StringComparison.Ordinal))
                            {
                                shardStillInUse = true;
                                break;
                            }
                        }
                    }

                    if (!shardStillInUse)
                    {
                        versionVectorManager.RemoveShard(shardId);
                    }

                    changed = true;
                    telemetry.RecordOrchestratorDocumentDeleted(doc.DocumentId);
                    logger.LogDebug("Deleted active CRDT document: {DocumentId} ({TypeAlias})", doc.DocumentId, doc.TypeAlias);
                }
            }

            if (changed)
            {
                await NotifyTopologyChangedAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to synchronize documents.");
            throw;
        }
    }

    private async Task DeleteDocumentInternalAsync(string documentId, string? typeAlias, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        documentId = stringPool.GetOrAdd(documentId);
        if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            typeAlias = stringPool.GetOrAdd(typeAlias);
        }

        var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");
        var registryDoc = await registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);

        var matchingKeys = new List<KeyValuePair<string, CrdtRegistryEntry>>();
        if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            var compositeKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
            if (registryDoc.Data.Documents.TryGetValue(compositeKey, out var compositeEntry) && !compositeEntry.IsDeleted)
            {
                matchingKeys.Add(new KeyValuePair<string, CrdtRegistryEntry>(compositeKey, compositeEntry));
            }
            else if (registryDoc.Data.Documents.TryGetValue(documentId, out var legacyEntry) &&
                     !legacyEntry.IsDeleted &&
                     string.Equals(legacyEntry.TypeAlias, typeAlias, StringComparison.Ordinal))
            {
                matchingKeys.Add(new KeyValuePair<string, CrdtRegistryEntry>(documentId, legacyEntry));
            }
        }
        else
        {
            foreach (var kvp in registryDoc.Data.Documents)
            {
                if (!kvp.Value.IsDeleted &&
                    (string.Equals(kvp.Key, documentId, StringComparison.Ordinal) ||
                     string.Equals(kvp.Value.DocumentId, documentId, StringComparison.Ordinal)))
                {
                    matchingKeys.Add(kvp);
                }
            }
        }

        foreach (var match in matchingKeys)
        {
            var intent = new MapSetIntent(match.Key, match.Value with { IsDeleted = true });
            var success = await ApplyIntentAsync<CrdtRegistryState>(registry.DocumentId, intent, nameof(CrdtRegistryState.Documents), deliveryMode, cancellationToken).ConfigureAwait(false);

            if (success)
            {
                var compositeKey = CrdtRegistryEntry.GetRegistryKey(match.Value.TypeAlias, match.Value.DocumentId);
                shardIdCache.TryRemove(compositeKey, out _);
                shardIdCache.TryRemove(match.Key, out _);
                shardIdCache.TryRemove(documentId, out _);
                logger.LogDebug("Forwarded delete intent for document {DocumentId} ({TypeAlias}).", match.Value.DocumentId, match.Value.TypeAlias);
            }
            else
            {
                logger.LogWarning("Failed to forward delete intent for document {DocumentId} ({TypeAlias}).", match.Value.DocumentId, match.Value.TypeAlias);
            }
        }
    }

    private static bool TryFindRegistryEntry(
        IDictionary<string, CrdtRegistryEntry> registryMap,
        string documentId,
        string? typeAlias,
        out CrdtRegistryEntry? entry)
    {
        if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            var compositeKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
            if (registryMap.TryGetValue(compositeKey, out entry))
            {
                return true;
            }

            if (registryMap.TryGetValue(documentId, out var legacyEntry) &&
                string.Equals(legacyEntry.TypeAlias, typeAlias, StringComparison.Ordinal))
            {
                entry = legacyEntry;
                return true;
            }

            foreach (var candidate in registryMap.Values)
            {
                if (string.Equals(candidate.DocumentId, documentId, StringComparison.Ordinal) &&
                    string.Equals(candidate.TypeAlias, typeAlias, StringComparison.Ordinal))
                {
                    entry = candidate;
                    return true;
                }
            }

            entry = null;
            return false;
        }

        if (registryMap.TryGetValue(documentId, out entry))
        {
            return true;
        }

        CrdtRegistryEntry? deletedMatch = null;
        foreach (var candidate in registryMap.Values)
        {
            if (string.Equals(candidate.DocumentId, documentId, StringComparison.Ordinal))
            {
                if (!candidate.IsDeleted)
                {
                    entry = candidate;
                    return true;
                }

                deletedMatch ??= candidate;
            }
        }

        if (deletedMatch != null)
        {
            entry = deletedMatch;
            return true;
        }

        entry = null;
        return false;
    }

    private ValueTask<string> GetShardIdAsync<TState>(string documentId, CancellationToken cancellationToken) where TState : class, new()
    {
        documentId = stringPool.GetOrAdd(documentId);
        var typeAlias = GetTypeAlias<TState>();
        var cacheKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
        if (shardIdCache.TryGetValue(cacheKey, out var cachedShardId))
        {
            return new ValueTask<string>(cachedShardId);
        }

        var activeDoc = TryGetLocalDocument<TState>(documentId);
        if (activeDoc != null)
        {
            shardIdCache.TryAdd(cacheKey, activeDoc.ShardId);
            return new ValueTask<string>(activeDoc.ShardId);
        }

        var shardIdProvider = serviceProvider.GetRequiredService<IShardIdProvider>();
        var shardId = stringPool.GetOrAdd(shardIdProvider.GetShardId<TState>(documentId) ?? documentId);
        shardIdCache.TryAdd(cacheKey, shardId);
        return new ValueTask<string>(shardId);
    }

    private string? GetTypeAlias<TState>()
    {
        if (typeof(TState) == typeof(CrdtRegistryState))
        {
            return stringPool.GetOrAdd(Constants.SystemRegistryDocumentId);
        }

        return stateTypeAliases.TryGetValue(typeof(TState), out var alias) ? alias : null;
    }

    private async Task DemoteActiveDocumentSafelyAsync(IDistributedCrdtDocument doc, CancellationToken cancellationToken)
    {
        try
        {
            await doc.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to flush document {DocumentId} ({TypeAlias}) prior to shard demotion.", doc.DocumentId, doc.TypeAlias);
        }

        try
        {
            await doc.CheckpointAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to checkpoint document {DocumentId} ({TypeAlias}) prior to shard demotion.", doc.DocumentId, doc.TypeAlias);
        }

        await remoteProxyClient.PushDemotedDocumentSnapshotToTargetPeersAsync(doc, CancellationToken.None).ConfigureAwait(false);

        try
        {
            if (doc is IDisposable d)
            {
                d.Dispose();
            }
        }
        catch (ObjectDisposedException)
        {
        }
    }
}