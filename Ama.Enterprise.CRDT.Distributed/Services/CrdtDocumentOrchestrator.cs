namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Models.Serialization;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Pooling;
using Ama.CRDT.Services.Providers;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Centralized gateway that manages distributed CRDT documents and handles remote requests.
/// </summary>
public sealed class CrdtDocumentOrchestrator : ICrdtDocumentOrchestrator, IDisposable
{
    private static readonly AsyncLocal<CrdtDocumentOrchestrator?> CurrentProcessingOrchestrator = new();

    private readonly IServiceProvider serviceProvider;
    private readonly IDistributedCrdtStorage storage;
    private readonly IAsyncCrdtPatcher patcher;
    private readonly IShardDistributionManager distributionManager;
    private readonly ICrdtOrchestratorObserver[] orchestratorObservers;
    private readonly ILogger<CrdtDocumentOrchestrator> logger;
    private readonly IDistributedCrdtTelemetry telemetry;
    private readonly ICrdtStringPool stringPool;
    private readonly IDisposable telemetryRegistration;
    private readonly DistributedCrdtOptions options;
    
    private readonly ConcurrentDictionary<string, IDistributedCrdtDocument> activeDocuments = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string> shardIdCache = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Type, string> stateTypeAliases = new();
    private readonly ConcurrentDictionary<string, byte> activeAntiEntropyShards = new(StringComparer.Ordinal);
    
    private readonly Channel<PooledOrchestratorCommand> commandChannel;
    private readonly ConcurrentQueue<PooledOrchestratorCommand> commandPool = new();
    private readonly CancellationTokenSource disposeCts = new();
    private readonly Task processingTask;
    private int syncPending;
    private int activeOrchestratorCommandCount;
    private int activeAntiEntropyCount;
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
    public bool IsAntiEntropyBusy => Volatile.Read(ref activeAntiEntropyCount) > 0 || !activeAntiEntropyShards.IsEmpty;

    /// <inheritdoc />
    public OrchestratorInitializationStage InitializationStage => (OrchestratorInitializationStage)Volatile.Read(ref initializationStage);
    
    public IDistributedCrdtDocument<CrdtRegistryState> Registry { get; private set; } = null!;

    public CrdtDocumentOrchestrator(
        IServiceProvider serviceProvider,
        IDistributedCrdtStorage storage,
        IAsyncCrdtPatcher patcher,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtDocumentOrchestrator> logger,
        ICrdtTimestampProvider timestampProvider,
        IShardDistributionManager distributionManager,
        IEnumerable<ICrdtOrchestratorObserver> orchestratorObservers,
        IEnumerable<IDocumentPatchObserver> patchObservers,
        IDistributedCrdtTelemetry telemetry,
        ICrdtStringPool? stringPool = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));
        this.distributionManager = distributionManager ?? throw new ArgumentNullException(nameof(distributionManager));
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
    
    public Task ReevaluateTopologyAsync(CancellationToken cancellationToken = default)
    {
        return ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, cancellationToken);
    }

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

    public IReadOnlyList<IDistributedCrdtDocument> GetActiveDocuments()
    {
        var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");
        var docs = new List<IDistributedCrdtDocument>(activeDocuments.Count + 1) { registry };
        
        docs.AddRange(activeDocuments.Values);
        return docs;
    }

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
        
        var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();
        var clusterStateTracker = serviceProvider.GetRequiredService<IClusterStateTracker>();
        var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
        var rpcCorrelator = serviceProvider.GetRequiredService<IP2pRpcCorrelator>();
        var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();

        var candidateReplicas = await GetCandidateRemoteReplicasForShardAsync(shardId, replicaContext.ReplicaId, clusterStateTracker, cancellationToken).ConfigureAwait(false);
        if (candidateReplicas.Count == 0)
        {
            var noNodesEx = new InvalidOperationException($"Cannot read state: no active nodes found for shard {shardId}.");
            logger.LogError(noNodesEx, "Cannot read state for document {DocumentId}: no active nodes found for shard {ShardId}.", documentId, shardId);
            throw noNodesEx;
        }

        var maxRetries = Math.Max(1, options.RpcMaxRetries);
        var rpcTimeout = TimeSpan.FromMilliseconds(Math.Max(1, options.RpcTimeoutMilliseconds));
        var retryDelayMs = Math.Max(0, options.RpcRetryDelayMilliseconds);
        Exception? lastException = null;

        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            if (attempt > 0)
            {
                candidateReplicas = await GetCandidateRemoteReplicasForShardAsync(shardId, replicaContext.ReplicaId, clusterStateTracker, cancellationToken).ConfigureAwait(false);
                if (candidateReplicas.Count == 0)
                {
                    break;
                }
            }

            bool triedAnyPeer = false;

            for (int i = 0; i < candidateReplicas.Count; i++)
            {
                var candidateReplica = candidateReplicas[(i + attempt) % candidateReplicas.Count];
                if (string.Equals(candidateReplica, replicaContext.ReplicaId, StringComparison.Ordinal))
                {
                    continue;
                }

                var candidatePeer = clusterStateTracker.GetPeerIdByReplicaId(candidateReplica);
                if (string.IsNullOrWhiteSpace(candidatePeer))
                {
                    continue;
                }

                triedAnyPeer = true;
                var correlationId = Guid.NewGuid();
                var req = new CrdtReadRequestDto
                {
                    CorrelationId = correlationId,
                    DocumentId = documentId,
                    DocumentTypeAlias = documentTypeAlias ?? string.Empty
                };

                var reqPayload = serializer.SerializeToBytes(req);
                var wrapper = new CrdtMessageWrapper(documentId, "CrdtReadReq", reqPayload, documentTypeAlias);
                var wrapperBytes = serializer.SerializeToBytes(wrapper);

                logger.LogTrace("Requesting remote state for document {DocumentId} from replica {TargetReplica} (peer {TargetPeer}), attempt {Attempt}.", documentId, candidateReplica, candidatePeer, attempt + 1);

                try
                {
                    var waitTask = rpcCorrelator.RegisterRequestAsync<CrdtReadResponseDto>(correlationId, rpcTimeout, cancellationToken);
                    await directSender.SendDirectAsync(new PeerId(Guid.Parse(candidatePeer)), wrapperBytes, cancellationToken).ConfigureAwait(false);
                    var response = await waitTask.ConfigureAwait(false);

                    if (response.StatePayload == null || response.StatePayload.Length == 0)
                    {
                        var emptyEx = new InvalidOperationException($"Remote node {candidateReplica} failed to provide state for document {documentId}.");
                        logger.LogWarning(emptyEx, "Remote node {TargetReplica} returned empty state for document {DocumentId}.", candidateReplica, documentId);
                        lastException = emptyEx;
                        continue;
                    }

                    return serializer.DeserializeFromBytes<CrdtDocument<TState>>(response.StatePayload);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    logger.LogWarning(ex, "Read state attempt {Attempt} from replica {TargetReplica} failed for document {DocumentId}.", attempt + 1, candidateReplica, documentId);
                }
            }

            if (!triedAnyPeer)
            {
                var noPeerEx = new InvalidOperationException($"Cannot read state: no reachable network peer found for shard {shardId}.");
                logger.LogError(noPeerEx, "Cannot read state for document {DocumentId}: no reachable network peer found for shard {ShardId}.", documentId, shardId);
                throw noPeerEx;
            }

            if (attempt < maxRetries - 1 && retryDelayMs > 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(retryDelayMs * (attempt + 1)), cancellationToken).ConfigureAwait(false);
            }
        }

        var finalException = lastException ?? new TimeoutException($"Timed out waiting for remote state of document {documentId} on shard {shardId}.");
        logger.LogError(finalException, "Failed to read remote state for document {DocumentId} on shard {ShardId} after {MaxRetries} attempts.", documentId, shardId, maxRetries);
        throw finalException;
    }

    public Task<bool> ApplyIntentAsync<TState>(string documentId, IOperationIntent intent, string targetProperty, CancellationToken cancellationToken = default) where TState : class, new()
    {
        return ApplyIntentAsync<TState>(documentId, intent, targetProperty, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

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
        
        var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();
        var clusterStateTracker = serviceProvider.GetRequiredService<IClusterStateTracker>();
        var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
        var rpcCorrelator = serviceProvider.GetRequiredService<IP2pRpcCorrelator>();
        var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();

        var activeNodes = await distributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
        if (activeNodes == null || activeNodes.Count == 0)
        {
            logger.LogWarning("Cannot forward intent: no active nodes found for shard {ShardId}.", shardId);
            telemetry.RecordOrchestratorIntentFailed(documentId);
            return false;
        }

        var intentTypeAlias = CrdtTypeRegistry.GetDiscriminator(intent.GetType());
        var serializedIntent = serializer.SerializeToBytes(intent);

        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            for (int i = 0; i < activeNodes.Count; i++)
            {
                var candidateReplica = activeNodes[i];
                if (string.Equals(candidateReplica, replicaContext.ReplicaId, StringComparison.Ordinal))
                {
                    continue;
                }

                var candidatePeer = clusterStateTracker.GetPeerIdByReplicaId(candidateReplica);
                if (string.IsNullOrWhiteSpace(candidatePeer))
                {
                    continue;
                }

                var ffReq = new CrdtIntentForwardRequestDto
                {
                    CorrelationId = Guid.Empty,
                    DocumentId = documentId,
                    SerializedIntent = serializedIntent,
                    IntentTypeAlias = intentTypeAlias,
                    TargetProperty = targetProperty,
                    DocumentTypeAlias = documentTypeAlias ?? string.Empty
                };

                var ffReqPayload = serializer.SerializeToBytes(ffReq);
                var ffWrapper = new CrdtMessageWrapper(documentId, "CrdtIntentReq", ffReqPayload, documentTypeAlias);
                var ffWrapperBytes = serializer.SerializeToBytes(ffWrapper);

                logger.LogTrace("Forwarding fire-and-forget intent for document {DocumentId} to remote replica {TargetReplica} (peer {TargetPeer}).", documentId, candidateReplica, candidatePeer);
                telemetry.RecordOrchestratorIntentForwarded(documentId);

                try
                {
                    await directSender.SendDirectAsync(new PeerId(Guid.Parse(candidatePeer)), ffWrapperBytes, cancellationToken).ConfigureAwait(false);
                    return true;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Fire-and-forget intent forwarding to replica {TargetReplica} failed for document {DocumentId}.", candidateReplica, documentId);
                }
            }

            logger.LogWarning("Cannot forward fire-and-forget intent: no reachable network peer found for shard {ShardId}.", shardId);
            telemetry.RecordOrchestratorIntentFailed(documentId);
            return false;
        }

        var maxRetries = Math.Max(1, options.RpcMaxRetries);
        var rpcTimeout = TimeSpan.FromMilliseconds(Math.Max(1, options.RpcTimeoutMilliseconds));
        var retryDelayMs = Math.Max(0, options.RpcRetryDelayMilliseconds);
        Exception? lastException = null;

        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            if (attempt > 0)
            {
                activeNodes = await distributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
                if (activeNodes == null || activeNodes.Count == 0)
                {
                    break;
                }
            }

            bool triedAnyPeer = false;

            for (int i = 0; i < activeNodes.Count; i++)
            {
                var candidateReplica = activeNodes[(i + attempt) % activeNodes.Count];
                if (string.Equals(candidateReplica, replicaContext.ReplicaId, StringComparison.Ordinal))
                {
                    continue;
                }

                var candidatePeer = clusterStateTracker.GetPeerIdByReplicaId(candidateReplica);
                if (string.IsNullOrWhiteSpace(candidatePeer))
                {
                    continue;
                }

                triedAnyPeer = true;
                var correlationId = Guid.NewGuid();
                var req = new CrdtIntentForwardRequestDto
                {
                    CorrelationId = correlationId,
                    DocumentId = documentId,
                    SerializedIntent = serializedIntent,
                    IntentTypeAlias = intentTypeAlias,
                    TargetProperty = targetProperty,
                    DocumentTypeAlias = documentTypeAlias ?? string.Empty
                };

                var reqPayload = serializer.SerializeToBytes(req);
                var wrapper = new CrdtMessageWrapper(documentId, "CrdtIntentReq", reqPayload, documentTypeAlias);
                var wrapperBytes = serializer.SerializeToBytes(wrapper);

                logger.LogTrace("Forwarding intent for document {DocumentId} to remote replica {TargetReplica} (peer {TargetPeer}), attempt {Attempt}.", documentId, candidateReplica, candidatePeer, attempt + 1);
                telemetry.RecordOrchestratorIntentForwarded(documentId);

                try
                {
                    var waitTask = rpcCorrelator.RegisterRequestAsync<CrdtIntentForwardResponseDto>(correlationId, rpcTimeout, cancellationToken);
                    await directSender.SendDirectAsync(new PeerId(Guid.Parse(candidatePeer)), wrapperBytes, cancellationToken).ConfigureAwait(false);
                    var response = await waitTask.ConfigureAwait(false);

                    if (response.IsSuccess)
                    {
                        return true;
                    }

                    logger.LogWarning("Remote replica {TargetReplica} failed to apply intent: {ErrorMessage}", candidateReplica, response.ErrorMessage);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    logger.LogWarning(ex, "Intent forwarding attempt {Attempt} to replica {TargetReplica} failed for document {DocumentId}.", attempt + 1, candidateReplica, documentId);
                }
            }

            if (!triedAnyPeer)
            {
                logger.LogWarning("Cannot forward intent: no reachable network peer found for shard {ShardId}.", shardId);
                telemetry.RecordOrchestratorIntentFailed(documentId);
                return false;
            }

            if (attempt < maxRetries - 1 && retryDelayMs > 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(retryDelayMs * (attempt + 1)), cancellationToken).ConfigureAwait(false);
            }
        }

        if (lastException != null)
        {
            logger.LogError(lastException, "Failed to forward intent for document {DocumentId} on shard {ShardId} after {MaxRetries} attempts.", documentId, shardId, maxRetries);
        }
        else
        {
            logger.LogError("Failed to forward intent for document {DocumentId} on shard {ShardId} after {MaxRetries} attempts.", documentId, shardId, maxRetries);
        }

        telemetry.RecordOrchestratorIntentFailed(documentId);
        return false;
    }

    public Task ApplyPatchAsync<TState>(string documentId, CrdtPatch patch, CancellationToken cancellationToken = default) where TState : class, new()
    {
        return ApplyPatchAsync<TState>(documentId, patch, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

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
        
        var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();
        var clusterStateTracker = serviceProvider.GetRequiredService<IClusterStateTracker>();
        var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
        var p2pAlgorithm = serviceProvider.GetService<IP2pAlgorithm>();
        var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();

        var patchMsg = new CrdtPatchMessage(replicaContext.ReplicaId, patch);
        var payload = serializer.SerializeToBytes(patchMsg);
        var wrapper = new CrdtMessageWrapper(documentId, "CrdtPatch", payload, documentTypeAlias);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        var activeNodes = await distributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
        bool sentToTarget = false;

        if (activeNodes != null && activeNodes.Count > 0)
        {
            foreach (var targetReplicaId in activeNodes)
            {
                if (string.Equals(targetReplicaId, replicaContext.ReplicaId, StringComparison.Ordinal))
                {
                    continue;
                }

                var targetPeerId = clusterStateTracker.GetPeerIdByReplicaId(targetReplicaId);
                if (!string.IsNullOrWhiteSpace(targetPeerId))
                {
                    logger.LogTrace("Forwarding patch for document {DocumentId} to remote replica {TargetReplica} (peer {TargetPeer}).", documentId, targetReplicaId, targetPeerId);
                    telemetry.RecordOrchestratorPatchForwarded(documentId);
                    await directSender.SendDirectAsync(new PeerId(Guid.Parse(targetPeerId)), wrapperBytes, cancellationToken).ConfigureAwait(false);
                    sentToTarget = true;
                }
            }
        }

        if (p2pAlgorithm != null)
        {
            await p2pAlgorithm.BroadcastAsync(wrapperBytes, cancellationToken).ConfigureAwait(false);
            sentToTarget = true;
        }

        if (!sentToTarget)
        {
            logger.LogWarning("Cannot forward patch: no active nodes found for shard {ShardId}.", shardId);
        }
    }

    public Task CreateDocumentAsync(string documentId, string typeAlias, CancellationToken cancellationToken = default)
    {
        return CreateDocumentAsync(documentId, typeAlias, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

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

    public Task DeleteDocumentAsync(string documentId, CancellationToken cancellationToken = default)
    {
        return DeleteDocumentAsync(documentId, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    public Task DeleteDocumentAsync(string documentId, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        return DeleteDocumentInternalAsync(documentId, null, deliveryMode, cancellationToken);
    }

    public Task DeleteDocumentAsync(string documentId, string typeAlias, CancellationToken cancellationToken = default)
    {
        return DeleteDocumentAsync(documentId, typeAlias, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    public Task DeleteDocumentAsync(string documentId, string typeAlias, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(typeAlias)) throw new ArgumentException("Type alias cannot be null or empty.", nameof(typeAlias));
        return DeleteDocumentInternalAsync(documentId, typeAlias, deliveryMode, cancellationToken);
    }

    public Task ProvideSnapshotAsync(string documentId, string targetReplicaId, PeerId targetPeerId, CancellationToken cancellationToken = default)
    {
        return ProvideSnapshotAsync(documentId, null, targetReplicaId, targetPeerId, null, null, cancellationToken);
    }

    public Task ProvideSnapshotAsync(string documentId, string? typeAlias, string targetReplicaId, PeerId targetPeerId, CancellationToken cancellationToken = default)
    {
        return ProvideSnapshotAsync(documentId, typeAlias, targetReplicaId, targetPeerId, null, null, cancellationToken);
    }

    public async Task ProvideSnapshotAsync(
        string documentId,
        string? typeAlias,
        string targetReplicaId,
        PeerId targetPeerId,
        IReadOnlyList<string>? shardDocumentIds,
        ShardVersionVectorMap? consistentGlobalState = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        documentId = stringPool.GetOrAdd(documentId);
        if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            typeAlias = stringPool.GetOrAdd(typeAlias);
        }

        var topologyProvider = serviceProvider.GetRequiredService<IScopeTopologyProvider>();
        var isExpected = await topologyProvider.IsPeerExpectedAsync(targetPeerId.Value.ToString(), cancellationToken).ConfigureAwait(false);
        if (!isExpected)
        {
            logger.LogWarning("Rejected snapshot request for document {DocumentId} from excluded peer {PeerId}.", documentId, targetPeerId.Value);
            return;
        }

        var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");

        IDistributedCrdtDocument? targetDoc = null;
        if (string.Equals(documentId, registry.DocumentId, StringComparison.Ordinal))
        {
            targetDoc = registry;
        }
        else if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            var compositeKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
            activeDocuments.TryGetValue(compositeKey, out targetDoc);
        }
        else if (!activeDocuments.TryGetValue(documentId, out targetDoc))
        {
            foreach (var doc in activeDocuments.Values)
            {
                if (string.Equals(doc.DocumentId, documentId, StringComparison.Ordinal))
                {
                    targetDoc = doc;
                    break;
                }
            }
        }

        try
        {
            byte[] snapshotData;
            ShardVersionVectorMap globalState;
            string? documentTypeAlias = typeAlias;

            if (targetDoc != null)
            {
                var snapshotResult = await targetDoc.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
                snapshotData = snapshotResult.SnapshotData;
                globalState = consistentGlobalState ?? snapshotResult.GlobalState;
                if (string.IsNullOrWhiteSpace(documentTypeAlias))
                {
                    documentTypeAlias = targetDoc.TypeAlias;
                }
            }
            else
            {
                snapshotData = await TryLoadStoredDocumentSnapshotBytesAsync(documentId, typeAlias, cancellationToken).ConfigureAwait(false);
                var shardId = await TryGetShardIdAsync(documentId, typeAlias, cancellationToken).ConfigureAwait(false);

                if (snapshotData.Length == 0 && !string.IsNullOrWhiteSpace(shardId) && await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false))
                {
                    targetDoc = await ResolveOrPromoteLocalDocumentAsync(documentId, typeAlias, cancellationToken).ConfigureAwait(false);
                    if (targetDoc != null)
                    {
                        var snapshotResult = await targetDoc.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
                        snapshotData = snapshotResult.SnapshotData;
                        globalState = consistentGlobalState ?? snapshotResult.GlobalState;
                        if (string.IsNullOrWhiteSpace(documentTypeAlias))
                        {
                            documentTypeAlias = targetDoc.TypeAlias;
                        }
                    }
                    else
                    {
                        globalState = consistentGlobalState ?? new ShardVersionVectorMap();
                    }
                }
                else
                {
                    var versionVectorManager = serviceProvider.GetRequiredService<IShardVersionVectorManager>();
                    globalState = consistentGlobalState ?? (!string.IsNullOrWhiteSpace(shardId)
                        ? versionVectorManager.GetShardSnapshot(shardId)
                        : new ShardVersionVectorMap());
                }

                if (snapshotData.Length == 0)
                {
                    logger.LogWarning("Requested snapshot for unknown document {DocumentId} ({TypeAlias}).", documentId, typeAlias);
                    return;
                }
            }

            var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();

            IList<string>? batchDocIds = shardDocumentIds != null
                ? (shardDocumentIds as IList<string> ?? new List<string>(shardDocumentIds))
                : null;

            var resMsg = new CrdtSnapshotMessage(replicaContext.ReplicaId, snapshotData, globalState, batchDocIds);
            var payload = serializer.SerializeToBytes(resMsg);

            if (string.IsNullOrWhiteSpace(documentTypeAlias) && Registry != null && !string.Equals(documentId, Registry.DocumentId, StringComparison.Ordinal))
            {
                var registryDoc = await Registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
                if (TryFindRegistryEntry(registryDoc.Data.Documents, documentId, typeAlias, out var entry) && entry != null)
                {
                    documentTypeAlias = entry.TypeAlias;
                }
            }

            var wrapper = new CrdtMessageWrapper(documentId, "CrdtSnapshot", payload, documentTypeAlias);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            await directSender.SendDirectAsync(targetPeerId, finalBytes, cancellationToken).ConfigureAwait(false); 
            
            telemetry.RecordOrchestratorSnapshotDispatched(documentId, finalBytes.Length);
            logger.LogInformation("Sent full snapshot of document {DocumentId} ({TypeAlias}) to peer {PeerId}.", documentId, documentTypeAlias, targetPeerId.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to send snapshot for document {DocumentId} ({TypeAlias}).", documentId, typeAlias);
        }
    }

    /// <inheritdoc />
    public async Task<CrdtAntiEntropyWorkLoadDto> EvaluateAndDispatchAntiEntropyStateAsync(CancellationToken cancellationToken = default)
    {
        var commandPipeline = serviceProvider.GetService<IDocumentCommandPipeline>();
        var versionVectorManager = serviceProvider.GetService<IShardVersionVectorManager>();
        var inboundNetworkDepth = GetPendingInboundNetworkQueueDepth();
        var queueDepth = PendingCommandCount + (commandPipeline?.PendingCommandCount ?? 0) + inboundNetworkDepth;
        var busyThreshold = options.AntiEntropyBusyQueueThreshold;
        var isPipelineOverloaded = (busyThreshold > 0 && queueDepth >= busyThreshold) || inboundNetworkDepth > 0;

        var localShards = await GetLocalAntiEntropyShardsAsync(versionVectorManager, cancellationToken).ConfigureAwait(false);
        var snapshotActivityWindowMs = Math.Max(2000L, options.AntiEntropyStartupIntervalSeconds * 1500L);
        var totalRemainingSnapshotDocs = 0;
        var availableShards = new HashSet<string>(StringComparer.Ordinal);

        foreach (var shardId in localShards)
        {
            var isShardReceivingSnapshots = false;
            if (versionVectorManager != null &&
                versionVectorManager.TryGetShardSnapshotReceptionState(shardId, snapshotActivityWindowMs, out var shardRemainingDocs))
            {
                isShardReceivingSnapshots = true;
                totalRemainingSnapshotDocs += shardRemainingDocs;
            }

            if (!isShardReceivingSnapshots && !activeAntiEntropyShards.ContainsKey(shardId))
            {
                availableShards.Add(shardId);
            }
        }

        if (versionVectorManager != null &&
            versionVectorManager.TryGetActiveSnapshotReceptionState(snapshotActivityWindowMs, out var globalRemainingDocs) &&
            globalRemainingDocs > totalRemainingSnapshotDocs)
        {
            totalRemainingSnapshotDocs = globalRemainingDocs;
        }

        var isGlobalLockHeld = Volatile.Read(ref activeAntiEntropyCount) > 0;
        var hasAvailableShards = localShards.Count == 0
            ? (!isGlobalLockHeld && totalRemainingSnapshotDocs == 0)
            : (availableShards.Count > 0 && !isGlobalLockHeld);

        var isAlreadyWorking = isPipelineOverloaded || !hasAvailableShards;

        var estimatedDelta = await EstimatePendingAntiEntropyItemsAsync(cancellationToken).ConfigureAwait(false);
        if (totalRemainingSnapshotDocs > estimatedDelta)
        {
            estimatedDelta = totalRemainingSnapshotDocs;
        }

        if (isAlreadyWorking)
        {
            logger.LogDebug(
                "Skipping anti-entropy state dispatch because all local shards or queues are currently busy (QueueDepth: {QueueDepth}, InboundNetworkDepth: {InboundNetworkDepth}, BusyShards: {BusyShardCount}/{TotalShardCount}, RemainingSnapshotDocs: {RemainingSnapshotDocs}, EstimatedPendingItems: {EstimatedItems}).",
                queueDepth,
                inboundNetworkDepth,
                localShards.Count - availableShards.Count,
                localShards.Count,
                totalRemainingSnapshotDocs,
                estimatedDelta);

            return new CrdtAntiEntropyWorkLoadDto(
                isBusy: true,
                pendingQueueDepth: Math.Max(queueDepth, totalRemainingSnapshotDocs),
                estimatedPendingSyncItems: estimatedDelta);
        }

        var acquiredShards = new List<string>(availableShards.Count);
        foreach (var shardId in availableShards)
        {
            if (activeAntiEntropyShards.TryAdd(shardId, 1))
            {
                acquiredShards.Add(shardId);
            }
        }

        if (localShards.Count > 0 && acquiredShards.Count == 0)
        {
            return new CrdtAntiEntropyWorkLoadDto(
                isBusy: true,
                pendingQueueDepth: Math.Max(queueDepth, totalRemainingSnapshotDocs),
                estimatedPendingSyncItems: estimatedDelta);
        }

        try
        {
            var shardsFilter = acquiredShards.Count > 0 ? new HashSet<string>(acquiredShards, StringComparer.Ordinal) : null;
            await DispatchAntiEntropyStateCoreAsync(shardsFilter, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            for (int i = 0; i < acquiredShards.Count; i++)
            {
                activeAntiEntropyShards.TryRemove(acquiredShards[i], out _);
            }
        }

        var postQueueDepth = PendingCommandCount + (commandPipeline?.PendingCommandCount ?? 0) + GetPendingInboundNetworkQueueDepth();
        return new CrdtAntiEntropyWorkLoadDto(
            isBusy: false,
            pendingQueueDepth: Math.Max(queueDepth, postQueueDepth),
            estimatedPendingSyncItems: estimatedDelta);
    }

    public Task DispatchAntiEntropyStateAsync(CancellationToken cancellationToken = default)
    {
        return DispatchAntiEntropyStateCoreAsync(null, cancellationToken);
    }

    private async Task DispatchAntiEntropyStateCoreAsync(IReadOnlySet<string>? allowedShards, CancellationToken cancellationToken = default)
    {
        try
        {
            var topologyProvider = serviceProvider.GetRequiredService<IScopeTopologyProvider>();
            var peerRegistry = serviceProvider.GetRequiredService<IPeerRegistry>();
            var meshes = serviceProvider.GetServices<P2pMeshMetadata>();
            
            var validPeers = new List<PeerId>();
            foreach (var mesh in meshes)
            {
                var activePeers = await peerRegistry.GetPeersByStatusAsync(mesh.MeshId, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
                foreach (var peer in activePeers)
                {
                    var networkIdStr = peer.Id.Value.ToString();
                    var isExpected = await topologyProvider.IsPeerExpectedAsync(networkIdStr, cancellationToken).ConfigureAwait(false);
                    if (isExpected)
                    {
                        validPeers.Add(peer.Id);
                    }
                }
            }

            if (validPeers.Count == 0)
            {
                return;
            }

            var targetPeerId = validPeers[Random.Shared.Next(validPeers.Count)];
            await DispatchAntiEntropyStateAsync(targetPeerId, allowedShards, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to dispatch state sync to peer.");
        }
    }

    public Task DispatchAntiEntropyStateAsync(PeerId targetPeerId, CancellationToken cancellationToken = default)
    {
        return DispatchAntiEntropyStateAsync(targetPeerId, null, cancellationToken);
    }

    private async Task DispatchAntiEntropyStateAsync(PeerId targetPeerId, IReadOnlySet<string>? allowedShards, CancellationToken cancellationToken = default)
    {
        if (targetPeerId.Value == Guid.Empty) return;

        try
        {
            var topologyProvider = serviceProvider.GetRequiredService<IScopeTopologyProvider>();
            var isExpected = await topologyProvider.IsPeerExpectedAsync(targetPeerId.Value.ToString(), cancellationToken).ConfigureAwait(false);
            if (!isExpected)
            {
                return;
            }

            var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
            var versionVectorManager = serviceProvider.GetRequiredService<IShardVersionVectorManager>();
            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();

            HashSet<string> localShards;
            if (allowedShards != null)
            {
                localShards = new HashSet<string>(allowedShards, StringComparer.Ordinal);
            }
            else
            {
                localShards = await GetLocalAntiEntropyShardsAsync(versionVectorManager, cancellationToken).ConfigureAwait(false);
            }

            var globalState = await versionVectorManager.GetShardsSnapshotAsync(localShards, cancellationToken).ConfigureAwait(false);

            var syncMsg = new CrdtStateSyncMessage(replicaContext.ReplicaId, globalState, IsTopologyAnnouncementOnly: false);
            var payload = serializer.SerializeToBytes(syncMsg);
            
            var wrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", payload);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            await directSender.SendDirectAsync(targetPeerId, finalBytes, cancellationToken).ConfigureAwait(false);
            
            telemetry.RecordOrchestratorAntiEntropySync(replicaContext.ReplicaId);
            logger.LogTrace("Sent full state sync to peer {PeerId}.", targetPeerId.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to dispatch state sync to peer {PeerId}.", targetPeerId.Value);
        }
    }

    /// <inheritdoc />
    public async Task DispatchTopologyAnnouncementAsync(PeerId targetPeerId, CancellationToken cancellationToken = default)
    {
        if (targetPeerId.Value == Guid.Empty) return;

        try
        {
            var topologyProvider = serviceProvider.GetRequiredService<IScopeTopologyProvider>();
            var isExpected = await topologyProvider.IsPeerExpectedAsync(targetPeerId.Value.ToString(), cancellationToken).ConfigureAwait(false);
            if (!isExpected)
            {
                return;
            }

            var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
            var versionVectorManager = serviceProvider.GetRequiredService<IShardVersionVectorManager>();
            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();

            var announcementShards = new HashSet<string>(StringComparer.Ordinal);
            if (Registry != null)
            {
                versionVectorManager.EnsureShardRegistered(Registry.ShardId);
                announcementShards.Add(Registry.ShardId);
            }

            var registryState = announcementShards.Count > 0
                ? await versionVectorManager.GetShardsSnapshotAsync(announcementShards, cancellationToken).ConfigureAwait(false)
                : new ShardVersionVectorMap();

            var syncMsg = new CrdtStateSyncMessage(replicaContext.ReplicaId, registryState, IsTopologyAnnouncementOnly: true);
            var payload = serializer.SerializeToBytes(syncMsg);

            var wrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", payload);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            await directSender.SendDirectAsync(targetPeerId, finalBytes, cancellationToken).ConfigureAwait(false);

            telemetry.RecordOrchestratorAntiEntropySync(replicaContext.ReplicaId);
            logger.LogTrace("Sent topology announcement and registry state sync to peer {PeerId}.", targetPeerId.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to dispatch topology announcement to peer {PeerId}.", targetPeerId.Value);
        }
    }

    public IDisposable? TryAcquireAntiEntropyWorkScope()
    {
        var commandPipeline = serviceProvider.GetService<IDocumentCommandPipeline>();
        var queueDepth = PendingCommandCount + (commandPipeline?.PendingCommandCount ?? 0) + GetPendingInboundNetworkQueueDepth();
        var busyThreshold = options.AntiEntropyBusyQueueThreshold;
        if (busyThreshold > 0 && queueDepth >= busyThreshold)
        {
            return null;
        }

        if (Interlocked.CompareExchange(ref activeAntiEntropyCount, 1, 0) != 0 || !activeAntiEntropyShards.IsEmpty)
        {
            return null;
        }

        return new AntiEntropyWorkScope(this);
    }

    public IDisposable? TryAcquireAntiEntropyWorkScope(string shardId)
    {
        if (string.IsNullOrWhiteSpace(shardId))
        {
            return TryAcquireAntiEntropyWorkScope();
        }

        var commandPipeline = serviceProvider.GetService<IDocumentCommandPipeline>();
        var queueDepth = PendingCommandCount + (commandPipeline?.PendingCommandCount ?? 0) + GetPendingInboundNetworkQueueDepth();
        var busyThreshold = options.AntiEntropyBusyQueueThreshold;
        if (busyThreshold > 0 && queueDepth >= busyThreshold)
        {
            return null;
        }

        if (Volatile.Read(ref activeAntiEntropyCount) > 0)
        {
            return null;
        }

        shardId = stringPool.GetOrAdd(shardId);
        if (!activeAntiEntropyShards.TryAdd(shardId, 1))
        {
            return null;
        }

        if (Volatile.Read(ref activeAntiEntropyCount) > 0)
        {
            activeAntiEntropyShards.TryRemove(shardId, out _);
            return null;
        }

        return new ShardAntiEntropyWorkScope(this, shardId);
    }

    private int GetPendingInboundNetworkQueueDepth()
    {
        try
        {
            var total = 0;
            var meshes = serviceProvider.GetServices<P2pMeshMetadata>();
            foreach (var mesh in meshes)
            {
                var inboundQueue = serviceProvider.GetKeyedService<IInboundMessageQueue<IMeshMessage>>(mesh.MeshId);
                if (inboundQueue != null)
                {
                    total += inboundQueue.PendingCount;
                }
            }

            return total;
        }
        catch
        {
            return 0;
        }
    }

    private async ValueTask<HashSet<string>> GetLocalAntiEntropyShardsAsync(IShardVersionVectorManager? versionVectorManager, CancellationToken cancellationToken)
    {
        var activeShardIds = new HashSet<string>(StringComparer.Ordinal);
        if (Registry != null)
        {
            activeShardIds.Add(Registry.ShardId);
        }

        foreach (var doc in activeDocuments.Values)
        {
            activeShardIds.Add(doc.ShardId);
        }

        IReadOnlySet<string> knownShardIds = versionVectorManager != null
            ? versionVectorManager.EnsureShardsRegisteredAndGetAll(activeShardIds)
            : activeShardIds;

        var localShards = new HashSet<string>(StringComparer.Ordinal);
        if (Registry != null)
        {
            localShards.Add(Registry.ShardId);
        }

        foreach (var shardId in knownShardIds)
        {
            if (await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false))
            {
                localShards.Add(shardId);
            }
        }

        return localShards;
    }

    private async ValueTask<long> EstimatePendingAntiEntropyItemsAsync(CancellationToken cancellationToken)
    {
        try
        {
            var versionVectorManager = serviceProvider.GetService<IShardVersionVectorManager>();
            var clusterTracker = serviceProvider.GetService<IClusterStateTracker>();
            if (versionVectorManager == null || clusterTracker == null)
            {
                return 0;
            }

            var localState = await versionVectorManager.GetFullSnapshotAsync(cancellationToken).ConfigureAwait(false);
            var peerStates = clusterTracker.GetClusterStates();
            if (peerStates.Count == 0 || localState.Count == 0)
            {
                return 0;
            }

            long maxEstimatedGap = 0;

            for (int i = 0; i < peerStates.Count; i++)
            {
                var peerState = peerStates[i];
                long peerGap = 0;

                foreach (var localShardKvp in localState)
                {
                    var shardId = localShardKvp.Key;
                    var localDvv = localShardKvp.Value;
                    peerState.TryGetValue(shardId, out var peerDvv);

                    if (localDvv.Versions != null)
                    {
                        foreach (var verKvp in localDvv.Versions)
                        {
                            long peerVer = 0;
                            if (peerDvv?.Versions != null)
                            {
                                peerDvv.Versions.TryGetValue(verKvp.Key, out peerVer);
                            }

                            if (verKvp.Value != peerVer)
                            {
                                peerGap += Math.Abs(verKvp.Value - peerVer);
                            }
                        }
                    }

                    if (peerDvv?.Versions != null)
                    {
                        foreach (var peerVerKvp in peerDvv.Versions)
                        {
                            if (localDvv.Versions == null || !localDvv.Versions.ContainsKey(peerVerKvp.Key))
                            {
                                peerGap += Math.Max(0, peerVerKvp.Value);
                            }
                        }
                    }
                }

                if (peerGap > maxEstimatedGap)
                {
                    maxEstimatedGap = peerGap;
                }
            }

            return maxEstimatedGap;
        }
        catch
        {
            return 0;
        }
    }

    private sealed class AntiEntropyWorkScope(CrdtDocumentOrchestrator owner) : IDisposable
    {
        private int disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
            {
                Interlocked.Decrement(ref owner.activeAntiEntropyCount);
            }
        }
    }

    private sealed class ShardAntiEntropyWorkScope(CrdtDocumentOrchestrator owner, string shardId) : IDisposable
    {
        private int disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
            {
                owner.activeAntiEntropyShards.TryRemove(shardId, out _);
            }
        }
    }

    /// <inheritdoc />
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        // 1. Drain the document command pipeline first so any queued FireAndForget registry intents
        // (such as MapSetIntent from CreateDocumentAsync) have applied and triggered registry state observers.
        var commandPipeline = serviceProvider.GetRequiredService<IDocumentCommandPipeline>();
        await commandPipeline.FlushAsync(cancellationToken).ConfigureAwait(false);

        // 2. Drain any pending commands in the orchestrator channel, then run a final synchronous topology
        // evaluation if any FireAndForget promotions fell back to coalesced topology sync.
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

        // 3. Flush any remaining document commands and debounced active-sync patches across all active documents.
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
    public async Task<CrdtMapReduceResultDto<TResult>> ExecuteMapReduceAsync<TState, TInput, TAccumulate, TResult>(
        TInput input,
        CrdtMapReduceScopeDto scope,
        CancellationToken cancellationToken = default)
        where TState : class, new()
    {
        ArgumentNullException.ThrowIfNull(input);

        var handler = serviceProvider.GetService<ICrdtMapReduce<TState, TInput, TAccumulate, TResult>>()
            ?? throw new InvalidOperationException(
                $"No Map/Reduce or Cluster Query handler is registered for ({typeof(TState).Name}, {typeof(TInput).Name}, {typeof(TAccumulate).Name}, {typeof(TResult).Name}). " +
                "Register it using AddDistributedCrdtMapReduce or AddDistributedCrdtQuery.");

        var jobAlias = ResolveMapReduceJobAlias<TState, TInput, TAccumulate, TResult>();
        var documentTypeAlias = GetTypeAlias<TState>() ?? typeof(TState).Name;
        var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
        var clusterStateTracker = serviceProvider.GetRequiredService<IClusterStateTracker>();
        var shardIdProvider = serviceProvider.GetRequiredService<IShardIdProvider>();

        var candidateDocuments = await ResolveCandidateMapReduceDocumentsAsync<TState>(
            documentTypeAlias,
            scope,
             cancellationToken).ConfigureAwait(false);

        if (candidateDocuments.Count == 0)
        {
            var emptyCombined = handler.Combine(input, Array.Empty<TAccumulate>());
            var emptyResult = handler.Reduce(input, emptyCombined);
            telemetry.RecordOrchestratorMapReduceExecuted(jobAlias, 0);
            return new CrdtMapReduceResultDto<TResult>(
                emptyResult,
                TotalDocumentsTargeted: 0,
                LocalDocumentsEvaluated: 0,
                RemoteDocumentsEvaluated: 0,
                RemotePeersContacted: 0,
                WasShortCircuited: false,
                UnreachableDocumentIds: Array.Empty<string>());
        }

        // Plan local vs remote batches based on the requested routing mode, caching ring lookups per unique shardId
        var localDocumentBatch = new List<string>();
        var remoteBatchesByReplicaId = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var docCandidateReplicas = new Dictionary<string, List<string>>(candidateDocuments.Count, StringComparer.Ordinal);
        var shardTargetNodesCache = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var shardFallbackRemotesCache = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var replicaPeerIdCache = new Dictionary<string, string?>(StringComparer.Ordinal);
        var unreachableDocumentIds = new List<string>();

        string? GetCachedPeerId(string replicaId)
        {
            if (!replicaPeerIdCache.TryGetValue(replicaId, out var cachedPeerId))
            {
                cachedPeerId = clusterStateTracker.GetPeerIdByReplicaId(replicaId);
                replicaPeerIdCache[replicaId] = cachedPeerId;
            }

            return cachedPeerId;
        }

        async ValueTask<List<string>> GetCachedFallbackRemotesAsync(string shardId)
        {
            if (!shardFallbackRemotesCache.TryGetValue(shardId, out var cachedFallbacks))
            {
                cachedFallbacks = await GetCandidateRemoteReplicasForShardAsync(shardId, replicaContext.ReplicaId, clusterStateTracker, cancellationToken).ConfigureAwait(false);
                shardFallbackRemotesCache[shardId] = cachedFallbacks;
            }

            return cachedFallbacks;
        }

        foreach (var item in candidateDocuments)
        {
            var docId = item.DocumentId;
            var shardId = item.ShardId;

            if (!shardTargetNodesCache.TryGetValue(shardId, out var cachedShardReplicas))
            {
                var targetNodes = await distributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
                cachedShardReplicas = new List<string>(targetNodes?.Count ?? 0);
                if (targetNodes != null)
                {
                    for (var i = 0; i < targetNodes.Count; i++)
                    {
                        if (!cachedShardReplicas.Contains(targetNodes[i]))
                        {
                            cachedShardReplicas.Add(targetNodes[i]);
                        }
                    }
                }

                shardTargetNodesCache[shardId] = cachedShardReplicas;
            }

            var orderedReplicas = cachedShardReplicas;
            docCandidateReplicas[docId] = orderedReplicas;

            switch (scope.RoutingMode)
            {
                case CrdtMapReduceRoutingMode.MinimalShardCover:
                {
                    var isLoadedLocally = TryGetLocalDocument<TState>(docId) != null;
                    var isLocalReplicaInRing = orderedReplicas.Contains(replicaContext.ReplicaId);

                    if (isLoadedLocally || isLocalReplicaInRing)
                    {
                        localDocumentBatch.Add(docId);
                        break;
                    }

                    string? chosenRemote = null;
                    // Greedily pick an already-scheduled remote replica in targetNodes
                    for (var i = 0; i < orderedReplicas.Count; i++)
                    {
                        var candidate = orderedReplicas[i];
                        if (!string.Equals(candidate, replicaContext.ReplicaId, StringComparison.Ordinal) &&
                            remoteBatchesByReplicaId.ContainsKey(candidate) &&
                            !string.IsNullOrWhiteSpace(GetCachedPeerId(candidate)))
                        {
                            chosenRemote = candidate;
                            break;
                        }
                    }

                    if (chosenRemote is null)
                    {
                        for (var i = 0; i < orderedReplicas.Count; i++)
                        {
                            var candidate = orderedReplicas[i];
                            if (!string.Equals(candidate, replicaContext.ReplicaId, StringComparison.Ordinal) &&
                                !string.IsNullOrWhiteSpace(GetCachedPeerId(candidate)))
                            {
                                chosenRemote = candidate;
                                break;
                            }
                        }
                    }

                    if (chosenRemote is null)
                    {
                        var fallbackRemotes = await GetCachedFallbackRemotesAsync(shardId).ConfigureAwait(false);
                        for (var i = 0; i < fallbackRemotes.Count; i++)
                        {
                            var candidate = fallbackRemotes[i];
                            if (!orderedReplicas.Contains(candidate))
                            {
                                orderedReplicas.Add(candidate);
                            }

                            if (chosenRemote is null && !string.IsNullOrWhiteSpace(GetCachedPeerId(candidate)))
                            {
                                chosenRemote = candidate;
                            }
                        }
                    }

                    if (chosenRemote is not null)
                    {
                        if (!remoteBatchesByReplicaId.TryGetValue(chosenRemote, out var list))
                        {
                            list = new List<string>();
                            remoteBatchesByReplicaId[chosenRemote] = list;
                        }

                        list.Add(docId);
                    }
                    else
                    {
                        unreachableDocumentIds.Add(docId);
                    }

                    break;
                }

                case CrdtMapReduceRoutingMode.PrimaryReplicaOnly:
                {
                    string? chosenPrimary = null;
                    for (var i = 0; i < orderedReplicas.Count; i++)
                    {
                        var candidate = orderedReplicas[i];
                        if (string.Equals(candidate, replicaContext.ReplicaId, StringComparison.Ordinal))
                        {
                            chosenPrimary = candidate;
                            break;
                        }

                        if (!string.IsNullOrWhiteSpace(GetCachedPeerId(candidate)))
                        {
                            chosenPrimary = candidate;
                            break;
                        }
                    }

                    if (chosenPrimary is null)
                    {
                        var fallbackRemotes = await GetCachedFallbackRemotesAsync(shardId).ConfigureAwait(false);
                        for (var i = 0; i < fallbackRemotes.Count; i++)
                        {
                            var candidate = fallbackRemotes[i];
                            if (!orderedReplicas.Contains(candidate))
                            {
                                orderedReplicas.Add(candidate);
                            }

                            if (chosenPrimary is null && !string.IsNullOrWhiteSpace(GetCachedPeerId(candidate)))
                            {
                                chosenPrimary = candidate;
                            }
                        }
                    }

                    if (chosenPrimary is null)
                    {
                        unreachableDocumentIds.Add(docId);
                    }
                    else if (string.Equals(chosenPrimary, replicaContext.ReplicaId, StringComparison.Ordinal))
                    {
                        localDocumentBatch.Add(docId);
                    }
                    else
                    {
                        if (!remoteBatchesByReplicaId.TryGetValue(chosenPrimary, out var list))
                        {
                            list = new List<string>();
                            remoteBatchesByReplicaId[chosenPrimary] = list;
                        }

                        list.Add(docId);
                    }

                    break;
                }

                case CrdtMapReduceRoutingMode.AllReplicas:
                {
                    var assignedAny = false;
                    for (var i = 0; i < orderedReplicas.Count; i++)
                    {
                        var replica = orderedReplicas[i];
                        if (string.Equals(replica, replicaContext.ReplicaId, StringComparison.Ordinal))
                        {
                            localDocumentBatch.Add(docId);
                            assignedAny = true;
                        }
                        else if (!string.IsNullOrWhiteSpace(GetCachedPeerId(replica)))
                        {
                            if (!remoteBatchesByReplicaId.TryGetValue(replica, out var list))
                            {
                                list = new List<string>();
                                remoteBatchesByReplicaId[replica] = list;
                            }

                            list.Add(docId);
                            assignedAny = true;
                        }
                    }

                    if (!assignedAny)
                    {
                        unreachableDocumentIds.Add(docId);
                    }

                    break;
                }
            }
        }

        if (unreachableDocumentIds.Count > 0 && !scope.AllowPartialResultsOnPeerFailure)
        {
            throw new InvalidOperationException(
                $"Cannot execute Map/Reduce job '{jobAlias}': no active replicas found for document(s) [{string.Join(", ", unreachableDocumentIds)}].");
        }

        var allNodeAccumulators = new List<TAccumulate>();
        var localDocsEvaluated = 0;
        var remoteDocsEvaluated = 0;
        var remotePeersContacted = 0;
        var wasShortCircuited = false;

        // Step 3: Local Execution directly against CurrentDocument without channel round-trips
        if (localDocumentBatch.Count > 0)
        {
            var localAccumulators = new List<TAccumulate>(localDocumentBatch.Count);
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();

            for (var i = 0; i < localDocumentBatch.Count; i++)
            {
                var docId = localDocumentBatch[i];
                var liveDoc = TryGetLocalDocument<TState>(docId);
                if (liveDoc is null)
                {
                    var promoted = await ResolveOrPromoteLocalDocumentAsync(docId, documentTypeAlias, cancellationToken).ConfigureAwait(false);
                    liveDoc = promoted as IDistributedCrdtDocument<TState>;
                }

                if (liveDoc is not null)
                {
                    var crdtDoc = liveDoc.CurrentDocument;
                    var context = new CrdtMapContext<TState>(
                        docId,
                        liveDoc.ShardId,
                        replicaContext.ReplicaId,
                        crdtDoc,
                        liveDoc);

                    var mapValueTask = handler.MapDocumentAsync(context, input, cancellationToken);
                    var acc = mapValueTask.IsCompletedSuccessfully
                        ? mapValueTask.Result
                        : await mapValueTask.ConfigureAwait(false);
                    localAccumulators.Add(acc);
                    localDocsEvaluated++;

                    if (handler.CanShortCircuit(input, acc))
                    {
                        wasShortCircuited = true;
                        break;
                    }

                    continue;
                }

                var storedBytes = await TryLoadStoredDocumentSnapshotBytesAsync(docId, documentTypeAlias, cancellationToken).ConfigureAwait(false);
                if (storedBytes is { Length: > 0 })
                {
                    var storedDoc = serializer.DeserializeFromBytes<CrdtDocument<TState>>(storedBytes);
                    if (storedDoc.Data is not null)
                    {
                        var shardId = shardIdProvider.GetShardId<TState>(docId) ?? docId;
                        var demotedContext = new CrdtMapContext<TState>(
                            docId,
                            shardId,
                            replicaContext.ReplicaId,
                            storedDoc,
                            LiveDocumentHandle: null);

                        var mapValueTask = handler.MapDocumentAsync(demotedContext, input, cancellationToken);
                        var acc = mapValueTask.IsCompletedSuccessfully
                            ? mapValueTask.Result
                            : await mapValueTask.ConfigureAwait(false);
                        localAccumulators.Add(acc);
                        localDocsEvaluated++;

                        if (handler.CanShortCircuit(input, acc))
                        {
                            wasShortCircuited = true;
                            break;
                        }

                        continue;
                    }
                }

                // Local node could not evaluate the document; schedule for remote failover if possible
                if (scope.RoutingMode != CrdtMapReduceRoutingMode.AllReplicas &&
                    TryAssignFallbackReplica(docId, replicaContext.ReplicaId, docCandidateReplicas, new HashSet<string>(StringComparer.Ordinal) { replicaContext.ReplicaId }, clusterStateTracker, out var fallbackReplica))
                {
                    if (!remoteBatchesByReplicaId.TryGetValue(fallbackReplica!, out var list))
                    {
                        list = new List<string>();
                        remoteBatchesByReplicaId[fallbackReplica!] = list;
                    }

                    list.Add(docId);
                }
                else
                {
                    unreachableDocumentIds.Add(docId);
                }
            }

            if (localAccumulators.Count > 0)
            {
                var localCombined = localAccumulators.Count == 1
                    ? localAccumulators[0]
                    : handler.Combine(input, localAccumulators);
                allNodeAccumulators.Add(localCombined);
            }
        }

        // Step 4: Parallel Remote Batched RPCs & Failover
        if (!wasShortCircuited && remoteBatchesByReplicaId.Count > 0)
        {
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();
            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            var rpcCorrelator = serviceProvider.GetRequiredService<IP2pRpcCorrelator>();

            var serializedInput = serializer.SerializeToBytes(input);
            var maxRetries = Math.Max(1, options.RpcMaxRetries);
            var rpcTimeout = TimeSpan.FromMilliseconds(Math.Max(1, options.RpcTimeoutMilliseconds));
            var retryDelayMs = Math.Max(0, options.RpcRetryDelayMilliseconds);

            var triedReplicasPerDoc = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
            var currentBatches = new Dictionary<string, List<string>>(remoteBatchesByReplicaId, StringComparer.Ordinal);
            var contactedPeersSet = new HashSet<string>(StringComparer.Ordinal);
            Exception? lastRemoteException = null;

            for (var attempt = 0; attempt < maxRetries && currentBatches.Count > 0 && !wasShortCircuited; attempt++)
            {
                if (attempt > 0 && retryDelayMs > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(retryDelayMs * attempt), cancellationToken).ConfigureAwait(false);
                }

                using var remoteCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                var batchEntries = new List<KeyValuePair<string, List<string>>>(currentBatches);
                var batchTasks = new Task<RemoteBatchAttemptOutcome>[batchEntries.Count];

                for (var i = 0; i < batchEntries.Count; i++)
                {
                    var targetReplicaId = batchEntries[i].Key;
                    var docBatch = batchEntries[i].Value;
                    contactedPeersSet.Add(targetReplicaId);

                    for (var d = 0; d < docBatch.Count; d++)
                    {
                        var docId = docBatch[d];
                        if (!triedReplicasPerDoc.TryGetValue(docId, out var triedSet))
                        {
                            triedSet = new HashSet<string>(StringComparer.Ordinal);
                            triedReplicasPerDoc[docId] = triedSet;
                        }

                        triedSet.Add(targetReplicaId);
                    }

                    batchTasks[i] = DispatchRemoteMapReduceBatchAsync(
                        targetReplicaId,
                        jobAlias,
                        documentTypeAlias,
                        docBatch,
                        serializedInput,
                        rpcTimeout,
                        clusterStateTracker,
                        directSender,
                        rpcCorrelator,
                        serializer,
                        remoteCts.Token);
                }

                var pendingTasks = new List<Task<RemoteBatchAttemptOutcome>>(batchTasks);
                var failedOrMissingDocs = new List<string>();
                var successDocsThisRound = new HashSet<string>(StringComparer.Ordinal);

                while (pendingTasks.Count > 0)
                {
                    var completedTask = await Task.WhenAny(pendingTasks).ConfigureAwait(false);
                    pendingTasks.Remove(completedTask);

                    var outcome = await completedTask.ConfigureAwait(false);
                    if (outcome.IsSuccess)
                    {
                        remoteDocsEvaluated += outcome.EvaluatedDocumentCount;

                        HashSet<string>? missingSet = null;
                        if (outcome.MissingDocumentIds is { Count: > 0 })
                        {
                            missingSet = new HashSet<string>(outcome.MissingDocumentIds, StringComparer.Ordinal);
                            failedOrMissingDocs.AddRange(outcome.MissingDocumentIds);
                        }

                        for (var r = 0; r < outcome.RequestedDocumentIds.Count; r++)
                        {
                            var reqDocId = outcome.RequestedDocumentIds[r];
                            if (missingSet is null || !missingSet.Contains(reqDocId))
                            {
                                successDocsThisRound.Add(reqDocId);
                            }
                        }

                        if (outcome.HasAccumulator)
                        {
                            var peerAcc = serializer.DeserializeFromBytes<TAccumulate>(outcome.SerializedAccumulator);
                            if (peerAcc is not null)
                            {
                                allNodeAccumulators.Add(peerAcc);
                                if (handler.CanShortCircuit(input, peerAcc))
                                {
                                    wasShortCircuited = true;
                                    await remoteCts.CancelAsync().ConfigureAwait(false);
                                    break;
                                }
                            }
                        }
                    }
                    else
                    {
                        if (outcome.Exception is not null)
                        {
                            lastRemoteException = outcome.Exception;
                        }
                        else if (!string.IsNullOrWhiteSpace(outcome.ErrorMessage))
                        {
                            lastRemoteException = new InvalidOperationException(outcome.ErrorMessage);
                        }

                        if (!wasShortCircuited)
                        {
                            failedOrMissingDocs.AddRange(outcome.RequestedDocumentIds);
                        }
                    }
                }

                // Clear retry tracking for documents that succeeded on an earlier retry attempt
                foreach (var succeededDocId in successDocsThisRound)
                {
                    unreachableDocumentIds.Remove(succeededDocId);
                }

                currentBatches.Clear();

                if (wasShortCircuited || failedOrMissingDocs.Count == 0)
                {
                    break;
                }

                if (scope.RoutingMode == CrdtMapReduceRoutingMode.AllReplicas)
                {
                    for (var i = 0; i < failedOrMissingDocs.Count; i++)
                    {
                        if (!unreachableDocumentIds.Contains(failedOrMissingDocs[i]))
                        {
                            unreachableDocumentIds.Add(failedOrMissingDocs[i]);
                        }
                    }

                    break;
                }

                // Re-batch failed or missing documents onto the next untried candidate replica
                for (var i = 0; i < failedOrMissingDocs.Count; i++)
                {
                    var failedDocId = failedOrMissingDocs[i];
                    if (successDocsThisRound.Contains(failedDocId))
                    {
                        continue;
                    }

                    if (!triedReplicasPerDoc.TryGetValue(failedDocId, out var triedSet))
                    {
                        triedSet = new HashSet<string>(StringComparer.Ordinal);
                        triedReplicasPerDoc[failedDocId] = triedSet;
                    }

                    if (attempt < maxRetries - 1 &&
                        TryAssignFallbackReplica(failedDocId, replicaContext.ReplicaId, docCandidateReplicas, triedSet, clusterStateTracker, out var nextReplica))
                    {
                        if (!currentBatches.TryGetValue(nextReplica!, out var list))
                        {
                            list = new List<string>();
                            currentBatches[nextReplica!] = list;
                        }

                        if (!list.Contains(failedDocId))
                        {
                            list.Add(failedDocId);
                        }
                    }
                    else if (attempt < maxRetries - 1)
                    {
                        // All known candidate replicas have been tried once (e.g., transient RPC timeout on a large batch).
                        // Reset tried replicas and retry against the primary target replica on the next attempt.
                        triedSet.Clear();
                        if (TryAssignFallbackReplica(failedDocId, replicaContext.ReplicaId, docCandidateReplicas, triedSet, clusterStateTracker, out var retryReplica))
                        {
                            if (!currentBatches.TryGetValue(retryReplica!, out var list))
                            {
                                list = new List<string>();
                                currentBatches[retryReplica!] = list;
                            }

                            if (!list.Contains(failedDocId))
                            {
                                list.Add(failedDocId);
                            }
                        }
                        else if (!unreachableDocumentIds.Contains(failedDocId))
                        {
                            unreachableDocumentIds.Add(failedDocId);
                        }
                    }
                    else
                    {
                        if (!unreachableDocumentIds.Contains(failedDocId))
                        {
                            unreachableDocumentIds.Add(failedDocId);
                        }
                    }
                }
            }

            remotePeersContacted = contactedPeersSet.Count;

            if (!wasShortCircuited && unreachableDocumentIds.Count > 0 && !scope.AllowPartialResultsOnPeerFailure)
            {
                if (lastRemoteException is not null)
                {
                    throw new InvalidOperationException(
                        $"Map/Reduce job '{jobAlias}' failed for document(s) [{string.Join(", ", unreachableDocumentIds)}]: {lastRemoteException.Message}",
                        lastRemoteException);
                }

                throw new TimeoutException(
                    $"Map/Reduce job '{jobAlias}' could not reach any replica for document(s) [{string.Join(", ", unreachableDocumentIds)}].");
            }
        }

        // Step 5: Coordinator Final Combine & Reduce
        var finalAccumulator = allNodeAccumulators.Count == 1
            ? allNodeAccumulators[0]
            : handler.Combine(input, allNodeAccumulators);

        var finalResult = handler.Reduce(input, finalAccumulator);
        telemetry.RecordOrchestratorMapReduceExecuted(jobAlias, remotePeersContacted);

        return new CrdtMapReduceResultDto<TResult>(
            finalResult,
            TotalDocumentsTargeted: candidateDocuments.Count,
            LocalDocumentsEvaluated: localDocsEvaluated,
            RemoteDocumentsEvaluated: remoteDocsEvaluated,
            RemotePeersContacted: remotePeersContacted,
            WasShortCircuited: wasShortCircuited,
            UnreachableDocumentIds: unreachableDocumentIds);
    }

    public void Dispose()
    {
        telemetryRegistration.Dispose();
        disposeCts.Cancel();
        commandChannel?.Writer.TryComplete();
        disposeCts.Dispose();

        if (Registry != null)
        {
            if (Registry is IDisposable rd) rd.Dispose();
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
        catch (OperationCanceledException) { /* Clean termination */ }
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

        await PushDemotedDocumentSnapshotToTargetPeersAsync(doc, CancellationToken.None).ConfigureAwait(false);

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

    private async Task<List<string>> GetCandidateRemoteReplicasForShardAsync(
        string shardId,
        string localReplicaId,
        IClusterStateTracker clusterStateTracker,
        CancellationToken cancellationToken)
    {
        var candidates = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        var targetNodes = await distributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
        if (targetNodes != null)
        {
            for (int i = 0; i < targetNodes.Count; i++)
            {
                var replicaId = targetNodes[i];
                if (!string.Equals(replicaId, localReplicaId, StringComparison.Ordinal) && seen.Add(replicaId))
                {
                    candidates.Add(replicaId);
                }
            }
        }

        var activeReplicas = clusterStateTracker.GetActiveReplicaIds();
        for (int i = 0; i < activeReplicas.Count; i++)
        {
            var replicaId = activeReplicas[i];
            if (!string.Equals(replicaId, localReplicaId, StringComparison.Ordinal) && seen.Add(replicaId))
            {
                candidates.Add(replicaId);
            }
        }

        return candidates;
    }

    private async Task PushDemotedDocumentSnapshotToTargetPeersAsync(IDistributedCrdtDocument doc, CancellationToken cancellationToken)
    {
        try
        {
            var clusterStateTracker = serviceProvider.GetService<IClusterStateTracker>();
            var directSender = serviceProvider.GetService<IDirectMessageSender>();
            var serializer = serviceProvider.GetService<ICrdtSerializer>();
            var replicaContext = serviceProvider.GetService<ReplicaContext>();

            if (clusterStateTracker == null || directSender == null || serializer == null || replicaContext == null)
            {
                return;
            }

            var targetNodes = await distributionManager.GetTargetNodesForShardAsync(doc.ShardId, cancellationToken).ConfigureAwait(false);
            if (targetNodes == null || targetNodes.Count == 0)
            {
                return;
            }

            CrdtSnapshotDataDto? snapshotResult = null;

            for (int i = 0; i < targetNodes.Count; i++)
            {
                var targetReplica = targetNodes[i];
                if (string.Equals(targetReplica, replicaContext.ReplicaId, StringComparison.Ordinal))
                {
                    continue;
                }

                var targetPeerStr = clusterStateTracker.GetPeerIdByReplicaId(targetReplica);
                if (string.IsNullOrWhiteSpace(targetPeerStr) || !Guid.TryParse(targetPeerStr, out var peerGuid))
                {
                    continue;
                }

                snapshotResult ??= await doc.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
                var snapshot = snapshotResult.Value;

                var resMsg = new CrdtSnapshotMessage(replicaContext.ReplicaId, snapshot.SnapshotData, snapshot.GlobalState);
                var payload = serializer.SerializeToBytes(resMsg);
                var wrapper = new CrdtMessageWrapper(doc.DocumentId, "CrdtSnapshot", payload, doc.TypeAlias);
                var finalBytes = serializer.SerializeToBytes(wrapper);

                await directSender.SendDirectAsync(new PeerId(peerGuid), finalBytes, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to push final snapshot for demoted document {DocumentId} ({TypeAlias}).", doc.DocumentId, doc.TypeAlias);
        }
    }

    private readonly record struct CandidateMapReduceDocument(string DocumentId, string ShardId);

    private readonly record struct RemoteBatchAttemptOutcome(
        bool IsSuccess,
        bool HasAccumulator,
        int EvaluatedDocumentCount,
        byte[] SerializedAccumulator,
        IReadOnlyList<string> RequestedDocumentIds,
        IList<string>? MissingDocumentIds,
        string? ErrorMessage,
        Exception? Exception);

    private string ResolveMapReduceJobAlias<TState, TInput, TAccumulate, TResult>()
    {
        foreach (var executor in serviceProvider.GetServices<ICrdtMapReduceExecutor>())
        {
            if (executor.StateType == typeof(TState) &&
                executor.InputType == typeof(TInput) &&
                executor.AccumulateType == typeof(TAccumulate) &&
                executor.ResultType == typeof(TResult))
            {
                return executor.JobAlias;
            }
        }

        throw new InvalidOperationException(
            $"No ICrdtMapReduceExecutor is registered for ({typeof(TState).Name}, {typeof(TInput).Name}, {typeof(TAccumulate).Name}, {typeof(TResult).Name}).");
    }

    private async Task<List<CandidateMapReduceDocument>> ResolveCandidateMapReduceDocumentsAsync<TState>(
        string documentTypeAlias,
        CrdtMapReduceScopeDto scope,
        CancellationToken cancellationToken)
        where TState : class, new()
    {
        var candidates = new List<CandidateMapReduceDocument>();
        var seenDocIds = new HashSet<string>(StringComparer.Ordinal);
        HashSet<string>? targetShardSet = null;

        if (scope.TargetShardIds is { Count: > 0 })
        {
            targetShardSet = new HashSet<string>(scope.TargetShardIds, StringComparer.Ordinal);
        }

        var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");
        var registryDoc = registry.CurrentDocument;
        var registryMap = registryDoc.Data.Documents;

        if (scope.TargetDocumentIds is { Count: > 0 })
        {
            for (var i = 0; i < scope.TargetDocumentIds.Count; i++)
            {
                var docId = scope.TargetDocumentIds[i];
                if (string.IsNullOrWhiteSpace(docId))
                {
                    continue;
                }

                docId = stringPool.GetOrAdd(docId);
                if (!seenDocIds.Add(docId))
                {
                    continue;
                }

                if (TryFindRegistryEntry(registryMap, docId, documentTypeAlias, out var entry) && entry != null && entry.IsDeleted)
                {
                    continue;
                }

                var shardId = await GetShardIdAsync<TState>(docId, cancellationToken).ConfigureAwait(false);
                if (targetShardSet is null || targetShardSet.Contains(shardId))
                {
                    candidates.Add(new CandidateMapReduceDocument(docId, shardId));
                }
            }

            return candidates;
        }

        foreach (var kvp in registryMap)
        {
            var entry = kvp.Value;
            if (entry.IsDeleted || !string.Equals(entry.TypeAlias, documentTypeAlias, StringComparison.Ordinal))
            {
                continue;
            }

            if (!seenDocIds.Add(entry.DocumentId))
            {
                continue;
            }

            var shardId = await GetShardIdAsync<TState>(entry.DocumentId, cancellationToken).ConfigureAwait(false);
            if (targetShardSet is null || targetShardSet.Contains(shardId))
            {
                candidates.Add(new CandidateMapReduceDocument(entry.DocumentId, shardId));
            }
        }

        foreach (var activeDoc in activeDocuments.Values)
        {
            if (!string.Equals(activeDoc.TypeAlias, documentTypeAlias, StringComparison.Ordinal))
            {
                continue;
            }

            if (!seenDocIds.Add(activeDoc.DocumentId))
            {
                continue;
            }

            if (targetShardSet is null || targetShardSet.Contains(activeDoc.ShardId))
            {
                candidates.Add(new CandidateMapReduceDocument(activeDoc.DocumentId, activeDoc.ShardId));
            }
        }

        return candidates;
    }

    private static bool TryAssignFallbackReplica(
        string docId,
        string localReplicaId,
        Dictionary<string, List<string>> docCandidateReplicas,
        HashSet<string> triedReplicas,
        IClusterStateTracker clusterStateTracker,
        out string? fallbackReplica)
    {
        fallbackReplica = null;

        if (docCandidateReplicas.TryGetValue(docId, out var candidates))
        {
            for (var i = 0; i < candidates.Count; i++)
            {
                var candidate = candidates[i];
                if (string.Equals(candidate, localReplicaId, StringComparison.Ordinal) || triedReplicas.Contains(candidate))
                {
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(clusterStateTracker.GetPeerIdByReplicaId(candidate)))
                {
                    fallbackReplica = candidate;
                    return true;
                }
            }
        }

        var activeReplicas = clusterStateTracker.GetActiveReplicaIds();
        for (var i = 0; i < activeReplicas.Count; i++)
        {
            var candidate = activeReplicas[i];
            if (string.Equals(candidate, localReplicaId, StringComparison.Ordinal) || triedReplicas.Contains(candidate))
            {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(clusterStateTracker.GetPeerIdByReplicaId(candidate)))
            {
                fallbackReplica = candidate;
                return true;
            }
        }

        return false;
    }

    private static async Task<RemoteBatchAttemptOutcome> DispatchRemoteMapReduceBatchAsync(
        string targetReplicaId,
        string jobAlias,
        string documentTypeAlias,
        IReadOnlyList<string> documentBatch,
        byte[] serializedInput,
        TimeSpan rpcTimeout,
        IClusterStateTracker clusterStateTracker,
        IDirectMessageSender directSender,
        IP2pRpcCorrelator rpcCorrelator,
        ICrdtSerializer serializer,
        CancellationToken cancellationToken)
    {
        var peerIdStr = clusterStateTracker.GetPeerIdByReplicaId(targetReplicaId);
        if (string.IsNullOrWhiteSpace(peerIdStr) || !Guid.TryParse(peerIdStr, out var peerGuid))
        {
            return new RemoteBatchAttemptOutcome(
                IsSuccess: false,
                HasAccumulator: false,
                EvaluatedDocumentCount: 0,
                SerializedAccumulator: Array.Empty<byte>(),
                RequestedDocumentIds: documentBatch,
                MissingDocumentIds: null,
                ErrorMessage: $"No active network peer ID found for replica '{targetReplicaId}'.",
                Exception: null);
        }

        var correlationId = Guid.NewGuid();
        var req = new CrdtMapReduceRequestDto
        {
            CorrelationId = correlationId,
            JobAlias = jobAlias,
            DocumentTypeAlias = documentTypeAlias,
            DocumentIds = documentBatch as IList<string> ?? new List<string>(documentBatch),
            SerializedInput = serializedInput
        };

        var reqBytes = serializer.SerializeToBytes(req);
        var wrapper = new CrdtMessageWrapper("Cluster", "CrdtMapReduceReq", reqBytes, documentTypeAlias);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        try
        {
            var waitTask = rpcCorrelator.RegisterRequestAsync<CrdtMapReduceResponseDto>(correlationId, rpcTimeout, cancellationToken);
            await directSender.SendDirectAsync(new PeerId(peerGuid), wrapperBytes, cancellationToken).ConfigureAwait(false);
            var response = await waitTask.ConfigureAwait(false);

            return new RemoteBatchAttemptOutcome(
                IsSuccess: response.IsSuccess,
                HasAccumulator: response.HasAccumulator,
                EvaluatedDocumentCount: response.EvaluatedDocumentCount,
                SerializedAccumulator: response.SerializedAccumulator ?? Array.Empty<byte>(),
                RequestedDocumentIds: documentBatch,
                MissingDocumentIds: response.MissingDocumentIds,
                ErrorMessage: response.ErrorMessage,
                Exception: null);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return new RemoteBatchAttemptOutcome(
                IsSuccess: false,
                HasAccumulator: false,
                EvaluatedDocumentCount: 0,
                SerializedAccumulator: Array.Empty<byte>(),
                RequestedDocumentIds: documentBatch,
                MissingDocumentIds: null,
                ErrorMessage: "Map/Reduce RPC cancelled.",
                Exception: null);
        }
        catch (Exception ex)
        {
            return new RemoteBatchAttemptOutcome(
                IsSuccess: false,
                HasAccumulator: false,
                EvaluatedDocumentCount: 0,
                SerializedAccumulator: Array.Empty<byte>(),
                RequestedDocumentIds: documentBatch,
                MissingDocumentIds: null,
                ErrorMessage: ex.Message,
                Exception: ex);
        }
    }
}