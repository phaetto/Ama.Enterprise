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
    private readonly IServiceProvider serviceProvider;
    private readonly IDistributedCrdtStorage storage;
    private readonly IAsyncCrdtPatcher patcher;
    private readonly IShardDistributionManager distributionManager;
    private readonly IEnumerable<ICrdtOrchestratorObserver> orchestratorObservers;
    private readonly ILogger<CrdtDocumentOrchestrator> logger;
    private readonly IDistributedCrdtTelemetry telemetry;
    private readonly IDisposable telemetryRegistration;
    private readonly DistributedCrdtOptions options;
    
    private readonly ConcurrentDictionary<string, IDistributedCrdtDocument> activeDocuments = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string> shardIdCache = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Type, string> stateTypeAliases = new();
    
    private readonly Channel<PooledOrchestratorCommand> commandChannel;
    private readonly ConcurrentQueue<PooledOrchestratorCommand> commandPool = new();
    private readonly CancellationTokenSource disposeCts = new();
    private int syncPending;
    
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
        IDistributedCrdtTelemetry telemetry)
    {
        ArgumentNullException.ThrowIfNull(options);

        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));
        this.distributionManager = distributionManager ?? throw new ArgumentNullException(nameof(distributionManager));
        this.orchestratorObservers = orchestratorObservers ?? throw new ArgumentNullException(nameof(orchestratorObservers));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        this.options = options.Value;

        foreach (var factory in this.serviceProvider.GetServices<IDocumentFactory>())
        {
            this.stateTypeAliases.TryAdd(factory.StateType, factory.TypeAlias);
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

        _ = Task.Run(ProcessChannelAsync);
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
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

    public Task SyncDocumentsAsync(CancellationToken cancellationToken = default)
    {
        return SyncDocumentsAsync(IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    public async Task SyncDocumentsAsync(IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            if (Interlocked.Exchange(ref syncPending, 1) == 1)
            {
                return;
            }

            var ffCmd = GetCommand();
            ffCmd.Type = OrchestratorCommandType.SyncDocuments;
            ffCmd.IsFireAndForget = true;
            ffCmd.CancellationToken = CancellationToken.None;

            if (commandChannel.Writer.TryWrite(ffCmd))
            {
                telemetry.RecordOrchestratorCommandEnqueued();
                return;
            }

            await commandChannel.Writer.WriteAsync(ffCmd, cancellationToken).ConfigureAwait(false);
            telemetry.RecordOrchestratorCommandEnqueued();
            return;
        }

        var cmd = GetCommand();
        cmd.Type = OrchestratorCommandType.SyncDocuments;
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
        if (deliveryMode == IntentDeliveryMode.FireAndForget)
        {
            var ffCmd = GetCommand();
            ffCmd.Type = OrchestratorCommandType.ReevaluateTopology;
            ffCmd.IsFireAndForget = true;
            ffCmd.CancellationToken = CancellationToken.None;

            if (commandChannel.Writer.TryWrite(ffCmd))
            {
                telemetry.RecordOrchestratorCommandEnqueued();
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
    public ValueTask<string?> TryGetShardIdAsync(string documentId, CancellationToken cancellationToken = default)
    {
        return TryGetShardIdAsync(documentId, null, cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<string?> TryGetShardIdAsync(string documentId, string? typeAlias, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return null;

        var cacheKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
        if (shardIdCache.TryGetValue(cacheKey, out var cachedShardId))
        {
            return cachedShardId;
        }

        if (Registry != null && string.Equals(Registry.DocumentId, documentId, StringComparison.Ordinal))
        {
            shardIdCache.TryAdd(cacheKey, Registry.ShardId);
            return Registry.ShardId;
        }

        if (activeDocuments.TryGetValue(cacheKey, out var activeDoc))
        {
            shardIdCache.TryAdd(cacheKey, activeDoc.ShardId);
            return activeDoc.ShardId;
        }

        if (string.IsNullOrWhiteSpace(typeAlias))
        {
            foreach (var doc in activeDocuments.Values)
            {
                if (string.Equals(doc.DocumentId, documentId, StringComparison.Ordinal))
                {
                    shardIdCache.TryAdd(cacheKey, doc.ShardId);
                    return doc.ShardId;
                }
            }
        }

        if (Registry != null)
        {
            var registryDoc = await Registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            if (!TryFindRegistryEntry(registryDoc.Data.Documents, documentId, typeAlias, out var entry) && !string.IsNullOrWhiteSpace(typeAlias))
            {
                var factory = serviceProvider.GetKeyedService<IDocumentFactory>(typeAlias);
                if (factory != null)
                {
                    var resolvedShardId = factory.GetShardId(serviceProvider, documentId);
                    shardIdCache.TryAdd(cacheKey, resolvedShardId);
                    return resolvedShardId;
                }
            }
            else if (entry != null && !entry.IsDeleted)
            {
                var factory = serviceProvider.GetKeyedService<IDocumentFactory>(entry.TypeAlias);
                if (factory != null)
                {
                    var resolvedShardId = factory.GetShardId(serviceProvider, entry.DocumentId);
                    shardIdCache.TryAdd(CrdtRegistryEntry.GetRegistryKey(entry.TypeAlias, entry.DocumentId), resolvedShardId);
                    shardIdCache.TryAdd(cacheKey, resolvedShardId);
                    return resolvedShardId;
                }
            }
        }

        return null;
    }

    /// <inheritdoc />
    public async ValueTask<IDistributedCrdtDocument?> ResolveOrPromoteLocalDocumentAsync(string documentId, string? typeAlias = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return null;

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
            var registryDoc = await Registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
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
            shardId = factory.GetShardId(serviceProvider, documentId);
            shardIdCache.TryAdd(resolvedKey, shardId);
        }

        if (!await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false))
        {
            return null;
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

    public async ValueTask<CrdtDocument<TState>> GetDocumentStateAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        var localTypedDoc = TryGetLocalDocument<TState>(documentId);
        if (localTypedDoc != null)
        {
            return await localTypedDoc.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
        }

        var documentTypeAlias = GetTypeAlias<TState>();
        var shardId = await GetShardIdAsync<TState>(documentId, cancellationToken).ConfigureAwait(false);
        var isLocal = await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false);

        if (isLocal)
        {
            var promotedDoc = await ResolveOrPromoteLocalDocumentAsync(documentId, documentTypeAlias, cancellationToken).ConfigureAwait(false);
            if (promotedDoc is IDistributedCrdtDocument<TState> promotedTypedDoc)
            {
                return await promotedTypedDoc.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
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

        if (Registry != null && string.Equals(Registry.DocumentId, documentId, StringComparison.Ordinal) && Registry is IDistributedCrdtDocument<TState> registryTyped)
        {
            var applied = await registryTyped.ApplyRemoteIntentAsync(intent, targetProperty, patcher, deliveryMode, cancellationToken).ConfigureAwait(false);
            if (applied)
            {
                if (deliveryMode == IntentDeliveryMode.Acknowledged)
                {
                    await SyncDocumentsAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await SyncDocumentsAsync(IntentDeliveryMode.FireAndForget, cancellationToken).ConfigureAwait(false);
                }
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
        var serializedIntent = serializer.SerializeToBytes<IOperationIntent>(intent);

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

        if (Registry != null && string.Equals(Registry.DocumentId, documentId, StringComparison.Ordinal) && Registry is IDistributedCrdtDocument<TState> registryTyped)
        {
            await registryTyped.ApplyPatchAsync(patch, deliveryMode, cancellationToken).ConfigureAwait(false);
            if (deliveryMode == IntentDeliveryMode.Acknowledged)
            {
                await SyncDocumentsAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await SyncDocumentsAsync(IntentDeliveryMode.FireAndForget, cancellationToken).ConfigureAwait(false);
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

        var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");
        var registryKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
        var intent = new MapSetIntent(registryKey, new CrdtRegistryEntry(documentId, typeAlias, false));
        
        if (Registry is IDistributedCrdtDocument<CrdtRegistryState> registryTyped)
        {
            var applied = await registryTyped.ApplyRemoteIntentAsync(intent, nameof(CrdtRegistryState.Documents), patcher, deliveryMode, cancellationToken).ConfigureAwait(false);
            if (applied)
            {
                if (deliveryMode == IntentDeliveryMode.Acknowledged)
                {
                    await PromoteSingleDocumentAsync(documentId, typeAlias, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await SyncDocumentsAsync(IntentDeliveryMode.FireAndForget, cancellationToken).ConfigureAwait(false);
                }

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
        return ProvideSnapshotAsync(documentId, null, targetReplicaId, targetPeerId, cancellationToken);
    }

    public async Task ProvideSnapshotAsync(string documentId, string? typeAlias, string targetReplicaId, PeerId targetPeerId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

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

        if (targetDoc == null)
        {
            logger.LogWarning("Requested snapshot for unknown document {DocumentId} ({TypeAlias}).", documentId, typeAlias);
            return;
        }

        try
        {
            var snapshotResult = await targetDoc.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
            
            var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();

            var resMsg = new CrdtSnapshotMessage(replicaContext.ReplicaId, snapshotResult.SnapshotData, snapshotResult.GlobalState);
            var payload = serializer.SerializeToBytes(resMsg);

            string? documentTypeAlias = !string.IsNullOrWhiteSpace(typeAlias) ? typeAlias : targetDoc.TypeAlias;
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

    public async Task DispatchAntiEntropyStateAsync(CancellationToken cancellationToken = default)
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
            await DispatchAntiEntropyStateAsync(targetPeerId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to dispatch state sync to peer.");
        }
    }

    public async Task DispatchAntiEntropyStateAsync(PeerId targetPeerId, CancellationToken cancellationToken = default)
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

            var activeShardIds = new HashSet<string>(StringComparer.Ordinal);
            if (Registry != null)
            {
                activeShardIds.Add(Registry.ShardId);
            }

            foreach (var doc in activeDocuments.Values)
            {
                activeShardIds.Add(doc.ShardId);
            }

            var knownShardIds = versionVectorManager.EnsureShardsRegisteredAndGetAll(activeShardIds);

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

            var globalState = await versionVectorManager.GetShardsSnapshotAsync(localShards, cancellationToken).ConfigureAwait(false);

            var syncMsg = new CrdtStateSyncMessage(replicaContext.ReplicaId, globalState);
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
        foreach (var observer in orchestratorObservers)
        {
            try
            {
                await observer.OnTopologyChangedAsync(cancellationToken).ConfigureAwait(false);
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
                try
                {
                    cmd.CancellationToken.ThrowIfCancellationRequested();

                    switch (cmd.Type)
                    {
                        case OrchestratorCommandType.Initialize:
                            await ProcessInitializeInternalAsync(cmd.CancellationToken).ConfigureAwait(false);
                            break;
                        case OrchestratorCommandType.SyncDocuments:
                            Interlocked.Exchange(ref syncPending, 0);
                            await ProcessSyncDocumentsInternalAsync(cmd.CancellationToken).ConfigureAwait(false);
                            break;
                        case OrchestratorCommandType.CreateDocument:
                            await ProcessPromoteDocumentInternalAsync(cmd.DocumentId!, cmd.TypeAlias!, cmd.CancellationToken).ConfigureAwait(false);
                            break;
                        case OrchestratorCommandType.ReevaluateTopology:
                            Interlocked.Exchange(ref syncPending, 0);
                            await ProcessSyncDocumentsInternalAsync(cmd.CancellationToken).ConfigureAwait(false);
                            break;
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
        var registryState = new CrdtRegistryState();
        Registry = ActivatorUtilities.CreateInstance<DistributedCrdtDocument<CrdtRegistryState>>(serviceProvider, registryState);
        shardIdCache.TryAdd(Registry.DocumentId, Registry.ShardId);
        
        await Registry.InitializeAsync(cancellationToken).ConfigureAwait(false);

        var versionVectorManager = serviceProvider.GetRequiredService<IShardVersionVectorManager>();
        versionVectorManager.EnsureShardRegistered(Registry.ShardId);
        
        await ProcessSyncDocumentsInternalAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task ProcessPromoteDocumentInternalAsync(string documentId, string typeAlias, CancellationToken cancellationToken)
    {
        var compositeKey = CrdtRegistryEntry.GetRegistryKey(typeAlias, documentId);
        if (activeDocuments.ContainsKey(compositeKey))
        {
            return;
        }

        var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");
        var registryDoc = await registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
        if (TryFindRegistryEntry(registryDoc.Data.Documents, documentId, typeAlias, out var existingEntry) && existingEntry != null && existingEntry.IsDeleted)
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
            shardId = factory.GetShardId(serviceProvider, documentId);
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
            var versionVectorManager = serviceProvider.GetRequiredService<IShardVersionVectorManager>();
            versionVectorManager.EnsureShardRegistered(doc.ShardId);

            telemetry.RecordOrchestratorDocumentCreated(documentId);
            logger.LogDebug("Lazily promoted active CRDT document: {DocumentId} ({TypeAlias})", documentId, typeAlias);
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

    private async Task PromoteSingleDocumentAsync(string documentId, string typeAlias, CancellationToken cancellationToken)
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
            shardId = factory.GetShardId(serviceProvider, documentId);
            shardIdCache.TryAdd(compositeKey, shardId);
        }

        if (!await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false))
        {
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
                                shardId = factory.GetShardId(serviceProvider, entry.DocumentId);
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
                                var demotedShardId = activeDoc.ShardId;
                                try
                                {
                                    if (activeDoc is IDisposable d) d.Dispose();
                                }
                                catch (ObjectDisposedException) { }
                                
                                bool shardStillInUse = string.Equals(registry.ShardId, demotedShardId, StringComparison.Ordinal);
                                if (!shardStillInUse)
                                {
                                    foreach (var remainingDoc in activeDocuments.Values)
                                    {
                                        if (string.Equals(remainingDoc.ShardId, demotedShardId, StringComparison.Ordinal))
                                        {
                                            shardStillInUse = true;
                                            break;
                                        }
                                    }
                                }

                                if (!shardStillInUse)
                                {
                                    versionVectorManager.RemoveShard(demotedShardId);
                                }

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
                    var demotedShardId = doc.ShardId;
                    try
                    {
                        if (doc is IDisposable d) d.Dispose();
                    }
                    catch (ObjectDisposedException) { }

                    bool shardStillInUse = string.Equals(registry.ShardId, demotedShardId, StringComparison.Ordinal);
                    if (!shardStillInUse)
                    {
                        foreach (var remainingDoc in activeDocuments.Values)
                        {
                            if (string.Equals(remainingDoc.ShardId, demotedShardId, StringComparison.Ordinal))
                            {
                                shardStillInUse = true;
                                break;
                            }
                        }
                    }

                    if (!shardStillInUse)
                    {
                        versionVectorManager.RemoveShard(demotedShardId);
                    }

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
        var shardId = shardIdProvider.GetShardId<TState>(documentId) ?? documentId;
        shardIdCache.TryAdd(cacheKey, shardId);
        return new ValueTask<string>(shardId);
    }

    private string? GetTypeAlias<TState>()
    {
        if (typeof(TState) == typeof(CrdtRegistryState))
        {
            return Constants.SystemRegistryDocumentId;
        }

        return stateTypeAliases.TryGetValue(typeof(TState), out var alias) ? alias : null;
    }
}