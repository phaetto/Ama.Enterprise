namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
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
/// Centralized stateless gateway orchestrating global P2P CRDT document bindings eliminating proxy memory dependencies executing stateless async RPCs dynamically.
/// </summary>
public sealed class CrdtDocumentOrchestrator : ICrdtDocumentOrchestrator, IDisposable
{
    private readonly IServiceProvider serviceProvider;
    private readonly IDistributedCrdtStorage storage;
    private readonly IAsyncCrdtPatcher patcher;
    private readonly IShardDistributionManager distributionManager;
    private readonly IEnumerable<ICrdtOrchestratorObserver> orchestratorObservers;
    private readonly ILogger<CrdtDocumentOrchestrator> logger;
    
    private readonly ConcurrentDictionary<string, IDistributedCrdtDocument> activeDocuments = new(StringComparer.Ordinal);
    
    // Completely replaces lock blockages allowing multi-threaded external awaiting bounds
    private readonly Channel<PooledOrchestratorCommand> commandChannel;
    private readonly ConcurrentQueue<PooledOrchestratorCommand> commandPool = new();
    private readonly CancellationTokenSource disposeCts = new();

    private readonly Meter meter;
    private readonly Counter<long> documentCreatedCounter;
    private readonly Counter<long> documentDeletedCounter;
    private readonly Counter<long> antiEntropySyncCounter;
    private readonly Counter<long> snapshotsDispatchedCounter;
    private readonly Counter<long> snapshotBytesDispatchedCounter;
    
    // Channel metrics
    private readonly Counter<long> commandsEnqueuedCounter;
    private readonly Counter<long> commandsProcessedCounter;
    private readonly Counter<long> commandFailuresCounter;
    
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
        IMeterFactory? meterFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));
        this.distributionManager = distributionManager ?? throw new ArgumentNullException(nameof(distributionManager));
        this.orchestratorObservers = orchestratorObservers ?? throw new ArgumentNullException(nameof(orchestratorObservers));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var capacity = options.Value.ChannelCapacity;
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

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtDocumentOrchestrator") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtDocumentOrchestrator");
        this.documentCreatedCounter = this.meter.CreateCounter<long>("crdt.orchestrator.documents_created", "documents", "Total CRDT documents mapped dynamically");
        this.documentDeletedCounter = this.meter.CreateCounter<long>("crdt.orchestrator.documents_deleted", "documents", "Total CRDT documents permanently tombstoned");
        this.antiEntropySyncCounter = this.meter.CreateCounter<long>("crdt.orchestrator.anti_entropy_syncs", "syncs", "Total anti-entropy point-to-point synchronizations dispatched");
        this.snapshotsDispatchedCounter = this.meter.CreateCounter<long>("crdt.orchestrator.snapshots_dispatched", "snapshots", "Total full state snapshots explicitly provided responding to log gaps");
        this.snapshotBytesDispatchedCounter = this.meter.CreateCounter<long>("crdt.orchestrator.snapshot_bytes_dispatched", "bytes", "Total bytes dispatched for full state snapshot fallbacks");

        this.commandsEnqueuedCounter = this.meter.CreateCounter<long>("crdt.orchestrator.channel.commands_enqueued", "commands", "Total orchestrator commands enqueued to the lock-free channel");
        this.commandsProcessedCounter = this.meter.CreateCounter<long>("crdt.orchestrator.channel.commands_processed", "commands", "Total orchestrator commands processed by the channel");
        this.commandFailuresCounter = this.meter.CreateCounter<long>("crdt.orchestrator.channel.command_failures", "errors", "Total orchestrator command processing failures");
        
        _ = this.meter.CreateObservableGauge("crdt.orchestrator.channel.queue_length", () => this.commandChannel.Reader.CanCount ? this.commandChannel.Reader.Count : 0, "commands", "Current number of pending orchestrator commands");
        _ = this.meter.CreateObservableGauge("crdt.orchestrator.channel.pool_size", () => this.commandPool.Count, "commands", "Current size of the orchestrator command object pool");

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
            commandsEnqueuedCounter.Add(1);
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    public async Task SyncDocumentsAsync(CancellationToken cancellationToken = default)
    {
        var cmd = GetCommand();
        cmd.Type = OrchestratorCommandType.SyncDocuments;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1);
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }
    
    public async Task ReevaluateTopologyAsync(CancellationToken cancellationToken = default)
    {
        var cmd = GetCommand();
        cmd.Type = OrchestratorCommandType.ReevaluateTopology;
        cmd.CancellationToken = cancellationToken;

        try
        {
            await commandChannel.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
            commandsEnqueuedCounter.Add(1);
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
        if (Registry != null && Registry.DocumentId == documentId && Registry is IDistributedCrdtDocument<TState> registryTyped)
        {
            return registryTyped;
        }

        if (activeDocuments.TryGetValue(documentId, out var doc) && doc is IDistributedCrdtDocument<TState> typedDoc)
        {
            return typedDoc;
        }

        return null;
    }

    public async ValueTask<CrdtDocument<TState>> GetDocumentStateAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
    {
        if (Registry != null && Registry.DocumentId == documentId && Registry is IDistributedCrdtDocument<TState> registryTyped)
        {
            return await registryTyped.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
        }

        if (activeDocuments.TryGetValue(documentId, out var activeDoc) && activeDoc is IDistributedCrdtDocument<TState> typedDoc)
        {
            return await typedDoc.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
        }

        var shardId = await GetShardIdAsync<TState>(documentId, cancellationToken).ConfigureAwait(false);
        
        var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();
        var clusterStateTracker = serviceProvider.GetRequiredService<IClusterStateTracker>();
        var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
        var rpcCorrelator = serviceProvider.GetRequiredService<IP2pRpcCorrelator>();

        var req = new CrdtReadRequestDto { CorrelationId = Guid.NewGuid(), DocumentId = documentId };
        var reqPayload = serializer.SerializeToBytes(req);
        var wrapper = new CrdtMessageWrapper(documentId, "CrdtReadReq", reqPayload);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        var activeNodes = await distributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
        if (activeNodes == null || activeNodes.Count == 0)
        {
            throw new InvalidOperationException($"Cannot evaluate stateless CRDT State. No active nodes found mapping strictly for shard {shardId}.");
        }
        
        var targetReplicaId = activeNodes[0]; 
        var targetPeerId = clusterStateTracker.GetPeerIdByReplicaId(targetReplicaId);

        if (string.IsNullOrWhiteSpace(targetPeerId))
        {
            throw new InvalidOperationException($"Cannot evaluate stateless CRDT State. Replica {targetReplicaId} has no active network peer mapping.");
        }

        logger.LogTrace("Stateless gateway actively querying explicit network RPC bounds addressing document {DocumentId} against replica {TargetReplica} via peer {TargetPeer}.", documentId, targetReplicaId, targetPeerId);

        await directSender.SendDirectAsync(new PeerId(Guid.Parse(targetPeerId)), wrapperBytes, cancellationToken).ConfigureAwait(false);

        return await rpcCorrelator.RegisterRequestAsync<CrdtDocument<TState>>(req.CorrelationId, TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> ForwardIntentAsync<TState>(string documentId, IOperationIntent intent, string targetProperty, CancellationToken cancellationToken = default) where TState : class, new()
    {
        var shardId = await GetShardIdAsync<TState>(documentId, cancellationToken).ConfigureAwait(false);
        var isLocal = await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false);

        if (isLocal)
        {
            if (Registry != null && Registry.DocumentId == documentId && Registry is IDistributedCrdtDocument<TState> registryTyped)
            {
                logger.LogTrace("Stateless gateway executing intent directly against local Registry document {DocumentId}.", documentId);
                return await registryTyped.ApplyRemoteIntentAsync(intent, targetProperty, patcher, cancellationToken).ConfigureAwait(false);
            }

            if (activeDocuments.TryGetValue(documentId, out var activeDoc) && activeDoc is IDistributedCrdtDocument<TState> typedDoc)
            {
                logger.LogTrace("Stateless gateway executing intent directly against local active document {DocumentId}.", documentId);
                return await typedDoc.ApplyRemoteIntentAsync(intent, targetProperty, patcher, cancellationToken).ConfigureAwait(false);
            }
        }
        
        var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();
        var clusterStateTracker = serviceProvider.GetRequiredService<IClusterStateTracker>();
        var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
        var rpcCorrelator = serviceProvider.GetRequiredService<IP2pRpcCorrelator>();

        var req = new CrdtIntentForwardRequestDto
        {
            CorrelationId = Guid.NewGuid(),
            DocumentId = documentId,
            SerializedIntent = serializer.SerializeToBytes(intent),
            IntentTypeAlias = intent.GetType().Name,
            TargetProperty = targetProperty
        };

        var reqPayload = serializer.SerializeToBytes(req);
        var wrapper = new CrdtMessageWrapper(documentId, "CrdtIntentReq", reqPayload);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        var activeNodes = await distributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
        if (activeNodes == null || activeNodes.Count == 0)
        {
            logger.LogWarning("Forwarding generic intent rejected mapping strict zero node topologies bounding gateway.");
            return false;
        }

        var targetReplicaId = activeNodes[0];
        var targetPeerId = clusterStateTracker.GetPeerIdByReplicaId(targetReplicaId);

        if (string.IsNullOrWhiteSpace(targetPeerId))
        {
            logger.LogWarning("Forwarding generic intent rejected mapping strict disconnected network peer for replica {ReplicaId}.", targetReplicaId);
            return false;
        }
        
        logger.LogTrace("Stateless gateway forwarding execution intent mapping explicit document {DocumentId} towards remote shard owner {TargetReplica} via peer {TargetPeer}.", documentId, targetReplicaId, targetPeerId);
        
        await directSender.SendDirectAsync(new PeerId(Guid.Parse(targetPeerId)), wrapperBytes, cancellationToken).ConfigureAwait(false);

        var response = await rpcCorrelator.RegisterRequestAsync<CrdtIntentForwardResponseDto>(req.CorrelationId, TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
        
        if (!response.IsSuccess)
        {
            logger.LogWarning("Gateway execution explicitly faulting natively traversing intent: {ErrorMessage}", response.ErrorMessage);
        }

        return response.IsSuccess;
    }

    public async Task ApplyPatchAsync<TState>(string documentId, CrdtPatch patch, CancellationToken cancellationToken = default) where TState : class, new()
    {
        if (patch.Operations == null || patch.Operations.Count == 0)
        {
            return;
        }

        var shardId = await GetShardIdAsync<TState>(documentId, cancellationToken).ConfigureAwait(false);
        var isLocal = await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false);

        if (isLocal)
        {
            if (Registry != null && Registry.DocumentId == documentId && Registry is IDistributedCrdtDocument<TState> registryTyped)
            {
                await registryTyped.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (activeDocuments.TryGetValue(documentId, out var activeDoc) && activeDoc is IDistributedCrdtDocument<TState> typedDoc)
            {
                await typedDoc.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
                return;
            }
        }
        
        var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();
        var clusterStateTracker = serviceProvider.GetRequiredService<IClusterStateTracker>();
        var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
        var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();

        var patchMsg = new CrdtPatchMessage(replicaContext.ReplicaId, patch);
        var payload = serializer.SerializeToBytes(patchMsg);
        var wrapper = new CrdtMessageWrapper(documentId, "CrdtPatch", payload);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        var activeNodes = await distributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
        if (activeNodes == null || activeNodes.Count == 0)
        {
            logger.LogWarning("Forwarding patch rejected mapping strict zero node topologies bounding gateway for shard {ShardId}.", shardId);
            return;
        }

        foreach (var targetReplicaId in activeNodes)
        {
            var targetPeerId = clusterStateTracker.GetPeerIdByReplicaId(targetReplicaId);
            if (!string.IsNullOrWhiteSpace(targetPeerId))
            {
                logger.LogTrace("Stateless gateway forwarding explicit patch for document {DocumentId} towards remote shard owner {TargetReplica} via peer {TargetPeer}.", documentId, targetReplicaId, targetPeerId);
                await directSender.SendDirectAsync(new PeerId(Guid.Parse(targetPeerId)), wrapperBytes, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async Task CreateDocumentAsync(string documentId, string typeAlias, CancellationToken cancellationToken = default)
    {
        var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");
        var intent = new MapSetIntent(documentId, new CrdtRegistryEntry(documentId, typeAlias, false));
        
        var success = await ForwardIntentAsync<CrdtRegistryState>(registry.DocumentId, intent, nameof(CrdtRegistryState.Documents), cancellationToken).ConfigureAwait(false);
        
        if (success)
        {
            logger.LogInformation("Successfully forwarded document creation intent mapping explicit remote bounds for {DocumentId}.", documentId);
        }
        else
        {
            logger.LogWarning("Failed to forward document creation intent targeting remote shards for {DocumentId}.", documentId);
        }
    }

    public async Task DeleteDocumentAsync(string documentId, CancellationToken cancellationToken = default)
    {
        var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");

        var registryDoc = await registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
        if (registryDoc.Data.Documents.TryGetValue(documentId, out var existing) && !existing.IsDeleted)
        {
            var intent = new MapSetIntent(documentId, existing with { IsDeleted = true });
            
            var success = await ForwardIntentAsync<CrdtRegistryState>(registry.DocumentId, intent, nameof(CrdtRegistryState.Documents), cancellationToken).ConfigureAwait(false);
            
            if (success)
            {
                logger.LogInformation("Successfully forwarded document tombstone intent mapping explicit remote bounds for {DocumentId}.", documentId);
            }
            else
            {
                logger.LogWarning("Failed to forward document tombstone intent targeting remote shards for {DocumentId}.", documentId);
            }
        }
    }

    public async Task ProvideSnapshotAsync(string documentId, string targetReplicaId, PeerId targetPeerId, CancellationToken cancellationToken = default)
    {
        var topologyProvider = serviceProvider.GetRequiredService<IScopeTopologyProvider>();
        var isExpected = await topologyProvider.IsPeerExpectedAsync(targetPeerId.Value.ToString(), cancellationToken).ConfigureAwait(false);
        if (!isExpected)
        {
            logger.LogWarning("Rejected snapshot request for document {DocumentId} from explicitly excluded peer {PeerId}.", documentId, targetPeerId.Value);
            return;
        }

        var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");

        if (!activeDocuments.TryGetValue(documentId, out var doc) && registry.DocumentId != documentId)
        {
            logger.LogWarning("Requested snapshot for unknown or completely detached document {DocumentId}.", documentId);
            return;
        }

        var targetDoc = documentId == registry.DocumentId ? registry : doc;

        try
        {
            var snapshotResult = await targetDoc!.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
            
            var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();

            var resMsg = new CrdtSnapshotMessage(replicaContext.ReplicaId, snapshotResult.SnapshotData, snapshotResult.GlobalState);
            var payload = serializer.SerializeToBytes(resMsg);

            var wrapper = new CrdtMessageWrapper(documentId, "CrdtSnapshot", payload);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            await directSender.SendDirectAsync(targetPeerId, finalBytes, cancellationToken).ConfigureAwait(false); 
            
            snapshotsDispatchedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
            snapshotBytesDispatchedCounter.Add(finalBytes.Length, new KeyValuePair<string, object?>("document_id", documentId));
            logger.LogInformation("Dispatched targeted complete document snapshot fallback payload for document {DocumentId} to expected peer {PeerId}.", documentId, targetPeerId.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to dispatch snapshot fallback payload for document {DocumentId}.", documentId);
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

            var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();

            var localShards = new HashSet<string>(StringComparer.Ordinal);
            foreach (var shardKvp in replicaContext.ShardVersionVectors)
            {
                if (await distributionManager.IsShardLocalAsync(shardKvp.Key, cancellationToken).ConfigureAwait(false))
                {
                    localShards.Add(shardKvp.Key);
                }
            }

            ShardVersionVectorMap globalState;
            lock (replicaContext.ShardVersionVectors)
            {
                globalState = new ShardVersionVectorMap();
                foreach (var shardKvp in replicaContext.ShardVersionVectors)
                {
                    // Route specifically tracking strictly local shards avoiding network saturation limits
                    if (!localShards.Contains(shardKvp.Key))
                    {
                        continue;
                    }

                    var copiedVersions = new Dictionary<string, long>();
                    var copiedDots = new Dictionary<string, ISet<long>>();
                    foreach (var kvp in shardKvp.Value.Versions)
                    {
                        copiedVersions[kvp.Key] = kvp.Value;
                    }
                    if (shardKvp.Value.Dots != null)
                    {
                        foreach (var kvp in shardKvp.Value.Dots)
                        {
                            copiedDots[kvp.Key] = new HashSet<long>(kvp.Value);
                        }
                    }
                    globalState[shardKvp.Key] = new DottedVersionVector(copiedVersions, copiedDots);
                }
            }

            var syncMsg = new CrdtStateSyncMessage(replicaContext.ReplicaId, globalState);
            var payload = serializer.SerializeToBytes(syncMsg);
            
            var wrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", payload);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            await directSender.SendDirectAsync(targetPeerId, finalBytes, cancellationToken).ConfigureAwait(false);
            
            antiEntropySyncCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaContext.ReplicaId));
            logger.LogTrace("Dispatched targeted global DVV cluster state sync to expected peer {PeerId}.", targetPeerId.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to dispatch point-to-point cluster state sync.");
        }
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
                logger.LogError(ex, "Error notifying orchestrator topology observer.");
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
                            await ProcessSyncDocumentsInternalAsync(cmd.CancellationToken).ConfigureAwait(false);
                            break;
                        case OrchestratorCommandType.ReevaluateTopology:
                            await ProcessSyncDocumentsInternalAsync(cmd.CancellationToken).ConfigureAwait(false);
                            break;
                    }

                    cmd.SetResult();
                    commandsProcessedCounter.Add(1);
                }
                catch (OperationCanceledException)
                {
                    cmd.SetException(new OperationCanceledException("Orchestrator operation cancelled."));
                }
                catch (Exception ex)
                {
                    commandFailuresCounter.Add(1);
                    cmd.SetException(ex);
                }
            }
        }
        catch (OperationCanceledException) { /* Clean graceful termination */ }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Distributed CRDT Orchestrator lock-free processing loop critically faulted.");
        }
    }

    private async Task ProcessInitializeInternalAsync(CancellationToken cancellationToken)
    {
        var registryState = new CrdtRegistryState();
        Registry = ActivatorUtilities.CreateInstance<DistributedCrdtDocument<CrdtRegistryState>>(serviceProvider, registryState);
        
        await Registry.InitializeAsync(cancellationToken).ConfigureAwait(false);
        
        await ProcessSyncDocumentsInternalAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task ProcessSyncDocumentsInternalAsync(CancellationToken cancellationToken)
    {
        try
        {
            var registry = Registry ?? throw new InvalidOperationException("Orchestrator registry is not initialized. Ensure InitializeAsync is called.");
            var registryDoc = await registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            var registryMap = registryDoc.Data.Documents;
            bool changed = false;
            
            var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();

            foreach (var kvp in registryMap)
            {
                if (!kvp.Value.IsDeleted)
                {
                    if (!activeDocuments.ContainsKey(kvp.Key))
                    {
                        var factory = serviceProvider.GetKeyedService<IDocumentFactory>(kvp.Value.TypeAlias);
                        if (factory != null)
                        {
                            var doc = factory.CreateDocument(serviceProvider, kvp.Key);
                            
                            if (await distributionManager.IsShardLocalAsync(doc.ShardId, cancellationToken).ConfigureAwait(false))
                            {
                                await doc.InitializeAsync(cancellationToken).ConfigureAwait(false);
                                
                                if (activeDocuments.TryAdd(kvp.Key, doc))
                                {
                                    changed = true;
                                    documentCreatedCounter.Add(1, new KeyValuePair<string, object?>("document_id", kvp.Key));
                                    logger.LogInformation("Orchestrator dynamically mapped new active CRDT document: {DocumentId}", kvp.Key);
                                    
                                    lock (replicaContext.ShardVersionVectors)
                                    {
                                        if (!replicaContext.ShardVersionVectors.ContainsKey(doc.ShardId))
                                        {
                                            replicaContext.ShardVersionVectors[doc.ShardId] = new DottedVersionVector();
                                        }
                                    }
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
                            else
                            {
                                try
                                {
                                    if (doc is IDisposable d) d.Dispose();
                                }
                                catch (ObjectDisposedException) { }
                            }
                        }
                        else
                        {
                            logger.LogWarning("Missing AOT explicit type factory for CRDT alias: {TypeAlias}", kvp.Value.TypeAlias);
                        }
                    }
                    else if (activeDocuments.TryGetValue(kvp.Key, out var activeDoc))
                    {
                        if (!await distributionManager.IsShardLocalAsync(activeDoc.ShardId, cancellationToken).ConfigureAwait(false))
                        {
                            if (activeDocuments.TryRemove(kvp.Key, out _))
                            {
                                var demotedShardId = activeDoc.ShardId;
                                try
                                {
                                    if (activeDoc is IDisposable d) d.Dispose();
                                }
                                catch (ObjectDisposedException) { }
                                
                                bool shardStillInUse = false;
                                foreach (var remainingDoc in activeDocuments.Values)
                                {
                                    if (string.Equals(remainingDoc.ShardId, demotedShardId, StringComparison.Ordinal))
                                    {
                                        shardStillInUse = true;
                                        break;
                                    }
                                }
                                
                                if (!shardStillInUse)
                                {
                                    lock (replicaContext.ShardVersionVectors)
                                    {
                                        replicaContext.ShardVersionVectors.Remove(demotedShardId);
                                    }
                                }

                                changed = true;
                                logger.LogInformation("Orchestrator demoted transitioning CRDT document away eliminating footprint: {DocumentId}", kvp.Key);
                            }
                        }
                    }
                }
            }

            var toRemove = new List<string>();

            foreach (var active in activeDocuments)
            {
                if (!registryMap.TryGetValue(active.Key, out var entry) || entry.IsDeleted)
                {
                    toRemove.Add(active.Key);
                }
            }

            foreach (var docId in toRemove)
            {
                if (activeDocuments.TryRemove(docId, out var doc))
                {
                    var shardId = doc.ShardId;
                    try
                    {
                        if (doc is IDisposable d) d.Dispose();
                    }
                    catch (ObjectDisposedException) { }
                    await storage.DeleteDocumentAsync(shardId, docId, cancellationToken).ConfigureAwait(false);
                    
                    bool shardStillInUse = false;
                    foreach (var remainingDoc in activeDocuments.Values)
                    {
                        if (string.Equals(remainingDoc.ShardId, shardId, StringComparison.Ordinal))
                        {
                            shardStillInUse = true;
                            break;
                        }
                    }
                    
                    if (!shardStillInUse)
                    {
                        lock (replicaContext.ShardVersionVectors)
                        {
                            replicaContext.ShardVersionVectors.Remove(shardId);
                        }
                    }

                    changed = true;
                    documentDeletedCounter.Add(1, new KeyValuePair<string, object?>("document_id", docId));
                    logger.LogInformation("Orchestrator tombstoned active CRDT document: {DocumentId}", docId);
                }
            }

            if (changed)
            {
                await NotifyTopologyChangedAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An error occurred synchronizing dynamic P2P orchestrator matrices.");
            throw;
        }
    }

    private async ValueTask<string> GetShardIdAsync<TState>(string documentId, CancellationToken cancellationToken) where TState : class, new()
    {
        var activeDoc = TryGetLocalDocument<TState>(documentId);
        if (activeDoc != null)
        {
            return activeDoc.ShardId;
        }

        if (Registry != null)
        {
            var registryDoc = await Registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            if (registryDoc.Data.Documents.TryGetValue(documentId, out var entry))
            {
                var factory = serviceProvider.GetKeyedService<IDocumentFactory>(entry.TypeAlias);
                if (factory != null)
                {
                    var doc = factory.CreateDocument(serviceProvider, documentId);
                    var shardId = doc.ShardId;
                    if (doc is IDisposable d) d.Dispose();
                    return shardId;
                }
            }
        }

        var factoryFallback = serviceProvider.GetKeyedService<IDocumentFactory>(typeof(TState).Name);
        if (factoryFallback != null)
        {
            var doc = factoryFallback.CreateDocument(serviceProvider, documentId);
            var shardId = doc.ShardId;
            if (doc is IDisposable d) d.Dispose();
            return shardId;
        }

        var metadataManager = serviceProvider.GetRequiredService<ICrdtMetadataManager>();
        var shardIdProvider = serviceProvider.GetRequiredService<IShardIdProvider>();
        
        var initialState = new TState();
        var metadata = metadataManager.Initialize(initialState);
        var dummyDoc = new CrdtDocument<TState>(initialState, metadata);
        return shardIdProvider.GetShardId(dummyDoc) ?? documentId;
    }

    public void Dispose()
    {
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
        
        meter.Dispose();
    }
}