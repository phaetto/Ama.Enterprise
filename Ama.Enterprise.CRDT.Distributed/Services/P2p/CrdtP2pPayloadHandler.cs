namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Journaling;
using Ama.CRDT.Services.Serialization;
using Ama.CRDT.Services.Versioning;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

/// <summary>
/// Deserializes incoming network application payloads.
/// </summary>
public sealed class CrdtP2pPayloadHandler : IApplicationPayloadHandler, IDisposable
{
    private readonly string replicaId;
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<CrdtP2pPayloadHandler> logger;

    private readonly Meter meter;
    private readonly Counter<long> messagesProcessedCounter;
    private readonly Counter<long> operationsReceivedCounter;
    private readonly Counter<long> snapshotsMergedCounter;
    private readonly Counter<long> rebootsHandledCounter;
    
    private readonly Counter<long> rpcRequestsProcessedCounter;
    private readonly Counter<long> stateSyncsProcessedCounter;
    private readonly Counter<long> patchesProcessedCounter;

    private readonly ConcurrentDictionary<(string PeerReplicaId, string ShardId), SnapshotDispatchState> recentSnapshotDispatches = new();

    public CrdtP2pPayloadHandler(
        string replicaId,
        DistributedCrdtScopeManager scopeManager,
        ICrdtSerializer serializer,
        ILogger<CrdtP2pPayloadHandler> logger,
        IMeterFactory? meterFactory = null)
    {
        this.replicaId = replicaId ?? throw new ArgumentNullException(nameof(replicaId));
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtP2pPayloadHandler") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtP2pPayloadHandler");
        this.messagesProcessedCounter = this.meter.CreateCounter<long>("crdt.payloads.processed", "messages", "Total network payload wrappers intercepted");
        this.operationsReceivedCounter = this.meter.CreateCounter<long>("crdt.payloads.operations_received", "operations", "Total operations received");
        this.snapshotsMergedCounter = this.meter.CreateCounter<long>("crdt.payloads.snapshots_merged", "snapshots", "Total fallback snapshots merged");
        this.rebootsHandledCounter = this.meter.CreateCounter<long>("crdt.payloads.reboots_handled", "events", "Total eviction rejection payloads handled");

        this.rpcRequestsProcessedCounter = this.meter.CreateCounter<long>("crdt.payloads.rpc_processed", "requests", "Total RPC payloads processed");
        this.stateSyncsProcessedCounter = this.meter.CreateCounter<long>("crdt.payloads.syncs_processed", "syncs", "Total state sync payloads processed");
        this.patchesProcessedCounter = this.meter.CreateCounter<long>("crdt.payloads.patches_processed", "patches", "Total patch payloads processed");
    }

    /// <inheritdoc />
    public async Task HandlePayloadAsync(string meshId, PeerId senderId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (payload.IsEmpty)
        {
            return;
        }

        CrdtMessageWrapper wrapper;
        try
        {
            wrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(payload.ToArray());
        }
        catch (Exception)
        {
            return;
        }

        if (string.IsNullOrEmpty(wrapper.MessageType) || wrapper.Payload == null)
        {
            return;
        }

        var scope = scopeManager.GetOrCreateScope(replicaId);

        if (senderId.Value != Guid.Empty)
        {
            var topologyProvider = scope.ServiceProvider.GetService<IScopeTopologyProvider>();
            if (topologyProvider != null)
            {
                var isExpected = await topologyProvider.IsPeerExpectedAsync(senderId.Value.ToString(), cancellationToken).ConfigureAwait(false);
                if (!isExpected)
                {
                    logger.LogWarning(
                        "[{ReplicaId}] Dropped inbound {MessageType} payload from unexpected peer '{PeerId}'.",
                        scope.ReplicaId,
                        wrapper.MessageType,
                        senderId.Value);
                    return;
                }
            }
        }

        messagesProcessedCounter.Add(1, new KeyValuePair<string, object?>("message_type", wrapper.MessageType));

        if (wrapper.MessageType == "CrdtReadReq" || wrapper.MessageType == "CrdtReadRes" || 
            wrapper.MessageType == "CrdtIntentReq" || wrapper.MessageType == "CrdtIntentRes" ||
            wrapper.MessageType == "CrdtMapReduceReq" || wrapper.MessageType == "CrdtMapReduceRes")
        {
            await ProcessProxyRpcPayloadsAsync(scope, meshId, wrapper, senderId, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (wrapper.MessageType == "CrdtEviction")
        {
            await ProcessEvictionRejectionAsync(wrapper, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (wrapper.MessageType == "CrdtSync")
        {
            await ProcessStateSyncAsync(meshId, wrapper, senderId, cancellationToken).ConfigureAwait(false);
            return;
        }

        var targetDoc = await ResolveLocalDocumentAsync(scope, wrapper.DocumentId, wrapper.DocumentTypeAlias, cancellationToken).ConfigureAwait(false);

        if (targetDoc == null)
        {
            if (wrapper.MessageType == "CrdtSnapshot")
            {
                RecordSkippedSnapshotProgress(scope, wrapper, senderId);
            }
            return;
        }

        if (wrapper.MessageType == "CrdtPatch")
        {
            await ProcessPatchAsync(scope, targetDoc, meshId, wrapper, senderId, cancellationToken).ConfigureAwait(false);
        }
        else if (wrapper.MessageType == "CrdtSnapshot")
        {
            await ProcessSnapshotAsync(scope, targetDoc, meshId, wrapper, senderId, cancellationToken).ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        meter.Dispose();
    }

    private static ValueTask<IDistributedCrdtDocument?> ResolveLocalDocumentAsync(IDistributedCrdtScope scope, string? documentId, string? documentTypeAlias, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(documentId))
        {
            return new ValueTask<IDistributedCrdtDocument?>((IDistributedCrdtDocument?)null);
        }

        return scope.Orchestrator.ResolveOrPromoteLocalDocumentAsync(documentId, documentTypeAlias, cancellationToken);
    }

    private async Task ProcessProxyRpcPayloadsAsync(IDistributedCrdtScope scope, string meshId, CrdtMessageWrapper wrapper, PeerId senderId, CancellationToken cancellationToken)
    {
        try
        {
            rpcRequestsProcessedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
            
            if (wrapper.MessageType == "CrdtReadReq")
            {
                var req = serializer.DeserializeFromBytes<CrdtReadRequestDto>(wrapper.Payload!);
                var typeAlias = !string.IsNullOrWhiteSpace(req.DocumentTypeAlias) ? req.DocumentTypeAlias : wrapper.DocumentTypeAlias;
                IDistributedCrdtDocument? targetDoc = null;

                if (!string.IsNullOrWhiteSpace(req.DocumentId) &&
                    scope.Orchestrator.InitializationStage == OrchestratorInitializationStage.Initialized)
                {
                    // Fast path: check if the document is already active in local memory without invoking a remote promotion or hydration loop
                    var activeDocs = scope.Orchestrator.GetActiveDocuments();
                    for (int i = 0; i < activeDocs.Count; i++)
                    {
                        var doc = activeDocs[i];
                        if (string.Equals(doc.DocumentId, req.DocumentId, StringComparison.Ordinal) &&
                            (string.IsNullOrWhiteSpace(typeAlias) || string.Equals(doc.TypeAlias, typeAlias, StringComparison.Ordinal)))
                        {
                            targetDoc = doc;
                            break;
                        }
                    }
                }

                byte[] statePayload = [];
                if (targetDoc != null)
                {
                    var snapshot = await targetDoc.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
                    statePayload = snapshot.SnapshotData;
                }
                else if (!string.IsNullOrWhiteSpace(req.DocumentId))
                {
                    statePayload = await scope.Orchestrator.TryLoadStoredDocumentSnapshotBytesAsync(req.DocumentId, typeAlias, cancellationToken).ConfigureAwait(false);
                    if (statePayload.Length > 0)
                    {
                        targetDoc = await scope.Orchestrator.ResolveOrPromoteLocalDocumentAsync(req.DocumentId, typeAlias, cancellationToken).ConfigureAwait(false);
                        if (targetDoc != null)
                        {
                            var snapshot = await targetDoc.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
                            statePayload = snapshot.SnapshotData;
                        }
                    }
                }

                var res = new CrdtReadResponseDto { CorrelationId = req.CorrelationId, StatePayload = statePayload };
                var resPayload = serializer.SerializeToBytes(res);
                var resWrapper = new CrdtMessageWrapper(req.DocumentId, "CrdtReadRes", resPayload, typeAlias);
                var directSender = scope.ServiceProvider.GetRequiredService<IDirectMessageSender>();
                await directSender.SendDirectAsync(senderId, serializer.SerializeToBytes(resWrapper), cancellationToken).ConfigureAwait(false);
            }
            else if (wrapper.MessageType == "CrdtReadRes")
            {
                var res = serializer.DeserializeFromBytes<CrdtReadResponseDto>(wrapper.Payload!);
                var rpcCorrelator = scope.ServiceProvider.GetRequiredService<IP2pRpcCorrelator>();
                rpcCorrelator.ResolveRequest(res.CorrelationId, wrapper.Payload!);
            }
            else if (wrapper.MessageType == "CrdtIntentReq")
            {
                var req = serializer.DeserializeFromBytes<CrdtIntentForwardRequestDto>(wrapper.Payload!);
                var typeAlias = !string.IsNullOrWhiteSpace(req.DocumentTypeAlias) ? req.DocumentTypeAlias : wrapper.DocumentTypeAlias;
                var targetDoc = await ResolveLocalDocumentAsync(scope, req.DocumentId, typeAlias, cancellationToken).ConfigureAwait(false);
                var isFireAndForget = req.CorrelationId == Guid.Empty;
                var deliveryMode = isFireAndForget ? IntentDeliveryMode.FireAndForget : IntentDeliveryMode.Acknowledged;
                
                var isSuccess = false;
                var errorMessage = string.Empty;
                if (targetDoc != null)
                {
                    try 
                    {
                        var intent = serializer.DeserializeFromBytes<IOperationIntent>(req.SerializedIntent);
                        var patcher = scope.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
                        
                        isSuccess = await targetDoc.ApplyRemoteIntentAsync(intent, req.TargetProperty, patcher, deliveryMode, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        errorMessage = ex.Message;
                        logger.LogError(ex, "Failed to apply forwarded intent on target document {DocumentId}.", req.DocumentId);
                    }
                }
                else
                {
                    errorMessage = $"Target document '{req.DocumentId}' is not active on this node.";
                }

                if (!isFireAndForget)
                {
                    var res = new CrdtIntentForwardResponseDto { CorrelationId = req.CorrelationId, IsSuccess = isSuccess, ErrorMessage = errorMessage };
                    var resPayload = serializer.SerializeToBytes(res);
                    var resWrapper = new CrdtMessageWrapper(req.DocumentId, "CrdtIntentRes", resPayload);
                    var directSender = scope.ServiceProvider.GetRequiredService<IDirectMessageSender>();
                    await directSender.SendDirectAsync(senderId, serializer.SerializeToBytes(resWrapper), cancellationToken).ConfigureAwait(false);
                }
            }
            else if (wrapper.MessageType == "CrdtIntentRes")
            {
                var res = serializer.DeserializeFromBytes<CrdtIntentForwardResponseDto>(wrapper.Payload!);
                var rpcCorrelator = scope.ServiceProvider.GetRequiredService<IP2pRpcCorrelator>();
                rpcCorrelator.ResolveRequest(res.CorrelationId, wrapper.Payload!);
            }
            else if (wrapper.MessageType == "CrdtMapReduceReq")
            {
                var req = serializer.DeserializeFromBytes<CrdtMapReduceRequestDto>(wrapper.Payload!);
                var typeAlias = !string.IsNullOrWhiteSpace(req.DocumentTypeAlias) ? req.DocumentTypeAlias : wrapper.DocumentTypeAlias;
                CrdtMapReduceResponseDto responseDto;

                var executor = scope.ServiceProvider.GetKeyedService<ICrdtMapReduceExecutor>(req.JobAlias);
                if (executor is null)
                {
                    responseDto = new CrdtMapReduceResponseDto
                    {
                        CorrelationId = req.CorrelationId,
                        IsSuccess = false,
                        HasAccumulator = false,
                        EvaluatedDocumentCount = 0,
                        SerializedAccumulator = Array.Empty<byte>(),
                        ErrorMessage = $"Map/Reduce job alias '{req.JobAlias}' is not registered on replica '{scope.ReplicaId}'."
                    };
                }
                else
                {
                    try
                    {
                        var docList = req.DocumentIds as IReadOnlyList<string> ?? req.DocumentIds.ToList();
                        var outcome = await executor.ExecuteSerializedLocalBatchAsync(
                            scope.ServiceProvider,
                            docList,
                            req.SerializedInput,
                            cancellationToken).ConfigureAwait(false);

                        responseDto = new CrdtMapReduceResponseDto
                        {
                            CorrelationId = req.CorrelationId,
                            IsSuccess = outcome.IsSuccess,
                            HasAccumulator = outcome.HasAccumulator,
                            EvaluatedDocumentCount = outcome.EvaluatedDocumentCount,
                            SerializedAccumulator = outcome.SerializedAccumulator,
                            MissingDocumentIds = outcome.MissingDocumentIds,
                            ErrorMessage = outcome.ErrorMessage
                        };
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to execute local Map/Reduce batch for job {JobAlias}.", req.JobAlias);
                        responseDto = new CrdtMapReduceResponseDto
                        {
                            CorrelationId = req.CorrelationId,
                            IsSuccess = false,
                            HasAccumulator = false,
                            EvaluatedDocumentCount = 0,
                            SerializedAccumulator = Array.Empty<byte>(),
                            ErrorMessage = ex.Message
                        };
                    }
                }

                var resPayload = serializer.SerializeToBytes(responseDto);
                var resWrapper = new CrdtMessageWrapper(wrapper.DocumentId, "CrdtMapReduceRes", resPayload, typeAlias);
                var directSender = scope.ServiceProvider.GetRequiredService<IDirectMessageSender>();
                await directSender.SendDirectAsync(senderId, serializer.SerializeToBytes(resWrapper), cancellationToken).ConfigureAwait(false);
            }
            else if (wrapper.MessageType == "CrdtMapReduceRes")
            {
                var res = serializer.DeserializeFromBytes<CrdtMapReduceResponseDto>(wrapper.Payload!);
                var rpcCorrelator = scope.ServiceProvider.GetRequiredService<IP2pRpcCorrelator>();
                rpcCorrelator.ResolveRequest(res.CorrelationId, wrapper.Payload!);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process asynchronous RPC payload.");
        }
    }

    private async Task ProcessStateSyncAsync(string meshId, CrdtMessageWrapper wrapper, PeerId senderId, CancellationToken cancellationToken)
    {
        try
        {
            var scope = scopeManager.GetOrCreateScope(replicaId);
            var syncMsg = serializer.DeserializeFromBytes<CrdtStateSyncMessage>(wrapper.Payload!);

            if (syncMsg.ReplicaId != null && scope.ClusterTracker.IsReplicaTombstoned(syncMsg.ReplicaId))
            {
                await RejectEvictedReplicaAsync(scope, meshId, scope.Orchestrator.Registry.DocumentId, syncMsg.ReplicaId, cancellationToken).ConfigureAwait(false);
                return;
            }

            stateSyncsProcessedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
            var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
            var versionVectorManager = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
            var directSender = scope.ServiceProvider.GetService<IDirectMessageSender>();
            var syncService = scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
            var journalManager = scope.ServiceProvider.GetRequiredService<IJournalManager>();
            var distributionManager = scope.ServiceProvider.GetRequiredService<IShardDistributionManager>();

            if (string.IsNullOrEmpty(syncMsg.ReplicaId) || syncMsg.State == null)
            {
                return;
            }

            if (string.Equals(syncMsg.ReplicaId, replicaContext.ReplicaId, StringComparison.Ordinal))
            {
                if (IsLocalLoopbackSender(scope, meshId, senderId))
                {
                    return;
                }

                logger.LogCritical(
                    "[{ReplicaId}] Duplicate replica ID collision detected! Received CrdtSync message from remote peer '{PeerId}' claiming the same ReplicaId '{DuplicateReplicaId}'. Ignoring payload to prevent DVV corruption.",
                    replicaContext.ReplicaId,
                    senderId.Value,
                    syncMsg.ReplicaId);
                return;
            }

            scope.ClusterTracker.UpdatePeerState(syncMsg.ReplicaId, senderId.Value.ToString(), syncMsg.State);

            var concreteOrchestrator = scope.Orchestrator as CrdtDocumentOrchestrator;
            var safeLocalState = await versionVectorManager.GetFullSnapshotAsync(cancellationToken).ConfigureAwait(false);

            var allShardsToEvaluate = new HashSet<string>(StringComparer.Ordinal);
            if (syncMsg.IsTopologyAnnouncementOnly)
            {
                var registryShardId = scope.Orchestrator.Registry?.ShardId;
                if (!string.IsNullOrWhiteSpace(registryShardId))
                {
                    allShardsToEvaluate.Add(registryShardId);
                }
            }
            else
            {
                foreach (var remoteShardId in syncMsg.State.Keys)
                {
                    allShardsToEvaluate.Add(remoteShardId);
                }

                foreach (var localShardId in safeLocalState.Keys)
                {
                    var targetNodes = await distributionManager.GetTargetNodesForShardAsync(localShardId, cancellationToken).ConfigureAwait(false);
                    if (targetNodes.Contains(syncMsg.ReplicaId, StringComparer.Ordinal))
                    {
                        allShardsToEvaluate.Add(localShardId);
                    }
                }
            }

            var documents = scope.Orchestrator.GetActiveDocuments();
            var localActiveShards = new HashSet<string>(StringComparer.Ordinal);
            foreach (var doc in documents)
            {
                localActiveShards.Add(doc.ShardId);
            }

            foreach (var knownLocalShard in safeLocalState.Keys)
            {
                localActiveShards.Add(knownLocalShard);
            }

            allShardsToEvaluate.IntersectWith(localActiveShards);

            var acquiredShardScopes = new List<IDisposable>(allShardsToEvaluate.Count);
            try
            {
                if (concreteOrchestrator != null)
                {
                    var availableShards = new HashSet<string>(StringComparer.Ordinal);
                    foreach (var shardId in allShardsToEvaluate)
                    {
                        var shardScope = concreteOrchestrator.TryAcquireAntiEntropyWorkScope(shardId);
                        if (shardScope != null)
                        {
                            acquiredShardScopes.Add(shardScope);
                            availableShards.Add(shardId);
                        }
                    }

                    if (allShardsToEvaluate.Count > 0 && availableShards.Count == 0)
                    {
                        logger.LogDebug(
                            "[{ReplicaId}] Deferring incoming CrdtSync response to peer '{PeerReplicaId}' because all {ShardCount} target shard(s) or local queues are already busy.",
                            replicaContext.ReplicaId,
                            syncMsg.ReplicaId,
                            allShardsToEvaluate.Count);
                        return;
                    }

                    allShardsToEvaluate = availableShards;
                }

                var requirements = new List<ReplicaSyncRequirement>();
                var missingOpsStreams = new List<IAsyncEnumerable<JournaledOperation>>();
                var shardsWithNoCausalOverlap = new HashSet<string>(StringComparer.Ordinal);

                foreach (var shardId in allShardsToEvaluate)
                {
                    var sourceVector = syncMsg.State.TryGetValue(shardId, out var sv) ? sv : new DottedVersionVector();
                    var targetVector = safeLocalState.TryGetValue(shardId, out var tv) ? tv : new DottedVersionVector();

                    var req = syncService.CalculateRequirement(syncMsg.ReplicaId, sourceVector, replicaContext.ReplicaId, targetVector, shardId);
                    if (req.IsBehind)
                    {
                        requirements.Add(req);
                        missingOpsStreams.Add(journalManager.GetMissingOperationsAsync(shardId, req, cancellationToken));
                    }

                    // If the sender has zero causal history for a shard that we actively host AND the sender is
                    // an assigned replica for that shard, check if we have active non-empty documents for that shard
                    // (e.g., hydrated from a checkpoint whose DVV was not yet populated or where the peer just took ownership).
                    if (!req.IsBehind && IsVersionVectorEmpty(sourceVector))
                    {
                        var registryDocInstance = scope.Orchestrator.Registry;
                        if (syncMsg.IsTopologyAnnouncementOnly &&
                            registryDocInstance != null &&
                            string.Equals(shardId, registryDocInstance.ShardId, StringComparison.Ordinal))
                        {
                            var localRegistryDoc = await registryDocInstance.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
                            if (localRegistryDoc.Data?.Documents is { Count: > 0 })
                            {
                                shardsWithNoCausalOverlap.Add(shardId);
                            }
                        }
                        else if (!syncMsg.IsTopologyAnnouncementOnly)
                        {
                            var targetNodes = await distributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
                            if (targetNodes.Contains(syncMsg.ReplicaId, StringComparer.Ordinal))
                            {
                                for (int d = 0; d < documents.Count; d++)
                                {
                                    if (string.Equals(documents[d].ShardId, shardId, StringComparison.Ordinal) &&
                                        !string.Equals(documents[d].DocumentId, scope.Orchestrator.Registry.DocumentId, StringComparison.Ordinal))
                                    {
                                        shardsWithNoCausalOverlap.Add(shardId);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                if (requirements.Count == 0 && shardsWithNoCausalOverlap.Count == 0)
                {
                    return;
                }

                var shardsRequiringSnapshot = new HashSet<string>(shardsWithNoCausalOverlap, StringComparer.Ordinal);
                var allOps = new List<JournaledOperation>();

                for (int i = 0; i < requirements.Count; i++)
                {
                    var syncResult = await syncService.EvaluateJournalCompletionAsync(missingOpsStreams[i], requirements[i], cancellationToken).ConfigureAwait(false);
                    if (syncResult.SnapshotRequired)
                    {
                        shardsRequiringSnapshot.Add(requirements[i].ShardId);
                    }
                    else
                    {
                        allOps.AddRange(syncResult.Operations);
                    }
                }

                if (shardsRequiringSnapshot.Count > 0)
                {
                    var nowTicks = Environment.TickCount64;
                    var options = scope.ServiceProvider.GetService<Microsoft.Extensions.Options.IOptions<DistributedCrdtOptions>>()?.Value;
                    var baseCooldownMs = Math.Max(5000L, (options?.AntiEntropyStartupIntervalSeconds ?? 2) * 2000L);

                    shardsRequiringSnapshot.RemoveWhere(shardId =>
                    {
                        var key = (syncMsg.ReplicaId, shardId);
                        if (!recentSnapshotDispatches.TryGetValue(key, out var prevDispatch))
                        {
                            return false;
                        }

                        if (nowTicks - prevDispatch.DispatchedAtTicks >= prevDispatch.CooldownMs)
                        {
                            return false;
                        }

                        if (!safeLocalState.TryGetValue(shardId, out var currentLocalDvv))
                        {
                            return true;
                        }

                        // Suppress redundant snapshot re-sends within the cooldown window if our local shard DVV
                        // has not advanced beyond the snapshot DVV we already just sent to this peer.
                        var deltaReq = syncService.CalculateRequirement(
                            syncMsg.ReplicaId,
                            prevDispatch.DispatchedDvv,
                            replicaContext.ReplicaId,
                            currentLocalDvv,
                            shardId);

                        return !deltaReq.IsBehind;
                    });

                    if (shardsRequiringSnapshot.Count > 0)
                    {
                        logger.LogWarning("[{ReplicaId}] Journal missing or trimmed for {ShardCount} shard(s). Sending full snapshots for affected shards.", replicaContext.ReplicaId, shardsRequiringSnapshot.Count);
                        var sentSnapshots = new HashSet<string>(StringComparer.Ordinal);
                        var shardDocsToSend = new Dictionary<string, List<(string DocumentId, string? TypeAlias, string CompositeKey)>>(StringComparer.Ordinal);
                        var shardDocumentKeyLists = new Dictionary<string, List<string>>(StringComparer.Ordinal);
                        var dispatchedShardState = new ShardVersionVectorMap();

                        // Always include the system registry document when sending full shard snapshots so the recipient
                        // knows about all document entries and type aliases in the cluster.
                        var registryDocument = scope.Orchestrator.Registry;
                        if (registryDocument != null)
                        {
                            shardsRequiringSnapshot.Add(registryDocument.ShardId);
                        }

                        foreach (var targetDoc in documents)
                        {
                            if (shardsRequiringSnapshot.Contains(targetDoc.ShardId))
                            {
                                var isRegistryDoc = string.Equals(targetDoc.DocumentId, scope.Orchestrator.Registry.DocumentId, StringComparison.Ordinal);
                                var compositeKey = isRegistryDoc
                                    ? targetDoc.DocumentId
                                    : CrdtRegistryEntry.GetRegistryKey(targetDoc.TypeAlias, targetDoc.DocumentId);

                                if (sentSnapshots.Add(compositeKey))
                                {
                                    if (!shardDocsToSend.TryGetValue(targetDoc.ShardId, out var docList))
                                    {
                                        docList = new List<(string DocumentId, string? TypeAlias, string CompositeKey)>();
                                        shardDocsToSend[targetDoc.ShardId] = docList;
                                    }
                                    docList.Add((targetDoc.DocumentId, targetDoc.TypeAlias, compositeKey));

                                    if (!shardDocumentKeyLists.TryGetValue(targetDoc.ShardId, out var keyList))
                                    {
                                        keyList = new List<string>();
                                        shardDocumentKeyLists[targetDoc.ShardId] = keyList;
                                    }
                                    keyList.Add(compositeKey);
                                }
                            }
                        }

                        var registryDoc = await scope.Orchestrator.Registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
                        foreach (var entry in registryDoc.Data.Documents.Values)
                        {
                            if (entry.IsDeleted)
                            {
                                continue;
                            }

                            var compositeKey = CrdtRegistryEntry.GetRegistryKey(entry.TypeAlias, entry.DocumentId);
                            if (sentSnapshots.Contains(compositeKey))
                            {
                                continue;
                            }

                            var entryShardId = await scope.Orchestrator.TryGetShardIdAsync(entry.DocumentId, entry.TypeAlias, cancellationToken).ConfigureAwait(false);
                            if (!string.IsNullOrWhiteSpace(entryShardId) && shardsRequiringSnapshot.Contains(entryShardId))
                            {
                                sentSnapshots.Add(compositeKey);

                                if (!shardDocsToSend.TryGetValue(entryShardId, out var docList))
                                {
                                    docList = new List<(string DocumentId, string? TypeAlias, string CompositeKey)>();
                                    shardDocsToSend[entryShardId] = docList;
                                }
                                docList.Add((entry.DocumentId, entry.TypeAlias, compositeKey));

                                if (!shardDocumentKeyLists.TryGetValue(entryShardId, out var keyList))
                                {
                                    keyList = new List<string>();
                                    shardDocumentKeyLists[entryShardId] = keyList;
                                }
                                keyList.Add(compositeKey);
                            }
                        }

                        // Capture a consistent shard DVV once per shard right before sending that shard's batch of document snapshots.
                        // Send the system registry shard first so the recipient has registry metadata before user documents arrive.
                        var latestSnapshotState = await versionVectorManager.GetShardsSnapshotAsync(shardsRequiringSnapshot, cancellationToken).ConfigureAwait(false);
                        var orderedShards = shardDocsToSend
                            .OrderByDescending(kvp => registryDocument != null && string.Equals(kvp.Key, registryDocument.ShardId, StringComparison.Ordinal))
                            .ToList();

                        foreach (var shardKvp in orderedShards)
                        {
                            var shardId = shardKvp.Key;
                            var docsInShard = shardKvp.Value;
                            shardDocumentKeyLists.TryGetValue(shardId, out var batchKeys);

                            // Scale cooldown proportionally to the number of documents in the batch (at least 25ms per doc, up to 5 minutes)
                            // so large batches have enough time to transfer and merge on the recipient before another snapshot wave can trigger.
                            var startupSec = options?.AntiEntropyStartupIntervalSeconds ?? 2;
                            var baseDynamicCooldownMs = startupSec <= 1 ? 500L : Math.Max(500L, startupSec * 1000L);
                            var dynamicCooldownMs = startupSec <= 1
                                ? baseDynamicCooldownMs
                                : Math.Clamp(baseDynamicCooldownMs + (docsInShard.Count * 25L), baseDynamicCooldownMs, 300_000L);

                            ShardVersionVectorMap? consistentShardState = null;
                            if (latestSnapshotState.TryGetValue(shardId, out var shardDvv) ||
                                safeLocalState.TryGetValue(shardId, out shardDvv))
                            {
                                consistentShardState = new ShardVersionVectorMap { [shardId] = shardDvv };
                                dispatchedShardState[shardId] = shardDvv;
                                recentSnapshotDispatches[(syncMsg.ReplicaId, shardId)] = new SnapshotDispatchState(Environment.TickCount64, dynamicCooldownMs, shardDvv);
                            }

                            for (int i = 0; i < docsInShard.Count; i++)
                            {
                                var item = docsInShard[i];
                                await scope.Orchestrator.ProvideSnapshotAsync(
                                    item.DocumentId,
                                    item.TypeAlias,
                                    syncMsg.ReplicaId,
                                    senderId,
                                    batchKeys,
                                    consistentShardState,
                                    cancellationToken).ConfigureAwait(false);

                                // Refresh dispatch timestamp as we stream large batches so the cooldown starts after the stream completes.
                                if (consistentShardState != null && (i + 1) % 25 == 0 && consistentShardState.TryGetValue(shardId, out var refreshedDvv))
                                {
                                    recentSnapshotDispatches[(syncMsg.ReplicaId, shardId)] = new SnapshotDispatchState(Environment.TickCount64, dynamicCooldownMs, refreshedDvv);
                                }
                            }
                        }

                        if (dispatchedShardState.Count > 0)
                        {
                            scope.ClusterTracker.UpdatePeerState(syncMsg.ReplicaId, senderId.Value.ToString(), dispatchedShardState);
                        }
                    }
                }

                if (allOps.Count > 0 && directSender != null)
                {
                    var opsByDoc = allOps
                        .GroupBy(x => (Key: x.DocumentId?.ToString() ?? string.Empty, ShardId: x.Operation.ShardId ?? string.Empty))
                        .ToDictionary(g => g.Key, g => g.Select(x => x.Operation).ToArray());

                    var sentBucketKeys = new HashSet<(string Key, string ShardId)>();

                    foreach (var targetDoc in documents)
                    {
                        if (shardsRequiringSnapshot.Contains(targetDoc.ShardId))
                        {
                            continue;
                        }

                        var compositeKey = CrdtRegistryEntry.GetRegistryKey(targetDoc.TypeAlias, targetDoc.DocumentId);
                        CrdtOperation[]? docOps = null;
                        if (opsByDoc.TryGetValue((compositeKey, targetDoc.ShardId), out docOps))
                        {
                            sentBucketKeys.Add((compositeKey, targetDoc.ShardId));
                        }
                        else if (opsByDoc.TryGetValue((targetDoc.DocumentId, targetDoc.ShardId), out docOps))
                        {
                            sentBucketKeys.Add((targetDoc.DocumentId, targetDoc.ShardId));
                        }
                        else if (opsByDoc.TryGetValue((compositeKey, string.Empty), out docOps))
                        {
                            sentBucketKeys.Add((compositeKey, string.Empty));
                        }
                        else if (opsByDoc.TryGetValue((targetDoc.DocumentId, string.Empty), out docOps))
                        {
                            sentBucketKeys.Add((targetDoc.DocumentId, string.Empty));
                        }

                        if (docOps != null && docOps.Length > 0)
                        {
                            var patchMsg = new CrdtPatchMessage(replicaContext.ReplicaId, new CrdtPatch(docOps));
                            var patchPayload = serializer.SerializeToBytes(patchMsg);
                            var docTypeAlias = string.Equals(targetDoc.DocumentId, scope.Orchestrator.Registry.DocumentId, StringComparison.Ordinal)
                                ? null
                                : targetDoc.TypeAlias;

                            var replyWrapper = new CrdtMessageWrapper(targetDoc.DocumentId, "CrdtPatch", patchPayload, docTypeAlias);
                            var replyBytes = serializer.SerializeToBytes(replyWrapper);

                            await directSender.SendDirectAsync(senderId, replyBytes, cancellationToken).ConfigureAwait(false);
                        }
                    }

                    // Also forward journaled operations for shards that were recently demoted locally
                    // (so the document is no longer in `documents`, but its causal operations are still in our local journal/DVV).
                    foreach (var kvp in opsByDoc)
                    {
                        if (sentBucketKeys.Contains(kvp.Key))
                        {
                            continue;
                        }

                        if (!string.IsNullOrEmpty(kvp.Key.ShardId) && shardsRequiringSnapshot.Contains(kvp.Key.ShardId))
                        {
                            continue;
                        }

                        var docOps = kvp.Value;
                        if (docOps == null || docOps.Length == 0 || string.IsNullOrWhiteSpace(kvp.Key.Key))
                        {
                            continue;
                        }

                        string rawDocId = kvp.Key.Key;
                        string? docTypeAlias = null;
                        var colonIdx = rawDocId.IndexOf(':', StringComparison.Ordinal);
                        if (colonIdx > 0 && colonIdx < rawDocId.Length - 1)
                        {
                            docTypeAlias = rawDocId[..colonIdx];
                            rawDocId = rawDocId[(colonIdx + 1)..];
                        }

                        var patchMsg = new CrdtPatchMessage(replicaContext.ReplicaId, new CrdtPatch(docOps));
                        var patchPayload = serializer.SerializeToBytes(patchMsg);
                        var replyWrapper = new CrdtMessageWrapper(rawDocId, "CrdtPatch", patchPayload, docTypeAlias);
                        var replyBytes = serializer.SerializeToBytes(replyWrapper);

                        await directSender.SendDirectAsync(senderId, replyBytes, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            finally
            {
                for (int i = 0; i < acquiredShardScopes.Count; i++)
                {
                    acquiredShardScopes[i].Dispose();
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{ReplicaId}] Failed to process incoming CrdtSync message.", replicaId);
        }
    }

    private async Task ProcessPatchAsync(IDistributedCrdtScope scope, IDistributedCrdtDocument targetDoc, string meshId, CrdtMessageWrapper wrapper, PeerId senderId, CancellationToken cancellationToken)
    {
        try
        {
            var patchMsg = serializer.DeserializeFromBytes<CrdtPatchMessage>(wrapper.Payload!);
            var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

            if (!string.IsNullOrEmpty(patchMsg.ReplicaId) &&
                string.Equals(patchMsg.ReplicaId, replicaContext.ReplicaId, StringComparison.Ordinal))
            {
                if (IsLocalLoopbackSender(scope, meshId, senderId))
                {
                    return;
                }

                logger.LogCritical(
                    "[{ReplicaId}] Duplicate replica ID collision detected! Received CrdtPatch message from remote peer '{PeerId}' claiming the same ReplicaId '{DuplicateReplicaId}' for document '{DocumentId}'. Dropping patch to prevent DVV corruption.",
                    replicaContext.ReplicaId,
                    senderId.Value,
                    patchMsg.ReplicaId,
                    targetDoc.DocumentId);
                return;
            }
            
            if (patchMsg.ReplicaId != null && scope.ClusterTracker.IsReplicaTombstoned(patchMsg.ReplicaId))
            {
                await RejectEvictedReplicaAsync(scope, meshId, targetDoc.DocumentId, patchMsg.ReplicaId, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (!string.IsNullOrWhiteSpace(patchMsg.ReplicaId) && senderId.Value != Guid.Empty)
            {
                var senderIdStr = senderId.Value.ToString();
                var knownReplicaForSender = scope.ClusterTracker.GetReplicaIdByPeerId(senderIdStr);
                var knownPeerForReplica = scope.ClusterTracker.GetPeerIdByReplicaId(patchMsg.ReplicaId);

                if (string.IsNullOrWhiteSpace(knownReplicaForSender) && string.IsNullOrWhiteSpace(knownPeerForReplica))
                {
                    scope.ClusterTracker.UpdatePeerState(patchMsg.ReplicaId, senderIdStr, new ShardVersionVectorMap());
                }
            }

            if (patchMsg.Patch.Operations == null || patchMsg.Patch.Operations.Count == 0)
            {
                return;
            }
            
            patchesProcessedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
            await targetDoc.ApplyRemotePatchAsync(patchMsg.Patch, cancellationToken).ConfigureAwait(false);

            if (!string.IsNullOrWhiteSpace(patchMsg.ReplicaId) && senderId.Value != Guid.Empty)
            {
                var updatedShardState = targetDoc.GetLocalState();
                if (updatedShardState.TryGetValue(targetDoc.ShardId, out var localShardDvv))
                {
                    var peerKnownDvv = new DottedVersionVector();
                    if (localShardDvv.Versions != null &&
                        localShardDvv.Versions.TryGetValue(patchMsg.ReplicaId, out var contiguousVer) &&
                        contiguousVer > 0)
                    {
                        peerKnownDvv.Versions[patchMsg.ReplicaId] = contiguousVer;
                    }

                    if (localShardDvv.Dots != null &&
                        localShardDvv.Dots.TryGetValue(patchMsg.ReplicaId, out var dots) &&
                        dots.Count > 0)
                    {
                        peerKnownDvv.Dots[patchMsg.ReplicaId] = new HashSet<long>(dots);
                    }

                    if (peerKnownDvv.Versions.Count > 0 || peerKnownDvv.Dots.Count > 0)
                    {
                        scope.ClusterTracker.UpdatePeerState(
                            patchMsg.ReplicaId,
                            senderId.Value.ToString(),
                            new ShardVersionVectorMap { [targetDoc.ShardId] = peerKnownDvv });
                    }
                }
            }

            operationsReceivedCounter.Add(patchMsg.Patch.Operations.Count, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{ReplicaId}] Failed to process incoming CrdtPatch message for document {DocumentId}.", scope.ReplicaId, targetDoc.DocumentId);
        }
    }

    private async Task ProcessSnapshotAsync(IDistributedCrdtScope scope, IDistributedCrdtDocument targetDoc, string meshId, CrdtMessageWrapper wrapper, PeerId senderId, CancellationToken cancellationToken)
    {
        try
        {
            var resMsg = serializer.DeserializeFromBytes<CrdtSnapshotMessage>(wrapper.Payload!);
            
            if (resMsg.ReplicaId != null && scope.ClusterTracker.IsReplicaTombstoned(resMsg.ReplicaId))
            {
                await RejectEvictedReplicaAsync(scope, meshId, targetDoc.DocumentId, resMsg.ReplicaId, cancellationToken).ConfigureAwait(false);
                return;
            }

            var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

            if (string.Equals(resMsg.ReplicaId, replicaContext.ReplicaId, StringComparison.Ordinal))
            {
                if (IsLocalLoopbackSender(scope, meshId, senderId))
                {
                    return;
                }

                logger.LogCritical(
                    "[{ReplicaId}] Duplicate replica ID collision detected! Received CrdtSnapshot message from remote peer '{PeerId}' claiming the same ReplicaId '{DuplicateReplicaId}' for document '{DocumentId}'. Dropping snapshot to prevent DVV corruption.",
                    replicaContext.ReplicaId,
                    senderId.Value,
                    resMsg.ReplicaId,
                    targetDoc.DocumentId);
                return;
            }
            
            scope.ClusterTracker.UpdatePeerState(resMsg.ReplicaId, senderId.Value.ToString(), resMsg.GlobalState);
            
            var expectedShardDocIds = resMsg.ShardDocumentIds as IReadOnlyList<string>
                ?? (resMsg.ShardDocumentIds is { Count: > 0 } ? new List<string>(resMsg.ShardDocumentIds) : null);

            await targetDoc.MergeSnapshotAsync(resMsg.SnapshotData, resMsg.GlobalState, expectedShardDocIds, IntentDeliveryMode.Acknowledged, cancellationToken).ConfigureAwait(false);
            snapshotsMergedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{ReplicaId}] Failed to process full state snapshot payload for document {DocumentId}.", scope.ReplicaId, targetDoc.DocumentId);
        }
    }

    private async Task ProcessEvictionRejectionAsync(CrdtMessageWrapper wrapper, CancellationToken cancellationToken)
    {
        try
        {
            var scope = scopeManager.GetOrCreateScope(replicaId);
            var rejectionMsg = serializer.DeserializeFromBytes<CrdtEvictionRejectionMessage>(wrapper.Payload!);
            var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

            if (rejectionMsg.EvictedReplicaId == replicaContext.ReplicaId)
            {
                await scope.EvictionService.RebootLocalIdentityAsync(cancellationToken).ConfigureAwait(false);
                rebootsHandledCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaContext.ReplicaId));
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{ReplicaId}] Failed to execute identity reboot from an eviction rejection message.", replicaId);
        }
    }

    private async Task RejectEvictedReplicaAsync(IDistributedCrdtScope scope, string meshId, string documentId, string evictedReplicaId, CancellationToken cancellationToken)
    {
        logger.LogWarning("[{ReplicaId}] Rejecting payload from deleted replica {EvictedReplicaId} for document {DocumentId}. Forcing reboot.", scope.ReplicaId, evictedReplicaId, documentId);
        
        var rejectionMsg = new CrdtEvictionRejectionMessage(evictedReplicaId);
        var payload = serializer.SerializeToBytes(rejectionMsg);
        
        var wrapper = new CrdtMessageWrapper(documentId, "CrdtEviction", payload);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        var p2pProtocol = scope.ServiceProvider.GetService<IP2pAlgorithm>();
        if (p2pProtocol != null)
        {
            await p2pProtocol.BroadcastAsync(wrapperBytes, cancellationToken).ConfigureAwait(false);
        }
    }

    private static bool IsLocalLoopbackSender(IDistributedCrdtScope scope, string meshId, PeerId senderId)
    {
        if (senderId.Value == Guid.Empty)
        {
            return false;
        }

        var nodeOptionsMonitor = scope.ServiceProvider.GetService<Microsoft.Extensions.Options.IOptionsMonitor<P2pNodeOptions>>();
        if (nodeOptionsMonitor != null)
        {
            try
            {
                var localPeerId = nodeOptionsMonitor.Get(meshId).LocalPeerId;
                if (localPeerId != Guid.Empty && localPeerId == senderId.Value)
                {
                    return true;
                }
            }
            catch
            {
                // Ignore if mesh options are not configured
            }
        }

        var nodeOptions = scope.ServiceProvider.GetService<Microsoft.Extensions.Options.IOptions<P2pNodeOptions>>()?.Value;
        return nodeOptions != null && nodeOptions.LocalPeerId != Guid.Empty && nodeOptions.LocalPeerId == senderId.Value;
    }

    private static bool IsVersionVectorEmpty(DottedVersionVector dvv)
    {
        var hasVersions = false;
        if (dvv.Versions != null)
        {
            foreach (var v in dvv.Versions.Values)
            {
                if (v > 0)
                {
                    hasVersions = true;
                    break;
                }
            }
        }

        if (hasVersions)
        {
            return false;
        }

        if (dvv.Dots != null)
        {
            foreach (var dots in dvv.Dots.Values)
            {
                if (dots != null && dots.Count > 0)
                {
                    return false;
                }
            }
        }

        return true;
    }

    private void RecordSkippedSnapshotProgress(IDistributedCrdtScope scope, CrdtMessageWrapper wrapper, PeerId senderId)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(wrapper.DocumentId) || wrapper.Payload == null)
            {
                return;
            }

            var resMsg = serializer.DeserializeFromBytes<CrdtSnapshotMessage>(wrapper.Payload);
            if (resMsg.GlobalState == null || resMsg.GlobalState.Count == 0)
            {
                return;
            }

            var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
            if (string.Equals(resMsg.ReplicaId, replicaContext.ReplicaId, StringComparison.Ordinal))
            {
                return;
            }

            if (!string.IsNullOrWhiteSpace(resMsg.ReplicaId) && !scope.ClusterTracker.IsReplicaTombstoned(resMsg.ReplicaId))
            {
                scope.ClusterTracker.UpdatePeerState(resMsg.ReplicaId, senderId.Value.ToString(), resMsg.GlobalState);
            }

            var compositeDocId = string.IsNullOrWhiteSpace(wrapper.DocumentTypeAlias) ||
                                 string.Equals(wrapper.DocumentId, Constants.SystemRegistryDocumentId, StringComparison.Ordinal)
                ? wrapper.DocumentId
                : CrdtRegistryEntry.GetRegistryKey(wrapper.DocumentTypeAlias, wrapper.DocumentId);

            var expectedShardDocIds = resMsg.ShardDocumentIds as IReadOnlyList<string>
                ?? (resMsg.ShardDocumentIds is { Count: > 0 } ? new List<string>(resMsg.ShardDocumentIds) : [compositeDocId]);

            var versionVectorManager = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
            foreach (var shardId in resMsg.GlobalState.Keys)
            {
                versionVectorManager.RecordDocumentSnapshotMerge(shardId, compositeDocId, resMsg.GlobalState, expectedShardDocIds);
            }
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "[{ReplicaId}] Failed to record barrier progress for skipped snapshot {DocumentId}.", scope.ReplicaId, wrapper.DocumentId);
        }
    }

    private readonly record struct SnapshotDispatchState(long DispatchedAtTicks, long CooldownMs, DottedVersionVector DispatchedDvv);
}