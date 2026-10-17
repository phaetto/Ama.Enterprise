namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
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

        messagesProcessedCounter.Add(1, new KeyValuePair<string, object?>("message_type", wrapper.MessageType));

        var scope = scopeManager.GetOrCreateScope(replicaId);

        if (wrapper.MessageType == "CrdtReadReq" || wrapper.MessageType == "CrdtReadRes" || 
            wrapper.MessageType == "CrdtIntentReq" || wrapper.MessageType == "CrdtIntentRes")
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
                var targetDoc = await ResolveLocalDocumentAsync(scope, req.DocumentId, typeAlias, cancellationToken).ConfigureAwait(false);

                byte[] statePayload = [];
                if (targetDoc != null)
                {
                    var snapshot = await targetDoc.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
                    statePayload = snapshot.SnapshotData;
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

            if (string.IsNullOrEmpty(syncMsg.ReplicaId) || syncMsg.ReplicaId == replicaContext.ReplicaId || syncMsg.State == null)
            {
                return;
            }

            scope.ClusterTracker.UpdatePeerState(syncMsg.ReplicaId, senderId.Value.ToString(), syncMsg.State);

            var safeLocalState = await versionVectorManager.GetFullSnapshotAsync(cancellationToken).ConfigureAwait(false);

            var allShardsToEvaluate = new HashSet<string>(syncMsg.State.Keys, StringComparer.Ordinal);
            foreach (var localShardId in safeLocalState.Keys)
            {
                var targetNodes = await distributionManager.GetTargetNodesForShardAsync(localShardId, cancellationToken).ConfigureAwait(false);
                if (targetNodes.Contains(syncMsg.ReplicaId, StringComparer.Ordinal))
                {
                    allShardsToEvaluate.Add(localShardId);
                }
            }

            var documents = scope.Orchestrator.GetActiveDocuments();
            var localActiveShards = new HashSet<string>(StringComparer.Ordinal);
            foreach (var doc in documents)
            {
                localActiveShards.Add(doc.ShardId);
            }

            allShardsToEvaluate.IntersectWith(localActiveShards);

            var requirements = new List<ReplicaSyncRequirement>();
            var missingOpsStreams = new List<IAsyncEnumerable<JournaledOperation>>();

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
            }

            if (requirements.Count == 0)
            {
                return;
            }

            var shardsRequiringSnapshot = new HashSet<string>(StringComparer.Ordinal);
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
                logger.LogWarning("[{ReplicaId}] Journal missing or trimmed for {ShardCount} shard(s). Sending full snapshots for affected shards.", replicaContext.ReplicaId, shardsRequiringSnapshot.Count);
                foreach (var targetDoc in documents)
                {
                    if (shardsRequiringSnapshot.Contains(targetDoc.ShardId))
                    {
                        await scope.Orchestrator.ProvideSnapshotAsync(targetDoc.DocumentId, syncMsg.ReplicaId, senderId, cancellationToken).ConfigureAwait(false);
                    }
                }
            }

            if (allOps.Count > 0 && directSender != null)
            {
                var registryDoc = await scope.Orchestrator.Registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
                var opsByDoc = allOps
                    .GroupBy(x => x.DocumentId)
                    .ToDictionary(g => g.Key, g => g.Select(x => x.Operation).ToArray());

                foreach (var targetDoc in documents)
                {
                    if (shardsRequiringSnapshot.Contains(targetDoc.ShardId))
                    {
                        continue;
                    }

                    if (opsByDoc.TryGetValue(targetDoc.DocumentId, out var docOps) && docOps.Length > 0)
                    {
                        var patchMsg = new CrdtPatchMessage(replicaContext.ReplicaId, new CrdtPatch(docOps));
                        var patchPayload = serializer.SerializeToBytes(patchMsg);
                        
                        string? docTypeAlias = null;
                        if (registryDoc.Data.Documents.TryGetValue(targetDoc.DocumentId, out var entry))
                        {
                            docTypeAlias = entry.TypeAlias;
                        }

                        var replyWrapper = new CrdtMessageWrapper(targetDoc.DocumentId, "CrdtPatch", patchPayload, docTypeAlias);
                        var replyBytes = serializer.SerializeToBytes(replyWrapper);

                        await directSender.SendDirectAsync(senderId, replyBytes, cancellationToken).ConfigureAwait(false);
                    }
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
            operationsReceivedCounter.Add(patchMsg.Patch.Operations.Count, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
            
            if (targetDoc.DocumentId == scope.Orchestrator.Registry.DocumentId)
            {
                await scope.Orchestrator.SyncDocumentsAsync(cancellationToken).ConfigureAwait(false);
            }
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

            if (resMsg.ReplicaId == replicaContext.ReplicaId)
            {
                return;
            }
            
            scope.ClusterTracker.UpdatePeerState(resMsg.ReplicaId, senderId.Value.ToString(), resMsg.GlobalState);
            
            await targetDoc.MergeSnapshotAsync(resMsg.SnapshotData, resMsg.GlobalState, cancellationToken).ConfigureAwait(false);
            snapshotsMergedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
            
            if (targetDoc.DocumentId == scope.Orchestrator.Registry.DocumentId)
            {
                await scope.Orchestrator.SyncDocumentsAsync(cancellationToken).ConfigureAwait(false);
            }
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
}