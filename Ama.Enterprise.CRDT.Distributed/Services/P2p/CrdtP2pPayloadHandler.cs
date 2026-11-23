namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

/// <summary>
/// Deserializes incoming network application payloads and routes them to specialized RPC, state sync, patch, and snapshot processors.
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

        var targetScopeId = ResolveTargetScopeId(wrapper);
        var hasExistingScope = scopeManager.TryGetScope(targetScopeId, out var scope);
        var authScope = scope ?? scopeManager.GetOrCreateScope(replicaId);

        if (senderId.Value != Guid.Empty)
        {
            var senderIdStr = senderId.Value.ToString();
            var topologyProvider = authScope.ServiceProvider.GetService<IScopeTopologyProvider>();
            if (topologyProvider != null)
            {
                var accessCtx = new ScopeAccessContext(targetScopeId, senderIdStr, meshId, wrapper.MessageType);
                var isAllowed = await topologyProvider.CanAcceptScopeFromPeerAsync(accessCtx, cancellationToken).ConfigureAwait(false);
                if (!isAllowed && string.Equals(wrapper.MessageType, "CrdtSync", StringComparison.Ordinal))
                {
                    // Allow a downstream peer to send a CrdtSync pull request if we are allowed to replicate this scope to that peer
                    isAllowed = await topologyProvider.CanReplicateScopeToPeerAsync(accessCtx, cancellationToken).ConfigureAwait(false);
                }

                if (!isAllowed)
                {
                    logger.LogWarning(
                        "[{ReplicaId}] Dropped inbound {MessageType} payload for scope '{ScopeId}' from unauthorized peer '{PeerId}'.",
                        authScope.ReplicaId,
                        wrapper.MessageType,
                        targetScopeId,
                        senderId.Value);
                    return;
                }
            }
        }

        if (!hasExistingScope || scope == null)
        {
            scope = await scopeManager.GetOrCreateScopeAsync(targetScopeId, replicaId, cancellationToken).ConfigureAwait(false);
        }

        messagesProcessedCounter.Add(1, new KeyValuePair<string, object?>("message_type", wrapper.MessageType));

        if (wrapper.MessageType == "CrdtReadReq" || wrapper.MessageType == "CrdtReadRes" ||
            wrapper.MessageType == "CrdtIntentReq" || wrapper.MessageType == "CrdtIntentRes" ||
            wrapper.MessageType == "CrdtMapReduceReq" || wrapper.MessageType == "CrdtMapReduceRes")
        {
            rpcRequestsProcessedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
            var rpcProcessor = scope.ServiceProvider.GetRequiredService<ICrdtRpcPayloadProcessor>();
            await rpcProcessor.ProcessRpcPayloadAsync(scope, meshId, wrapper, senderId, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (wrapper.MessageType == "CrdtEviction")
        {
            await ProcessEvictionRejectionAsync(scope, wrapper, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (wrapper.MessageType == "CrdtSync")
        {
            stateSyncsProcessedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
            var syncProcessor = scope.ServiceProvider.GetRequiredService<ICrdtStateSyncPayloadProcessor>();
            await syncProcessor.ProcessStateSyncAsync(
                scope,
                meshId,
                wrapper,
                senderId,
                RejectEvictedReplicaAsync,
                IsLocalLoopbackSender,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        var targetDoc = string.IsNullOrWhiteSpace(wrapper.DocumentId)
            ? null
            : await scope.Orchestrator.ResolveOrPromoteLocalDocumentAsync(wrapper.DocumentId, wrapper.DocumentTypeAlias, cancellationToken).ConfigureAwait(false);

        if (targetDoc == null)
        {
            if (wrapper.MessageType == "CrdtSnapshot")
            {
                await RecordSkippedOrBufferedSnapshotAsync(scope, wrapper, senderId, cancellationToken).ConfigureAwait(false);
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

    /// <inheritdoc />
    public void Dispose()
    {
        meter.Dispose();
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

            var preApplyState = targetDoc.GetLocalState();
            preApplyState.TryGetValue(targetDoc.ShardId, out var preShardDvv);
            var preDvvSnapshot = CloneDvv(preShardDvv);

            patchesProcessedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));
            var versionVectorManager = scope.ServiceProvider.GetService<IShardVersionVectorManager>();
            versionVectorManager?.RecordShardPatchActivity(targetDoc.ShardId, patchMsg.Patch.Operations.Count);

            await targetDoc.ApplyRemotePatchAsync(patchMsg.Patch, cancellationToken).ConfigureAwait(false);

            var updatedShardState = targetDoc.GetLocalState();
            updatedShardState.TryGetValue(targetDoc.ShardId, out var localShardDvv);

            if (!string.IsNullOrWhiteSpace(patchMsg.ReplicaId) && senderId.Value != Guid.Empty && localShardDvv != null)
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

            operationsReceivedCounter.Add(patchMsg.Patch.Operations.Count, new KeyValuePair<string, object?>("replica_id", scope.ReplicaId));

            if (localShardDvv != null && DidDvvAdvance(preDvvSnapshot, localShardDvv))
            {
                await RelayPatchAcrossBoundMeshesAsync(scope, wrapper, senderId, cancellationToken).ConfigureAwait(false);
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

            if (string.Equals(targetDoc.DocumentId, Constants.SystemRegistryDocumentId, StringComparison.Ordinal))
            {
                await scope.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{ReplicaId}] Failed to process full state snapshot payload for document {DocumentId}.", scope.ReplicaId, targetDoc.DocumentId);
        }
    }

    private async Task ProcessEvictionRejectionAsync(IDistributedCrdtScope scope, CrdtMessageWrapper wrapper, CancellationToken cancellationToken)
    {
        try
        {
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
        var logicalScopeId = !string.IsNullOrWhiteSpace(scope.ScopeId) ? scope.ScopeId : scope.ReplicaId;
        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

        var wrapper = new CrdtMessageWrapper(
            documentId,
            "CrdtEviction",
            payload,
            replicaContext.ReplicaId,
            ScopeId: logicalScopeId);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        var p2pProtocol = scope.ServiceProvider.GetService<IP2pAlgorithm>();
        if (p2pProtocol != null)
        {
            await p2pProtocol.BroadcastAsync(wrapperBytes, cancellationToken).ConfigureAwait(false);
        }
    }

    private static DottedVersionVector CloneDvv(DottedVersionVector? source)
    {
        if (source == null)
        {
            return new DottedVersionVector();
        }

        var versions = source.Versions != null
            ? new Dictionary<string, long>(source.Versions, StringComparer.Ordinal)
            : new Dictionary<string, long>(StringComparer.Ordinal);

        var dots = new Dictionary<string, ISet<long>>(StringComparer.Ordinal);
        if (source.Dots != null)
        {
            foreach (var kvp in source.Dots)
            {
                if (kvp.Value != null && kvp.Value.Count > 0)
                {
                    dots[kvp.Key] = new HashSet<long>(kvp.Value);
                }
            }
        }

        return new DottedVersionVector(versions, dots);
    }

    private static bool DidDvvAdvance(DottedVersionVector previous, DottedVersionVector current)
    {
        if (current.Versions != null)
        {
            foreach (var kvp in current.Versions)
            {
                long prevVer = 0;
                previous.Versions?.TryGetValue(kvp.Key, out prevVer);
                if (kvp.Value > prevVer)
                {
                    return true;
                }
            }
        }

        if (current.Dots != null)
        {
            foreach (var kvp in current.Dots)
            {
                if (kvp.Value == null || kvp.Value.Count == 0)
                {
                    continue;
                }

                long prevVer = 0;
                previous.Versions?.TryGetValue(kvp.Key, out prevVer);
                ISet<long>? prevDots = null;
                previous.Dots?.TryGetValue(kvp.Key, out prevDots);

                foreach (var dot in kvp.Value)
                {
                    if (dot > prevVer && (prevDots == null || !prevDots.Contains(dot)))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private async Task RelayPatchAcrossBoundMeshesAsync(
        IDistributedCrdtScope scope,
        CrdtMessageWrapper wrapper,
        PeerId senderId,
        CancellationToken cancellationToken)
    {
        var peerRegistry = scope.ServiceProvider.GetService<IPeerRegistry>();
        var directSender = scope.ServiceProvider.GetService<IDirectMessageSender>();
        if (peerRegistry == null || directSender == null)
        {
            return;
        }

        var hostId = !string.IsNullOrWhiteSpace(scope.HostReplicaId) ? scope.HostReplicaId : replicaId;
        var logicalScopeId = !string.IsNullOrWhiteSpace(scope.ScopeId) ? scope.ScopeId : scope.ReplicaId;

        var boundMeshes = new HashSet<string>(StringComparer.Ordinal);
        foreach (var reg in scope.ServiceProvider.GetServices<DistributedCrdtP2pRegistration>())
        {
            if ((string.Equals(reg.ReplicaId, scope.ReplicaId, StringComparison.Ordinal) ||
                 string.Equals(reg.ReplicaId, hostId, StringComparison.Ordinal)) &&
                !string.IsNullOrWhiteSpace(reg.MeshId))
            {
                boundMeshes.Add(reg.MeshId);
            }
        }

        if (boundMeshes.Count == 0)
        {
            return;
        }

        var relayWrapper = string.IsNullOrWhiteSpace(wrapper.ScopeId)
            ? wrapper with { ScopeId = logicalScopeId }
            : wrapper;
        byte[]? serializedRelayBytes = null;

        var topologyProvider = scope.ServiceProvider.GetService<IScopeTopologyProvider>();
        var seenPeers = new HashSet<Guid>();
        if (senderId.Value != Guid.Empty)
        {
            seenPeers.Add(senderId.Value);
        }

        foreach (var boundMeshId in boundMeshes)
        {
            var activePeers = await peerRegistry.GetPeersByStatusAsync(boundMeshId, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
            foreach (var peer in activePeers)
            {
                if (!seenPeers.Add(peer.Id.Value))
                {
                    continue;
                }

                var peerIdStr = peer.Id.Value.ToString();
                var isAllowed = true;
                if (topologyProvider != null)
                {
                    isAllowed = await topologyProvider.CanReplicateScopeToPeerAsync(
                        new ScopeAccessContext(logicalScopeId, peerIdStr, boundMeshId, "CrdtPatch"),
                        cancellationToken).ConfigureAwait(false);
                }

                if (isAllowed)
                {
                    serializedRelayBytes ??= serializer.SerializeToBytes(relayWrapper);
                    await directSender.SendDirectAsync(boundMeshId, peer.Id, serializedRelayBytes, cancellationToken).ConfigureAwait(false);
                }
            }
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
            }
        }

        var nodeOptions = scope.ServiceProvider.GetService<Microsoft.Extensions.Options.IOptions<P2pNodeOptions>>()?.Value;
        return nodeOptions != null && nodeOptions.LocalPeerId != Guid.Empty && nodeOptions.LocalPeerId == senderId.Value;
    }

    private async Task RecordSkippedOrBufferedSnapshotAsync(IDistributedCrdtScope scope, CrdtMessageWrapper wrapper, PeerId senderId, CancellationToken cancellationToken)
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

            if (resMsg.SnapshotData is { Length: > 0 } && !string.IsNullOrWhiteSpace(wrapper.DocumentTypeAlias))
            {
                var factory = scope.ServiceProvider.GetKeyedService<IDocumentFactory>(wrapper.DocumentTypeAlias);
                if (factory != null)
                {
                    var shardId = await scope.Orchestrator.TryGetShardIdAsync(wrapper.DocumentId, wrapper.DocumentTypeAlias, cancellationToken).ConfigureAwait(false)
                                  ?? factory.GetShardId(scope.ServiceProvider, wrapper.DocumentId);
                    if (!string.IsNullOrWhiteSpace(shardId))
                    {
                        await scope.Storage.SaveSerializedDocumentAsync<object>(shardId, compositeDocId, resMsg.SnapshotData, cancellationToken).ConfigureAwait(false);
                    }
                }
            }

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

    private string ResolveTargetScopeId(CrdtMessageWrapper wrapper)
    {
        if (string.IsNullOrWhiteSpace(wrapper.ScopeId))
        {
            return replicaId;
        }

        var incomingScopeId = wrapper.ScopeId!;
        if (scopeManager.TryGetScope(incomingScopeId, out _))
        {
            return incomingScopeId;
        }

        // If the sender stamped its own physical replica ID (e.g. "ReplicaA" or "node-1") as the default ScopeId
        // because no explicit multi-tenant ScopeId was configured, route to this handler's local default replica scope
        // instead of creating a phantom "localReplica@senderReplica" composite scope.
        if (!incomingScopeId.Contains('@', StringComparison.Ordinal) &&
            !string.IsNullOrWhiteSpace(wrapper.SenderReplicaId) &&
            string.Equals(incomingScopeId, wrapper.SenderReplicaId, StringComparison.Ordinal))
        {
            return replicaId;
        }

        return incomingScopeId;
    }
}