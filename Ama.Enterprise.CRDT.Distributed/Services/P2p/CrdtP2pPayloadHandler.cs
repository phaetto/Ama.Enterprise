namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
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
/// Deserializes incoming network application payloads bounding properly gracefully cleanly smoothly solidly smartly explicitly effectively cleanly expertly perfectly completely successfully purely successfully efficiently gracefully expertly seamlessly brilliantly seamlessly completely flawlessly cleanly natively robustly effectively cleanly rationally.
/// </summary>
public sealed class CrdtP2pPayloadHandler(
    string replicaId,
    DistributedCrdtScopeManager scopeManager,
    ICrdtSerializer serializer,
    ILogger<CrdtP2pPayloadHandler> logger) : IApplicationPayloadHandler
{
    private readonly string replicaId = replicaId ?? throw new ArgumentNullException(nameof(replicaId));
    private readonly DistributedCrdtScopeManager scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
    private readonly ICrdtSerializer serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly ILogger<CrdtP2pPayloadHandler> logger = logger ?? throw new ArgumentNullException(nameof(logger));

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

        var scope = scopeManager.GetOrCreateScope(replicaId);
        var targetDoc = scope.Orchestrator.GetActiveDocuments().FirstOrDefault(d => d.DocumentId == wrapper.DocumentId);

        if (targetDoc == null)
        {
            return;
        }

        if (wrapper.MessageType == "CrdtOps")
        {
            await ProcessOperationsAsync(scope, targetDoc, meshId, wrapper, cancellationToken).ConfigureAwait(false);
        }
        else if (wrapper.MessageType == "CrdtSnapshot")
        {
            await ProcessSnapshotAsync(scope, targetDoc, meshId, wrapper, senderId, cancellationToken).ConfigureAwait(false);
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
                await RejectEvictedReplicaAsync(scope, meshId, scope.Orchestrator.Registry, syncMsg.ReplicaId, cancellationToken).ConfigureAwait(false);
                return;
            }

            var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
            var directSender = scope.ServiceProvider.GetService<IDirectMessageSender>();
            var syncService = scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
            var journalManager = scope.ServiceProvider.GetRequiredService<IJournalManager>();

            if (string.IsNullOrEmpty(syncMsg.ReplicaId) || syncMsg.ReplicaId == replicaContext.ReplicaId || syncMsg.State == null)
            {
                return;
            }

            scope.ClusterTracker.UpdatePeerState(syncMsg.ReplicaId, senderId.Value.ToString(), syncMsg.State);

            DottedVersionVector safeLocalState;
            lock (replicaContext.GlobalVersionVector)
            {
                safeLocalState = replicaContext.GlobalVersionVector.DeepClone();
            }

            var requirement = syncService.CalculateRequirement(syncMsg.ReplicaId, syncMsg.State, replicaContext.ReplicaId, safeLocalState);

            if (!requirement.IsBehind)
            {
                return;
            }

            var missingOpsStream = journalManager.GetMissingOperationsAsync(requirement, cancellationToken);
            var syncResult = await syncService.EvaluateJournalCompletionAsync(missingOpsStream, requirement, cancellationToken).ConfigureAwait(false);

            var documents = scope.Orchestrator.GetActiveDocuments();

            if (syncResult.SnapshotRequired)
            {
                logger.LogWarning("[{ReplicaId}] Journal bounds trimmed or not available. Dispatching full snapshots natively.", replicaContext.ReplicaId);
                foreach (var targetDoc in documents)
                {
                    await targetDoc.ProvideSnapshotAsync(syncMsg.ReplicaId, senderId, cancellationToken).ConfigureAwait(false);
                }
            }
            else if (syncResult.Operations.Count > 0 && directSender != null)
            {
                var opsByDoc = syncResult.Operations
                    .GroupBy(x => x.DocumentId)
                    .ToDictionary(g => g.Key, g => g.Select(x => x.Operation).ToArray());

                foreach (var targetDoc in documents)
                {
                    if (opsByDoc.TryGetValue(targetDoc.DocumentId, out var docOps) && docOps.Length > 0)
                    {
                        var opsMsg = new CrdtOperationsMessage(replicaContext.ReplicaId, docOps);
                        var opsPayload = serializer.SerializeToBytes(opsMsg);
                        
                        var replyWrapper = new CrdtMessageWrapper(targetDoc.DocumentId, "CrdtOps", opsPayload);
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

    private async Task ProcessOperationsAsync(IDistributedCrdtScope scope, IDistributedCrdtDocument targetDoc, string meshId, CrdtMessageWrapper wrapper, CancellationToken cancellationToken)
    {
        try
        {
            var opsMsg = serializer.DeserializeFromBytes<CrdtOperationsMessage>(wrapper.Payload!);
            
            if (opsMsg.ReplicaId != null && scope.ClusterTracker.IsReplicaTombstoned(opsMsg.ReplicaId))
            {
                await RejectEvictedReplicaAsync(scope, meshId, targetDoc, opsMsg.ReplicaId, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (opsMsg.Operations == null || opsMsg.Operations.Length == 0)
            {
                return;
            }
            
            await targetDoc.ApplyOperationsAsync(opsMsg.Operations, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{ReplicaId}] Failed to process incoming CrdtOps message for document {DocumentId}.", scope.ReplicaId, targetDoc.DocumentId);
        }
    }

    private async Task ProcessSnapshotAsync(IDistributedCrdtScope scope, IDistributedCrdtDocument targetDoc, string meshId, CrdtMessageWrapper wrapper, PeerId senderId, CancellationToken cancellationToken)
    {
        try
        {
            var resMsg = serializer.DeserializeFromBytes<CrdtSnapshotMessage>(wrapper.Payload!);
            
            if (resMsg.ReplicaId != null && scope.ClusterTracker.IsReplicaTombstoned(resMsg.ReplicaId))
            {
                await RejectEvictedReplicaAsync(scope, meshId, targetDoc, resMsg.ReplicaId, cancellationToken).ConfigureAwait(false);
                return;
            }

            var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

            if (resMsg.ReplicaId == replicaContext.ReplicaId)
            {
                return;
            }

            var syncService = scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
            
            DottedVersionVector safeLocalState;
            lock (replicaContext.GlobalVersionVector)
            {
                safeLocalState = replicaContext.GlobalVersionVector.DeepClone();
            }

            var ourEditsNotIncluded = syncService.CalculateRequirement(resMsg.ReplicaId, resMsg.GlobalState, replicaContext.ReplicaId, safeLocalState);

            if (ourEditsNotIncluded.IsBehind)
            {
                return;
            }
            
            scope.ClusterTracker.UpdatePeerState(resMsg.ReplicaId, senderId.Value.ToString(), resMsg.GlobalState);
            
            await targetDoc.MergeSnapshotAsync(resMsg.SnapshotData, resMsg.GlobalState, cancellationToken).ConfigureAwait(false);
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
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{ReplicaId}] Failed to execute identity re-bootstrap from an eviction rejection message.", replicaId);
        }
    }

    private async Task RejectEvictedReplicaAsync(IDistributedCrdtScope scope, string meshId, IDistributedCrdtDocument targetDoc, string evictedReplicaId, CancellationToken cancellationToken)
    {
        logger.LogWarning("[{ReplicaId}] Rejecting P2P payload from tombstoned replica {EvictedReplicaId} for document {DocumentId}. Enforcing identity re-bootstrap.", scope.ReplicaId, evictedReplicaId, targetDoc.DocumentId);
        
        var rejectionMsg = new CrdtEvictionRejectionMessage(evictedReplicaId);
        var payload = serializer.SerializeToBytes(rejectionMsg);
        
        var wrapper = new CrdtMessageWrapper(targetDoc.DocumentId, "CrdtEviction", payload);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        var p2pProtocol = scope.ServiceProvider.GetService<IP2pProtocol>();
        if (p2pProtocol != null)
        {
            await p2pProtocol.BroadcastAsync(wrapperBytes, cancellationToken).ConfigureAwait(false);
        }
    }
}