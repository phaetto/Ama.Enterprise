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
/// Deserializes incoming network application payloads and routes the parsed CRDT intents to the distributed document manager.
/// Agnostic to the underlying distribution algorithm.
/// </summary>
public sealed class CrdtP2pPayloadHandler(
    DistributedCrdtScopeProvider scopeProvider,
    IClusterStateTracker clusterTracker,
    ICrdtSerializer serializer,
    ICrdtEvictionService evictionService,
    ILogger<CrdtP2pPayloadHandler> logger) : IApplicationPayloadHandler
{
    private readonly DistributedCrdtScopeProvider scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
    private readonly IClusterStateTracker clusterTracker = clusterTracker ?? throw new ArgumentNullException(nameof(clusterTracker));
    private readonly ICrdtSerializer serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly ICrdtEvictionService evictionService = evictionService ?? throw new ArgumentNullException(nameof(evictionService));
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
            // The payload belongs to another generic handler mapping within the same P2P pipeline.
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
            await ProcessStateSyncAsync(wrapper, senderId, cancellationToken).ConfigureAwait(false);
            return;
        }

        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var targetDoc = orchestrator.GetActiveDocuments().FirstOrDefault(d => d.DocumentId == wrapper.DocumentId);

        if (targetDoc == null)
        {
            return;
        }

        if (wrapper.MessageType == "CrdtOps")
        {
            await ProcessOperationsAsync(targetDoc, wrapper, cancellationToken).ConfigureAwait(false);
        }
        else if (wrapper.MessageType == "CrdtSnapshot")
        {
            await ProcessSnapshotAsync(targetDoc, wrapper, senderId, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ProcessStateSyncAsync(CrdtMessageWrapper wrapper, PeerId senderId, CancellationToken cancellationToken)
    {
        try
        {
            var syncMsg = serializer.DeserializeFromBytes<CrdtStateSyncMessage>(wrapper.Payload!);
            
            var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

            if (syncMsg.ReplicaId != null && clusterTracker.IsReplicaTombstoned(syncMsg.ReplicaId))
            {
                await RejectEvictedReplicaAsync(orchestrator.Registry, syncMsg.ReplicaId, cancellationToken).ConfigureAwait(false);
                return;
            }

            var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();
            var directSender = scopeProvider.Scope.ServiceProvider.GetRequiredService<IDirectMessageSender>();
            var syncService = scopeProvider.Scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
            var journalManager = scopeProvider.Scope.ServiceProvider.GetRequiredService<IJournalManager>();

            if (string.IsNullOrEmpty(syncMsg.ReplicaId) || syncMsg.ReplicaId == replicaContext.ReplicaId || syncMsg.State == null)
            {
                return;
            }

            clusterTracker.UpdatePeerState(syncMsg.ReplicaId, senderId.Value.ToString(), syncMsg.State);

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

            // Centralized transaction: We retrieve missing operations across all documents in one unified query
            var missingOpsStream = journalManager.GetMissingOperationsAsync(requirement, cancellationToken);
            var syncResult = await syncService.EvaluateJournalCompletionAsync(missingOpsStream, requirement, cancellationToken).ConfigureAwait(false);

            var documents = orchestrator.GetActiveDocuments();

            if (syncResult.SnapshotRequired)
            {
                logger.LogWarning("Journal bounds trimmed or not available in this node. Cannot map operations for replica {ReplicaId}. Dispatching full snapshots directly.", syncMsg.ReplicaId);
                foreach (var targetDoc in documents)
                {
                    await targetDoc.ProvideSnapshotAsync(syncMsg.ReplicaId, senderId, cancellationToken).ConfigureAwait(false);
                }
            }
            else if (syncResult.Operations.Count > 0)
            {
                var opsByDoc = syncResult.Operations
                    .GroupBy(x => x.DocumentId)
                    .ToDictionary(g => g.Key, g => g.Select(x => x.Operation).ToArray());

                foreach (var targetDoc in documents)
                {
                    if (opsByDoc.TryGetValue(targetDoc.DocumentId, out var docOps) && docOps.Length > 0)
                    {
                        logger.LogWarning("Targeting direct delivery of {Count} missing operations for document {DocumentId} to peer {PeerId}", docOps.Length, targetDoc.DocumentId, senderId.Value);
                        
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
            logger.LogError(ex, "Failed to process incoming CrdtSync message.");
        }
    }

    private async Task ProcessOperationsAsync(IDistributedCrdtDocument targetDoc, CrdtMessageWrapper wrapper, CancellationToken cancellationToken)
    {
        try
        {
            var opsMsg = serializer.DeserializeFromBytes<CrdtOperationsMessage>(wrapper.Payload!);
            
            if (opsMsg.ReplicaId != null && clusterTracker.IsReplicaTombstoned(opsMsg.ReplicaId))
            {
                await RejectEvictedReplicaAsync(targetDoc, opsMsg.ReplicaId, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (opsMsg.Operations == null || opsMsg.Operations.Length == 0)
            {
                return;
            }

            logger.LogDebug("Applying {Count} incoming operations for document {DocumentId} from replica {ReplicaId}", opsMsg.Operations.Length, targetDoc.DocumentId, opsMsg.ReplicaId);
            
            await targetDoc.ApplyOperationsAsync(opsMsg.Operations, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process incoming CrdtOps message for document {DocumentId}.", targetDoc.DocumentId);
        }
    }

    private async Task ProcessSnapshotAsync(IDistributedCrdtDocument targetDoc, CrdtMessageWrapper wrapper, PeerId senderId, CancellationToken cancellationToken)
    {
        try
        {
            var resMsg = serializer.DeserializeFromBytes<CrdtSnapshotMessage>(wrapper.Payload!);
            
            if (resMsg.ReplicaId != null && clusterTracker.IsReplicaTombstoned(resMsg.ReplicaId))
            {
                await RejectEvictedReplicaAsync(targetDoc, resMsg.ReplicaId, cancellationToken).ConfigureAwait(false);
                return;
            }

            var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();

            if (resMsg.ReplicaId == replicaContext.ReplicaId)
            {
                return;
            }

            var syncService = scopeProvider.Scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
            
            DottedVersionVector safeLocalState;
            lock (replicaContext.GlobalVersionVector)
            {
                safeLocalState = replicaContext.GlobalVersionVector.DeepClone();
            }

            // DVV Concurrency Check: Verify if our local state contains offline dimensions completely absent from the incoming snapshot.
            var ourEditsNotIncluded = syncService.CalculateRequirement(resMsg.ReplicaId, resMsg.GlobalState, replicaContext.ReplicaId, safeLocalState);

            if (ourEditsNotIncluded.IsBehind)
            {
                logger.LogWarning("Rejecting incoming snapshot for document {DocumentId}. Local state possesses concurrent offline modifications. Applying the snapshot would cause irreversible data amnesia.", targetDoc.DocumentId);
                return;
            }

            logger.LogInformation("Receiving mathematically safe full state network snapshot for document {DocumentId}.", targetDoc.DocumentId);
            
            clusterTracker.UpdatePeerState(resMsg.ReplicaId, senderId.Value.ToString(), resMsg.GlobalState);
            
            await targetDoc.MergeSnapshotAsync(resMsg.SnapshotData, resMsg.GlobalState, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process full state snapshot payload for document {DocumentId}.", targetDoc.DocumentId);
        }
    }

    private async Task ProcessEvictionRejectionAsync(CrdtMessageWrapper wrapper, CancellationToken cancellationToken)
    {
        try
        {
            var rejectionMsg = serializer.DeserializeFromBytes<CrdtEvictionRejectionMessage>(wrapper.Payload!);
            var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();

            if (rejectionMsg.EvictedReplicaId == replicaContext.ReplicaId)
            {
                await evictionService.RebootLocalIdentityAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to execute identity re-bootstrap from an eviction rejection message.");
        }
    }

    private async Task RejectEvictedReplicaAsync(IDistributedCrdtDocument targetDoc, string evictedReplicaId, CancellationToken cancellationToken)
    {
        logger.LogWarning("Rejecting P2P payload from tombstoned replica {ReplicaId} for document {DocumentId}. Enforcing identity re-bootstrap.", evictedReplicaId, targetDoc.DocumentId);
        
        var rejectionMsg = new CrdtEvictionRejectionMessage(evictedReplicaId);
        var payload = serializer.SerializeToBytes(rejectionMsg);
        
        var wrapper = new CrdtMessageWrapper(targetDoc.DocumentId, "CrdtEviction", payload);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        var p2pProtocol = scopeProvider.Scope.ServiceProvider.GetRequiredService<IP2pProtocol>();
        await p2pProtocol.BroadcastAsync(wrapperBytes, cancellationToken).ConfigureAwait(false);
    }
}