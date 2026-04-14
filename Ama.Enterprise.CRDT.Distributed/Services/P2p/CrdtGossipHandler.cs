namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

/// <summary>
/// Deserializes incoming network Gossip bytes and routes the parsed CRDT intents to the correct distributed document manager.
/// </summary>
public sealed class CrdtGossipHandler : IMessageHandler<GossipMessage>
{
    private readonly DistributedCrdtScopeProvider scopeProvider;
    private readonly IClusterStateTracker clusterTracker;
    private readonly ICrdtSerializer serializer;
    private readonly ICrdtEvictionService evictionService;
    private readonly ILogger<CrdtGossipHandler> logger;

    public CrdtGossipHandler(
        DistributedCrdtScopeProvider scopeProvider,
        IClusterStateTracker clusterTracker,
        ICrdtSerializer serializer,
        ICrdtEvictionService evictionService,
        ILogger<CrdtGossipHandler> logger)
    {
        this.scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
        this.clusterTracker = clusterTracker ?? throw new ArgumentNullException(nameof(clusterTracker));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.evictionService = evictionService ?? throw new ArgumentNullException(nameof(evictionService));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task HandleAsync(GossipMessage message, CancellationToken cancellationToken)
    {
        if (message.Payload.IsEmpty)
        {
            return;
        }

        CrdtMessageWrapper wrapper;
        try
        {
            wrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(message.Payload.ToArray());
        }
        catch (Exception)
        {
            // The payload belongs to another generic handler mapping within the same P2P pipeline.
            return;
        }

        if (string.IsNullOrEmpty(wrapper.DocumentId) || string.IsNullOrEmpty(wrapper.MessageType) || wrapper.Payload == null)
        {
            return;
        }

        if (wrapper.MessageType == "CrdtEviction")
        {
            await ProcessEvictionRejectionAsync(wrapper, cancellationToken).ConfigureAwait(false);
            return;
        }

        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var targetDoc = orchestrator.GetActiveDocuments().FirstOrDefault(d => d.DocumentId == wrapper.DocumentId);

        if (targetDoc == null)
        {
            return;
        }

        if (wrapper.MessageType == "CrdtSync")
        {
            await ProcessStateSyncAsync(targetDoc, wrapper, message.SenderId, cancellationToken).ConfigureAwait(false);
        }
        else if (wrapper.MessageType == "CrdtOps")
        {
            await ProcessOperationsAsync(targetDoc, wrapper, cancellationToken).ConfigureAwait(false);
        }
        else if (wrapper.MessageType == "CrdtSnapshot")
        {
            await ProcessSnapshotAsync(targetDoc, wrapper, message.SenderId, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ProcessStateSyncAsync(IDistributedCrdtDocument targetDoc, CrdtMessageWrapper wrapper, PeerId senderId, CancellationToken cancellationToken)
    {
        try
        {
            var syncMsg = serializer.DeserializeFromBytes<CrdtStateSyncMessage>(wrapper.Payload!);
            
            if (syncMsg.ReplicaId != null && clusterTracker.IsReplicaTombstoned(syncMsg.ReplicaId))
            {
                await RejectEvictedReplicaAsync(targetDoc, syncMsg.ReplicaId, cancellationToken).ConfigureAwait(false);
                return;
            }

            var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();
            var p2pProtocol = scopeProvider.Scope.ServiceProvider.GetRequiredService<IP2pProtocol>();

            if (string.IsNullOrEmpty(syncMsg.ReplicaId) || syncMsg.ReplicaId == replicaContext.ReplicaId || syncMsg.State == null)
            {
                return;
            }

            // Immediately track the globally reported DVV for background mathematically secure truncation trimming maps natively
            clusterTracker.UpdatePeerState(syncMsg.ReplicaId, senderId.Value.ToString(), syncMsg.State);

            var missingOpsResult = await targetDoc.GetMissingOperationsAsync(syncMsg.ReplicaId, syncMsg.State, cancellationToken).ConfigureAwait(false);
            
            if (missingOpsResult.SnapshotRequired)
            {
                logger.LogWarning("Journal bounds trimmed. Cannot map operations for replica {ReplicaId} in document {DocumentId}. Triggering full snapshot.", syncMsg.ReplicaId, targetDoc.DocumentId);
                await targetDoc.ProvideSnapshotAsync(syncMsg.ReplicaId, cancellationToken).ConfigureAwait(false);
            }
            else if (missingOpsResult.Operations.Count > 0)
            {
                logger.LogDebug("Sending {Count} missing operations for document {DocumentId} to replica {ReplicaId}", missingOpsResult.Operations.Count, targetDoc.DocumentId, syncMsg.ReplicaId);
                
                var opsMsg = new CrdtOperationsMessage(replicaContext.ReplicaId, missingOpsResult.Operations.ToArray());
                var opsPayload = serializer.SerializeToBytes(opsMsg);
                
                var replyWrapper = new CrdtMessageWrapper(targetDoc.DocumentId, "CrdtOps", opsPayload);
                var replyBytes = serializer.SerializeToBytes(replyWrapper);

                await p2pProtocol.BroadcastAsync(replyBytes, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process incoming CrdtSync message for document {DocumentId}.", targetDoc.DocumentId);
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

            logger.LogInformation("Receiving full state network snapshot for document {DocumentId}.", targetDoc.DocumentId);
            
            // Map explicitly overarching tracking vectors effectively natively bridging states cleanly
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