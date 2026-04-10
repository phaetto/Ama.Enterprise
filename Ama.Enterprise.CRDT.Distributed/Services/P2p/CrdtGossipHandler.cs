namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
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
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<CrdtGossipHandler> logger;

    public CrdtGossipHandler(
        DistributedCrdtScopeProvider scopeProvider,
        ICrdtSerializer serializer,
        ILogger<CrdtGossipHandler> logger)
    {
        this.scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
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

        var documents = scopeProvider.Scope.ServiceProvider.GetRequiredService<IEnumerable<IDistributedCrdtDocument>>();
        var targetDoc = documents.FirstOrDefault(d => d.DocumentId == wrapper.DocumentId);

        if (targetDoc == null)
        {
            return;
        }

        if (wrapper.MessageType == "CrdtSync")
        {
            await ProcessStateSyncAsync(targetDoc, wrapper, cancellationToken).ConfigureAwait(false);
        }
        else if (wrapper.MessageType == "CrdtOps")
        {
            await ProcessOperationsAsync(targetDoc, wrapper, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ProcessStateSyncAsync(IDistributedCrdtDocument targetDoc, CrdtMessageWrapper wrapper, CancellationToken cancellationToken)
    {
        try
        {
            var syncMsg = serializer.DeserializeFromBytes<CrdtStateSyncMessage>(wrapper.Payload!);
            var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();
            var p2pProtocol = scopeProvider.Scope.ServiceProvider.GetRequiredService<IP2pProtocol>();

            if (string.IsNullOrEmpty(syncMsg.ReplicaId) || syncMsg.ReplicaId == replicaContext.ReplicaId || syncMsg.State == null)
            {
                return;
            }

            var missingOps = await targetDoc.GetMissingOperationsAsync(syncMsg.ReplicaId, syncMsg.State, cancellationToken).ConfigureAwait(false);
            
            if (missingOps.Count > 0)
            {
                logger.LogDebug("Sending {Count} missing operations for document {DocumentId} to replica {ReplicaId}", missingOps.Count, targetDoc.DocumentId, syncMsg.ReplicaId);
                
                var opsMsg = new CrdtOperationsMessage(replicaContext.ReplicaId, missingOps.ToArray());
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
}