namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// A completely stateless pass-through proxy document bypassing local allocations executing seamless remote network queries resolving remote CRDT shards.
/// </summary>
public sealed class ProxyCrdtDocument<TState> : IDistributedCrdtDocument<TState> where TState : class, new()
{
    private readonly ReplicaContext replicaContext;
    private readonly IP2pRpcCorrelator rpcCorrelator;
    private readonly IDirectMessageSender directSender;
    private readonly IShardDistributionManager distributionManager;
    private readonly IClusterStateTracker clusterStateTracker;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<ProxyCrdtDocument<TState>> logger;

    /// <inheritdoc />
    public string DocumentId { get; }

    /// <inheritdoc />
    public string ShardId { get; }

    public ProxyCrdtDocument(
        string documentId,
        string shardId,
        ReplicaContext replicaContext,
        IP2pRpcCorrelator rpcCorrelator,
        IDirectMessageSender directSender,
        IShardDistributionManager distributionManager,
        IClusterStateTracker clusterStateTracker,
        ICrdtSerializer serializer,
        ILogger<ProxyCrdtDocument<TState>> logger)
    {
        DocumentId = documentId ?? throw new ArgumentNullException(nameof(documentId));
        ShardId = shardId ?? throw new ArgumentNullException(nameof(shardId));
        this.replicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));
        this.rpcCorrelator = rpcCorrelator ?? throw new ArgumentNullException(nameof(rpcCorrelator));
        this.directSender = directSender ?? throw new ArgumentNullException(nameof(directSender));
        this.distributionManager = distributionManager ?? throw new ArgumentNullException(nameof(distributionManager));
        this.clusterStateTracker = clusterStateTracker ?? throw new ArgumentNullException(nameof(clusterStateTracker));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async ValueTask<CrdtDocument<TState>> GetDocumentAsync(CancellationToken cancellationToken = default)
    {
        var req = new CrdtReadRequestDto { CorrelationId = Guid.NewGuid(), DocumentId = DocumentId };
        var reqPayload = serializer.SerializeToBytes(req);
        var wrapper = new CrdtMessageWrapper(DocumentId, "CrdtReadReq", reqPayload);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        var activeNodes = distributionManager.GetTargetReplicas(ShardId);
        if (activeNodes == null || activeNodes.Count == 0)
        {
            throw new InvalidOperationException($"Cannot evaluate Proxy CRDT State. No active nodes found mapping strictly for shard {ShardId}.");
        }
        
        var targetReplicaId = activeNodes[0]; 
        var targetPeerId = clusterStateTracker.GetPeerIdByReplicaId(targetReplicaId);

        if (string.IsNullOrWhiteSpace(targetPeerId))
        {
            throw new InvalidOperationException($"Cannot evaluate Proxy CRDT State. Replica {targetReplicaId} has no active network peer mapping.");
        }

        logger.LogTrace("Stateless proxy actively querying explicit network RPC bounds addressing document {DocumentId} against replica {TargetReplica} via peer {TargetPeer}.", DocumentId, targetReplicaId, targetPeerId);

        await directSender.SendDirectAsync(new PeerId(Guid.Parse(targetPeerId)), wrapperBytes, cancellationToken).ConfigureAwait(false);

        return await rpcCorrelator.RegisterRequestAsync<CrdtDocument<TState>>(req.CorrelationId, TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task ApplyPatchAsync(CrdtPatch patch, CancellationToken cancellationToken = default)
    {
        if (patch.Operations == null || patch.Operations.Count == 0)
        {
            return;
        }

        var patchMsg = new CrdtPatchMessage(replicaContext.ReplicaId, patch);
        var payload = serializer.SerializeToBytes(patchMsg);
        var wrapper = new CrdtMessageWrapper(DocumentId, "CrdtPatch", payload);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        var activeNodes = distributionManager.GetTargetReplicas(ShardId);
        if (activeNodes == null || activeNodes.Count == 0)
        {
            logger.LogWarning("Forwarding patch rejected mapping strict zero node topologies bounding proxy for shard {ShardId}.", ShardId);
            return;
        }

        foreach (var targetReplicaId in activeNodes)
        {
            var targetPeerId = clusterStateTracker.GetPeerIdByReplicaId(targetReplicaId);
            if (!string.IsNullOrWhiteSpace(targetPeerId))
            {
                logger.LogTrace("Stateless proxy forwarding explicit patch for document {DocumentId} towards remote shard owner {TargetReplica} via peer {TargetPeer}.", DocumentId, targetReplicaId, targetPeerId);
                await directSender.SendDirectAsync(new PeerId(Guid.Parse(targetPeerId)), wrapperBytes, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc />
    public async Task<bool> ForwardIntentAsync<TIntent>(TIntent intent, string targetProperty, CancellationToken cancellationToken = default) where TIntent : class
    {
        var req = new CrdtIntentForwardRequestDto
        {
            CorrelationId = Guid.NewGuid(),
            DocumentId = DocumentId,
            SerializedIntent = serializer.SerializeToBytes(intent),
            IntentTypeAlias = typeof(TIntent).Name,
            TargetProperty = targetProperty
        };

        var reqPayload = serializer.SerializeToBytes(req);
        var wrapper = new CrdtMessageWrapper(DocumentId, "CrdtIntentReq", reqPayload);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        var activeNodes = distributionManager.GetTargetReplicas(ShardId);
        if (activeNodes == null || activeNodes.Count == 0)
        {
            logger.LogWarning("Forwarding generic intent rejected mapping strict zero node topologies bounding proxy.");
            return false;
        }

        var targetReplicaId = activeNodes[0];
        var targetPeerId = clusterStateTracker.GetPeerIdByReplicaId(targetReplicaId);

        if (string.IsNullOrWhiteSpace(targetPeerId))
        {
            logger.LogWarning("Forwarding generic intent rejected mapping strict disconnected network peer for replica {ReplicaId}.", targetReplicaId);
            return false;
        }
        
        logger.LogTrace("Stateless proxy forwarding execution intent mapping explicit document {DocumentId} towards remote shard owner {TargetReplica} via peer {TargetPeer}.", DocumentId, targetReplicaId, targetPeerId);
        
        await directSender.SendDirectAsync(new PeerId(Guid.Parse(targetPeerId)), wrapperBytes, cancellationToken).ConfigureAwait(false);

        var response = await rpcCorrelator.RegisterRequestAsync<CrdtIntentForwardResponseDto>(req.CorrelationId, TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
        
        if (!response.IsSuccess)
        {
            logger.LogWarning("Proxy execution explicitly faulting natively traversing intent: {ErrorMessage}", response.ErrorMessage);
        }

        return response.IsSuccess;
    }

    public Task InitializeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public ShardVersionVectorMap GetLocalState() => new();

    public Task<CrdtSnapshotDataDto> GetSnapshotDataAsync(CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Stateless proxies do not strictly execute snapshots locally across local channels.");
    }

    public async Task ApplyOperationsAsync(IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default)
    {
        if (operations == null || operations.Count == 0)
        {
            return;
        }

        var opsMsg = new CrdtOperationsMessage(replicaContext.ReplicaId, operations.ToArray());
        var payload = serializer.SerializeToBytes(opsMsg);
        var wrapper = new CrdtMessageWrapper(DocumentId, "CrdtOps", payload);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        var activeNodes = distributionManager.GetTargetReplicas(ShardId);
        if (activeNodes == null || activeNodes.Count == 0)
        {
            logger.LogWarning("Forwarding operations rejected mapping strict zero node topologies bounding proxy for shard {ShardId}.", ShardId);
            return;
        }

        foreach (var targetReplicaId in activeNodes)
        {
            var targetPeerId = clusterStateTracker.GetPeerIdByReplicaId(targetReplicaId);
            if (!string.IsNullOrWhiteSpace(targetPeerId))
            {
                logger.LogTrace("Stateless proxy forwarding explicit operations for document {DocumentId} towards remote shard owner {TargetReplica} via peer {TargetPeer}.", DocumentId, targetReplicaId, targetPeerId);
                await directSender.SendDirectAsync(new PeerId(Guid.Parse(targetPeerId)), wrapperBytes, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public Task ApplyJournaledOperationsAsync(IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Stateless proxies bypass standard offline journaling bounds structurally completely.");
    }

    public Task MergeSnapshotAsync(byte[] snapshotData, ShardVersionVectorMap globalState, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Stateless proxies do not merge network payloads actively strictly dynamically.");
    }

    public Task CheckpointAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task EvictReplicaAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Stateless proxies bypass generalized cluster constraints handling evictions globally natively.");
    }

    public Task ResetLocalStateAsync(string oldReplicaId, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Stateless proxies correctly execute decoupled from exact underlying structural boundaries.");
    }
}