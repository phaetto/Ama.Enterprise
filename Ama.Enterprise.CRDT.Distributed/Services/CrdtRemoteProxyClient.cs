namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Models.Serialization;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Default implementation of <see cref="ICrdtRemoteProxyClient"/> managing P2P RPC retries, failover, and direct message forwarding.
/// </summary>
public sealed class CrdtRemoteProxyClient : ICrdtRemoteProxyClient
{
    private readonly IServiceProvider serviceProvider;
    private readonly IShardDistributionManager distributionManager;
    private readonly IClusterStateTracker clusterStateTracker;
    private readonly ICrdtSerializer serializer;
    private readonly ReplicaContext replicaContext;
    private readonly IDistributedCrdtTelemetry telemetry;
    private readonly ILogger<CrdtRemoteProxyClient> logger;
    private readonly DistributedCrdtOptions options;

    public CrdtRemoteProxyClient(
        IServiceProvider serviceProvider,
        IShardDistributionManager distributionManager,
        IClusterStateTracker clusterStateTracker,
        ICrdtSerializer serializer,
        ReplicaContext replicaContext,
        IDistributedCrdtTelemetry telemetry,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtRemoteProxyClient> logger)
    {
        ArgumentNullException.ThrowIfNull(options);

        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.distributionManager = distributionManager ?? throw new ArgumentNullException(nameof(distributionManager));
        this.clusterStateTracker = clusterStateTracker ?? throw new ArgumentNullException(nameof(clusterStateTracker));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.replicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));
        this.telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.options = options.Value;
    }

    /// <inheritdoc />
    public async ValueTask<CrdtDocument<TState>> GetRemoteDocumentStateAsync<TState>(
        string documentId,
        string shardId,
        string? documentTypeAlias,
        CancellationToken cancellationToken = default)
        where TState : class, new()
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(shardId)) throw new ArgumentException("Shard ID cannot be null or empty.", nameof(shardId));

        var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
        var rpcCorrelator = serviceProvider.GetRequiredService<IP2pRpcCorrelator>();

        var candidateReplicas = await GetCandidateRemoteReplicasForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
        if (candidateReplicas.Count == 0)
        {
            var noNodesEx = new InvalidOperationException($"Cannot read state: no active nodes found for shard {shardId}.");
            logger.LogError(noNodesEx, "Cannot read state for document {DocumentId}: no active nodes found for shard {ShardId}.", documentId, shardId);
            throw noNodesEx;
        }

        var maxRetries = Math.Max(1, options.RpcMaxRetries);
        var rpcTimeout = TimeSpan.FromMilliseconds(Math.Max(1, options.RpcTimeoutMilliseconds));
        var retryDelayMs = Math.Max(0, options.RpcRetryDelayMilliseconds);
        var wireScopeId = GetWireScopeId();
        Exception? lastException = null;

        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            if (attempt > 0)
            {
                candidateReplicas = await GetCandidateRemoteReplicasForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
                if (candidateReplicas.Count == 0)
                {
                    break;
                }
            }

            bool triedAnyPeer = false;

            for (int i = 0; i < candidateReplicas.Count; i++)
            {
                var candidateReplica = candidateReplicas[(i + attempt) % candidateReplicas.Count];
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
                var wrapper = new CrdtMessageWrapper(
                    documentId,
                    "CrdtReadReq",
                    reqPayload,
                    replicaContext.ReplicaId,
                    documentTypeAlias,
                    wireScopeId);
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

    /// <inheritdoc />
    public async Task<bool> ForwardIntentAsync(
        string documentId,
        string shardId,
        string? documentTypeAlias,
        IOperationIntent intent,
        string targetProperty,
        IntentDeliveryMode deliveryMode,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(shardId)) throw new ArgumentException("Shard ID cannot be null or empty.", nameof(shardId));
        ArgumentNullException.ThrowIfNull(intent);

        var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
        var rpcCorrelator = serviceProvider.GetRequiredService<IP2pRpcCorrelator>();

        var activeNodes = await distributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
        if (activeNodes == null || activeNodes.Count == 0)
        {
            logger.LogWarning("Cannot forward intent: no active nodes found for shard {ShardId}.", shardId);
            telemetry.RecordOrchestratorIntentFailed(documentId);
            return false;
        }

        var intentTypeAlias = CrdtTypeRegistry.GetDiscriminator(intent.GetType());
        var serializedIntent = serializer.SerializeToBytes(intent);
        var wireScopeId = GetWireScopeId();

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
                var ffWrapper = new CrdtMessageWrapper(
                    documentId,
                    "CrdtIntentReq",
                    ffReqPayload,
                    replicaContext.ReplicaId,
                    documentTypeAlias,
                    wireScopeId);
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
                var wrapper = new CrdtMessageWrapper(
                    documentId,
                    "CrdtIntentReq",
                    reqPayload,
                    replicaContext.ReplicaId,
                    documentTypeAlias,
                    wireScopeId);
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

    /// <inheritdoc />
    public async Task ForwardPatchAsync(
        string documentId,
        string shardId,
        string? documentTypeAlias,
        CrdtPatch patch,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(shardId)) throw new ArgumentException("Shard ID cannot be null or empty.", nameof(shardId));

        var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
        var p2pAlgorithm = serviceProvider.GetService<IP2pAlgorithm>();
        var wireScopeId = GetWireScopeId();

        var patchMsg = new CrdtPatchMessage(replicaContext.ReplicaId, patch);
        var payload = serializer.SerializeToBytes(patchMsg);
        var wrapper = new CrdtMessageWrapper(
            documentId,
            "CrdtPatch",
            payload,
            replicaContext.ReplicaId,
            documentTypeAlias,
            wireScopeId);
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

    /// <inheritdoc />
    public async Task PushDemotedDocumentSnapshotToTargetPeersAsync(
        IDistributedCrdtDocument document,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(document);

        try
        {
            var directSender = serviceProvider.GetService<IDirectMessageSender>();
            if (directSender == null)
            {
                return;
            }

            var targetNodes = await distributionManager.GetTargetNodesForShardAsync(document.ShardId, cancellationToken).ConfigureAwait(false);
            if (targetNodes == null || targetNodes.Count == 0)
            {
                return;
            }

            var wireScopeId = GetWireScopeId();
            byte[]? finalBytes = null;
            List<Task>? sendTasks = null;

            for (int i = 0; i < targetNodes.Count; i++)
            {
                var targetReplica = targetNodes[i];
                if (string.Equals(targetReplica, replicaContext.ReplicaId, StringComparison.Ordinal))
                {
                    continue;
                }

                var targetPeerStr = clusterStateTracker.GetPeerIdByReplicaId(targetReplica);
                if (string.IsNullOrWhiteSpace(targetPeerStr) || !Guid.TryParse(targetPeerStr, out var peerGuid))
                {
                    continue;
                }

                if (finalBytes == null)
                {
                    var snapshot = await document.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
                    var resMsg = new CrdtSnapshotMessage(replicaContext.ReplicaId, snapshot.SnapshotData, snapshot.GlobalState);
                    var payload = serializer.SerializeToBytes(resMsg);
                    var wrapper = new CrdtMessageWrapper(
                        document.DocumentId,
                        "CrdtSnapshot",
                        payload,
                        replicaContext.ReplicaId,
                        document.TypeAlias,
                        wireScopeId);
                    finalBytes = serializer.SerializeToBytes(wrapper);
                }

                sendTasks ??= new List<Task>(targetNodes.Count);
                sendTasks.Add(directSender.SendDirectAsync(new PeerId(peerGuid), finalBytes, cancellationToken));
            }

            if (sendTasks != null)
            {
                await Task.WhenAll(sendTasks).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to push final snapshot for demoted document {DocumentId} ({TypeAlias}).", document.DocumentId, document.TypeAlias);
        }
    }

    /// <inheritdoc />
    public async Task<List<string>> GetCandidateRemoteReplicasForShardAsync(
        string shardId,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(shardId))
        {
            return new List<string>();
        }

        var candidates = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        var targetNodes = await distributionManager.GetTargetNodesForShardAsync(shardId, cancellationToken).ConfigureAwait(false);
        if (targetNodes != null)
        {
            for (int i = 0; i < targetNodes.Count; i++)
            {
                var replicaId = targetNodes[i];
                if (!string.Equals(replicaId, replicaContext.ReplicaId, StringComparison.Ordinal) && seen.Add(replicaId))
                {
                    candidates.Add(replicaId);
                }
            }
        }

        var activeReplicas = clusterStateTracker.GetActiveReplicaIds();
        for (int i = 0; i < activeReplicas.Count; i++)
        {
            var replicaId = activeReplicas[i];
            if (!string.Equals(replicaId, replicaContext.ReplicaId, StringComparison.Ordinal) && seen.Add(replicaId))
            {
                candidates.Add(replicaId);
            }
        }

        return candidates;
    }

    private string? GetWireScopeId()
    {
        var scopeContext = serviceProvider.GetService<CrdtScopeContext>();
        if (scopeContext != null &&
            !string.IsNullOrWhiteSpace(scopeContext.ScopeId) &&
            !string.Equals(scopeContext.ScopeId, scopeContext.HostReplicaId, StringComparison.Ordinal))
        {
            return scopeContext.ScopeId;
        }

        return null;
    }
}