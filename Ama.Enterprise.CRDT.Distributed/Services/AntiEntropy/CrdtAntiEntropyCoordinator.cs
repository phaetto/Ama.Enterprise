namespace Ama.Enterprise.CRDT.Distributed.Services.AntiEntropy;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Pooling;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.Commands;
using Ama.Enterprise.CRDT.Distributed.Services.Documents;
using Ama.Enterprise.CRDT.Distributed.Services.Lifecycle;
using Ama.Enterprise.CRDT.Distributed.Services.Sharding;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Default implementation of <see cref="ICrdtAntiEntropyCoordinator"/> managing anti-entropy scheduling, shard concurrency locks, and snapshot dispatches.
/// </summary>
public sealed class CrdtAntiEntropyCoordinator : ICrdtAntiEntropyCoordinator
{
    private readonly IServiceProvider serviceProvider;
    private readonly IShardDistributionManager distributionManager;
    private readonly IDistributedCrdtTelemetry telemetry;
    private readonly ICrdtStringPool stringPool;
    private readonly ILogger<CrdtAntiEntropyCoordinator> logger;
    private readonly DistributedCrdtOptions options;
    private readonly ConcurrentDictionary<string, byte> activeAntiEntropyShards = new(StringComparer.Ordinal);
    private int activeAntiEntropyCount;

    public CrdtAntiEntropyCoordinator(
        IServiceProvider serviceProvider,
        IShardDistributionManager distributionManager,
        IDistributedCrdtTelemetry telemetry,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtAntiEntropyCoordinator> logger,
        ICrdtStringPool? stringPool = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.distributionManager = distributionManager ?? throw new ArgumentNullException(nameof(distributionManager));
        this.telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.stringPool = stringPool ?? CrdtStringPool.Shared;
        this.options = options.Value;
    }

    /// <inheritdoc />
    public bool IsAntiEntropyBusy => Volatile.Read(ref activeAntiEntropyCount) > 0 || !activeAntiEntropyShards.IsEmpty;

    /// <inheritdoc />
    public async Task<CrdtAntiEntropyWorkLoadDto> EvaluateAndDispatchAntiEntropyStateAsync(
        ICrdtDocumentOrchestrator orchestrator,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(orchestrator);

        var commandPipeline = serviceProvider.GetService<IDocumentCommandPipeline>();
        var versionVectorManager = serviceProvider.GetService<IShardVersionVectorManager>();
        var orchestratorPending = (orchestrator as CrdtDocumentOrchestrator)?.PendingCommandCount ?? 0;
        var inboundNetworkDepth = GetPendingInboundNetworkQueueDepth();
        var queueDepth = orchestratorPending + (commandPipeline?.PendingCommandCount ?? 0) + inboundNetworkDepth;
        var busyThreshold = options.AntiEntropyBusyQueueThreshold;
        var isPipelineOverloaded = (busyThreshold > 0 && queueDepth >= busyThreshold) || inboundNetworkDepth > 0;

        var localShards = await GetLocalAntiEntropyShardsAsync(orchestrator, versionVectorManager, cancellationToken).ConfigureAwait(false);
        var snapshotActivityWindowMs = Math.Max(2000L, options.AntiEntropyStartupIntervalSeconds * 1500L);
        var totalRemainingSnapshotDocs = 0;
        var availableShards = new HashSet<string>(StringComparer.Ordinal);

        foreach (var shardId in localShards)
        {
            var isShardReceivingSnapshots = false;
            if (versionVectorManager != null &&
                versionVectorManager.TryGetShardSnapshotReceptionState(shardId, snapshotActivityWindowMs, out var shardRemainingDocs))
            {
                isShardReceivingSnapshots = true;
                totalRemainingSnapshotDocs += shardRemainingDocs;
            }

            if (!isShardReceivingSnapshots && !activeAntiEntropyShards.ContainsKey(shardId))
            {
                availableShards.Add(shardId);
            }
        }

        if (versionVectorManager != null &&
            versionVectorManager.TryGetActiveSnapshotReceptionState(snapshotActivityWindowMs, out var globalRemainingDocs) &&
            globalRemainingDocs > totalRemainingSnapshotDocs)
        {
            totalRemainingSnapshotDocs = globalRemainingDocs;
        }

        var isGlobalLockHeld = Volatile.Read(ref activeAntiEntropyCount) > 0;
        var hasAvailableShards = localShards.Count == 0
            ? (!isGlobalLockHeld && totalRemainingSnapshotDocs == 0)
            : (availableShards.Count > 0 && !isGlobalLockHeld);

        var isAlreadyWorking = isPipelineOverloaded || !hasAvailableShards;

        var estimatedDelta = await EstimatePendingAntiEntropyItemsAsync(cancellationToken).ConfigureAwait(false);
        if (totalRemainingSnapshotDocs > estimatedDelta)
        {
            estimatedDelta = totalRemainingSnapshotDocs;
        }

        if (isAlreadyWorking)
        {
            logger.LogDebug(
                "Skipping anti-entropy state dispatch because all local shards or queues are currently busy (QueueDepth: {QueueDepth}, InboundNetworkDepth: {InboundNetworkDepth}, BusyShards: {BusyShardCount}/{TotalShardCount}, RemainingSnapshotDocs: {RemainingSnapshotDocs}, EstimatedPendingItems: {EstimatedItems}).",
                queueDepth,
                inboundNetworkDepth,
                localShards.Count - availableShards.Count,
                localShards.Count,
                totalRemainingSnapshotDocs,
                estimatedDelta);

            return new CrdtAntiEntropyWorkLoadDto(
                isBusy: true,
                pendingQueueDepth: Math.Max(queueDepth, totalRemainingSnapshotDocs),
                estimatedPendingSyncItems: estimatedDelta);
        }

        var acquiredShards = new List<string>(availableShards.Count);
        foreach (var shardId in availableShards)
        {
            if (activeAntiEntropyShards.TryAdd(shardId, 1))
            {
                acquiredShards.Add(shardId);
            }
        }

        if (localShards.Count > 0 && acquiredShards.Count == 0)
        {
            return new CrdtAntiEntropyWorkLoadDto(
                isBusy: true,
                pendingQueueDepth: Math.Max(queueDepth, totalRemainingSnapshotDocs),
                estimatedPendingSyncItems: estimatedDelta);
        }

        try
        {
            var shardsFilter = acquiredShards.Count > 0 ? new HashSet<string>(acquiredShards, StringComparer.Ordinal) : null;
            await DispatchAntiEntropyStateCoreAsync(orchestrator, shardsFilter, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            for (int i = 0; i < acquiredShards.Count; i++)
            {
                activeAntiEntropyShards.TryRemove(acquiredShards[i], out _);
            }
        }

        var postOrchestratorPending = (orchestrator as CrdtDocumentOrchestrator)?.PendingCommandCount ?? 0;
        var postQueueDepth = postOrchestratorPending + (commandPipeline?.PendingCommandCount ?? 0) + GetPendingInboundNetworkQueueDepth();
        return new CrdtAntiEntropyWorkLoadDto(
            isBusy: false,
            pendingQueueDepth: Math.Max(queueDepth, postQueueDepth),
            estimatedPendingSyncItems: estimatedDelta);
    }

    /// <inheritdoc />
    public Task DispatchAntiEntropyStateAsync(
        ICrdtDocumentOrchestrator orchestrator,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(orchestrator);
        return DispatchAntiEntropyStateCoreAsync(orchestrator, null, cancellationToken);
    }

    /// <inheritdoc />
    public Task DispatchAntiEntropyStateAsync(
        ICrdtDocumentOrchestrator orchestrator,
        PeerId targetPeerId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(orchestrator);
        return DispatchAntiEntropyStateAsync(orchestrator, targetPeerId, null, cancellationToken);
    }

    /// <inheritdoc />
    public async Task DispatchTopologyAnnouncementAsync(
        ICrdtDocumentOrchestrator orchestrator,
        PeerId targetPeerId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(orchestrator);
        if (targetPeerId.Value == Guid.Empty) return;

        try
        {
            var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
            var logicalScopeId = GetLogicalScopeId(replicaContext.ReplicaId);
            if (!await CanCommunicateScopeWithPeerAsync(logicalScopeId, targetPeerId.Value.ToString(), "CrdtSync", cancellationToken).ConfigureAwait(false))
            {
                return;
            }

            var versionVectorManager = serviceProvider.GetRequiredService<IShardVersionVectorManager>();
            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();

            var announcementShards = new HashSet<string>(StringComparer.Ordinal);
            if (orchestrator.Registry != null)
            {
                versionVectorManager.EnsureShardRegistered(orchestrator.Registry.ShardId);
                announcementShards.Add(orchestrator.Registry.ShardId);
            }

            var registryState = announcementShards.Count > 0
                ? await versionVectorManager.GetShardsSnapshotAsync(announcementShards, cancellationToken).ConfigureAwait(false)
                : new ShardVersionVectorMap();

            var syncMsg = new CrdtStateSyncMessage(replicaContext.ReplicaId, registryState, IsTopologyAnnouncementOnly: true);
            var payload = serializer.SerializeToBytes(syncMsg);

            var wrapper = new CrdtMessageWrapper(
                "Cluster",
                "CrdtSync",
                payload,
                replicaContext.ReplicaId,
                ScopeId: GetWireScopeId());
            var finalBytes = serializer.SerializeToBytes(wrapper);

            await directSender.SendDirectAsync(targetPeerId, finalBytes, cancellationToken).ConfigureAwait(false);

            telemetry.RecordOrchestratorAntiEntropySync(replicaContext.ReplicaId);
            logger.LogTrace("Sent topology announcement and registry state sync to peer {PeerId}.", targetPeerId.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to dispatch topology announcement to peer {PeerId}.", targetPeerId.Value);
        }
    }

    /// <inheritdoc />
    public async Task ProvideSnapshotAsync(
        ICrdtDocumentOrchestrator orchestrator,
        string documentId,
        string? typeAlias,
        string targetReplicaId,
        PeerId targetPeerId,
        IReadOnlyList<string>? shardDocumentIds = null,
        ShardVersionVectorMap? consistentGlobalState = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(orchestrator);
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        documentId = stringPool.GetOrAdd(documentId);
        if (!string.IsNullOrWhiteSpace(typeAlias))
        {
            typeAlias = stringPool.GetOrAdd(typeAlias);
        }

        var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
        var logicalScopeId = GetLogicalScopeId(replicaContext.ReplicaId);
        var isExpected = await CanReplicateScopeToPeerAsync(logicalScopeId, targetPeerId.Value.ToString(), "CrdtSnapshot", cancellationToken).ConfigureAwait(false);
        if (!isExpected)
        {
            logger.LogWarning("Rejected snapshot request for document {DocumentId} from excluded peer {PeerId}.", documentId, targetPeerId.Value);
            return;
        }

        if (!IsPeerConnectedInBoundMeshes(replicaContext.ReplicaId, targetPeerId, cancellationToken))
        {
            logger.LogWarning("Rejected snapshot request for document {DocumentId} from peer {PeerId} not connected in replica {ReplicaId} bound meshes.", documentId, targetPeerId.Value, replicaContext.ReplicaId);
            return;
        }

        var activeDocs = orchestrator.GetActiveDocuments();
        IDistributedCrdtDocument? targetDoc = null;

        for (int i = 0; i < activeDocs.Count; i++)
        {
            var doc = activeDocs[i];
            if (string.Equals(doc.DocumentId, documentId, StringComparison.Ordinal) &&
                (string.IsNullOrWhiteSpace(typeAlias) ||
                 string.Equals(doc.TypeAlias, typeAlias, StringComparison.Ordinal) ||
                 string.Equals(documentId, Constants.SystemRegistryDocumentId, StringComparison.Ordinal)))
            {
                targetDoc = doc;
                break;
            }
        }

        try
        {
            byte[] snapshotData;
            ShardVersionVectorMap globalState;
            string? documentTypeAlias = typeAlias;

            if (targetDoc != null)
            {
                var snapshotResult = await targetDoc.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
                snapshotData = snapshotResult.SnapshotData;
                globalState = consistentGlobalState ?? snapshotResult.GlobalState;
                if (string.IsNullOrWhiteSpace(documentTypeAlias))
                {
                    documentTypeAlias = targetDoc.TypeAlias;
                }
            }
            else
            {
                snapshotData = await orchestrator.TryLoadStoredDocumentSnapshotBytesAsync(documentId, typeAlias, cancellationToken).ConfigureAwait(false);
                var shardId = await orchestrator.TryGetShardIdAsync(documentId, typeAlias, cancellationToken).ConfigureAwait(false);

                if (snapshotData.Length == 0 && !string.IsNullOrWhiteSpace(shardId) && await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false))
                {
                    targetDoc = await orchestrator.ResolveOrPromoteLocalDocumentAsync(documentId, typeAlias, cancellationToken).ConfigureAwait(false);
                    if (targetDoc != null)
                    {
                        var snapshotResult = await targetDoc.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
                        snapshotData = snapshotResult.SnapshotData;
                        globalState = consistentGlobalState ?? snapshotResult.GlobalState;
                        if (string.IsNullOrWhiteSpace(documentTypeAlias))
                        {
                            documentTypeAlias = targetDoc.TypeAlias;
                        }
                    }
                    else
                    {
                        globalState = consistentGlobalState ?? new ShardVersionVectorMap();
                    }
                }
                else
                {
                    var versionVectorManager = serviceProvider.GetRequiredService<IShardVersionVectorManager>();
                    globalState = consistentGlobalState ?? (!string.IsNullOrWhiteSpace(shardId)
                        ? versionVectorManager.GetShardSnapshot(shardId)
                        : new ShardVersionVectorMap());
                }

                if (snapshotData.Length == 0)
                {
                    logger.LogWarning("Requested snapshot for unknown document {DocumentId} ({TypeAlias}).", documentId, typeAlias);
                    return;
                }
            }

            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();

            IList<string>? batchDocIds = shardDocumentIds != null
                ? (shardDocumentIds as IList<string> ?? new List<string>(shardDocumentIds))
                : null;

            var resMsg = new CrdtSnapshotMessage(replicaContext.ReplicaId, snapshotData, globalState, batchDocIds);
            var payload = serializer.SerializeToBytes(resMsg);

            if (string.IsNullOrWhiteSpace(documentTypeAlias) &&
                orchestrator.Registry != null &&
                !string.Equals(documentId, orchestrator.Registry.DocumentId, StringComparison.Ordinal))
            {
                var registryDoc = await orchestrator.Registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
                if (registryDoc.Data.Documents.TryGetValue(documentId, out var entry) && entry != null)
                {
                    documentTypeAlias = entry.TypeAlias;
                }
                else
                {
                    foreach (var candidate in registryDoc.Data.Documents.Values)
                    {
                        if (string.Equals(candidate.DocumentId, documentId, StringComparison.Ordinal))
                        {
                            documentTypeAlias = candidate.TypeAlias;
                            break;
                        }
                    }
                }
            }

            var wrapper = new CrdtMessageWrapper(
                documentId,
                "CrdtSnapshot",
                payload,
                replicaContext.ReplicaId,
                documentTypeAlias,
                logicalScopeId);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            await directSender.SendDirectAsync(targetPeerId, finalBytes, cancellationToken).ConfigureAwait(false);

            telemetry.RecordOrchestratorSnapshotDispatched(documentId, finalBytes.Length);
            logger.LogInformation("Sent full snapshot of document {DocumentId} ({TypeAlias}) to peer {PeerId}.", documentId, documentTypeAlias, targetPeerId.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to send snapshot for document {DocumentId} ({TypeAlias}).", documentId, typeAlias);
        }
    }

    /// <inheritdoc />
    public IDisposable? TryAcquireAntiEntropyWorkScope(int orchestratorPendingCommandCount)
    {
        var commandPipeline = serviceProvider.GetService<IDocumentCommandPipeline>();
        var queueDepth = orchestratorPendingCommandCount + (commandPipeline?.PendingCommandCount ?? 0) + GetPendingInboundNetworkQueueDepth();
        var busyThreshold = options.AntiEntropyBusyQueueThreshold;
        if (busyThreshold > 0 && queueDepth >= busyThreshold)
        {
            return null;
        }

        if (Interlocked.CompareExchange(ref activeAntiEntropyCount, 1, 0) != 0 || !activeAntiEntropyShards.IsEmpty)
        {
            return null;
        }

        return new AntiEntropyWorkScope(this);
    }

    /// <inheritdoc />
    public IDisposable? TryAcquireAntiEntropyWorkScope(string shardId, int orchestratorPendingCommandCount)
    {
        if (string.IsNullOrWhiteSpace(shardId))
        {
            return TryAcquireAntiEntropyWorkScope(orchestratorPendingCommandCount);
        }

        var commandPipeline = serviceProvider.GetService<IDocumentCommandPipeline>();
        var queueDepth = orchestratorPendingCommandCount + (commandPipeline?.PendingCommandCount ?? 0) + GetPendingInboundNetworkQueueDepth();
        var busyThreshold = options.AntiEntropyBusyQueueThreshold;
        if (busyThreshold > 0 && queueDepth >= busyThreshold)
        {
            return null;
        }

        if (Volatile.Read(ref activeAntiEntropyCount) > 0)
        {
            return null;
        }

        shardId = stringPool.GetOrAdd(shardId);
        if (!activeAntiEntropyShards.TryAdd(shardId, 1))
        {
            return null;
        }

        if (Volatile.Read(ref activeAntiEntropyCount) > 0)
        {
            activeAntiEntropyShards.TryRemove(shardId, out _);
            return null;
        }

        return new ShardAntiEntropyWorkScope(this, shardId);
    }

    private async Task DispatchAntiEntropyStateCoreAsync(
        ICrdtDocumentOrchestrator orchestrator,
        IReadOnlySet<string>? allowedShards,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
            var logicalScopeId = GetLogicalScopeId(replicaContext.ReplicaId);
            var peerRegistry = serviceProvider.GetService<IPeerRegistry>();
            if (peerRegistry == null)
            {
                return;
            }

            var boundMeshIds = GetBoundMeshIds(replicaContext.ReplicaId);

            var validPeers = new List<PeerId>();
            var seenPeerIds = new HashSet<Guid>();

            foreach (var meshId in boundMeshIds)
            {
                var activePeers = await peerRegistry.GetPeersByStatusAsync(meshId, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
                foreach (var peer in activePeers)
                {
                    if (!seenPeerIds.Add(peer.Id.Value))
                    {
                        continue;
                    }

                    var networkIdStr = peer.Id.Value.ToString();
                    var isExpected = await CanCommunicateScopeWithPeerAsync(logicalScopeId, networkIdStr, "CrdtSync", cancellationToken).ConfigureAwait(false);
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
            await DispatchAntiEntropyStateAsync(orchestrator, targetPeerId, allowedShards, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to dispatch state sync to peer.");
        }
    }

    private async Task DispatchAntiEntropyStateAsync(
        ICrdtDocumentOrchestrator orchestrator,
        PeerId targetPeerId,
        IReadOnlySet<string>? allowedShards,
        CancellationToken cancellationToken = default)
    {
        if (targetPeerId.Value == Guid.Empty) return;

        try
        {
            var replicaContext = serviceProvider.GetRequiredService<ReplicaContext>();
            var logicalScopeId = GetLogicalScopeId(replicaContext.ReplicaId);
            var isExpected = await CanCommunicateScopeWithPeerAsync(logicalScopeId, targetPeerId.Value.ToString(), "CrdtSync", cancellationToken).ConfigureAwait(false);
            if (!isExpected)
            {
                return;
            }

            var versionVectorManager = serviceProvider.GetRequiredService<IShardVersionVectorManager>();
            var directSender = serviceProvider.GetRequiredService<IDirectMessageSender>();
            var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();

            HashSet<string> localShards;
            var isPartialSelection = false;
            if (allowedShards != null)
            {
                localShards = new HashSet<string>(allowedShards, StringComparer.Ordinal);
                var allLocalShards = await GetLocalAntiEntropyShardsAsync(orchestrator, versionVectorManager, cancellationToken).ConfigureAwait(false);
                isPartialSelection = localShards.Count < allLocalShards.Count;
            }
            else
            {
                localShards = await GetLocalAntiEntropyShardsAsync(orchestrator, versionVectorManager, cancellationToken).ConfigureAwait(false);
            }

            var globalState = await versionVectorManager.GetShardsSnapshotAsync(localShards, cancellationToken).ConfigureAwait(false);

            var syncMsg = new CrdtStateSyncMessage(
                replicaContext.ReplicaId,
                globalState,
                IsTopologyAnnouncementOnly: false,
                IsPartialShardSelection: isPartialSelection);
            var payload = serializer.SerializeToBytes(syncMsg);

            var wrapper = new CrdtMessageWrapper(
                "Cluster",
                "CrdtSync",
                payload,
                replicaContext.ReplicaId,
                ScopeId: GetWireScopeId());
            var finalBytes = serializer.SerializeToBytes(wrapper);

            await directSender.SendDirectAsync(targetPeerId, finalBytes, cancellationToken).ConfigureAwait(false);

            telemetry.RecordOrchestratorAntiEntropySync(replicaContext.ReplicaId);
            logger.LogTrace("Sent full state sync to peer {PeerId}.", targetPeerId.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to dispatch state sync to peer {PeerId}.", targetPeerId.Value);
        }
    }

    private int GetPendingInboundNetworkQueueDepth()
    {
        try
        {
            var replicaContext = serviceProvider.GetService<ReplicaContext>();
            var boundMeshIds = GetBoundMeshIds(replicaContext?.ReplicaId);
            var total = 0;

            foreach (var meshId in boundMeshIds)
            {
                var inboundQueue = serviceProvider.GetKeyedService<IInboundMessageQueue<IMeshMessage>>(meshId);
                if (inboundQueue != null)
                {
                    total += inboundQueue.PendingCount;
                }
            }

            return total;
        }
        catch
        {
            return 0;
        }
    }

    private HashSet<string> GetBoundMeshIds(string? localReplicaId)
    {
        var meshIds = new HashSet<string>(StringComparer.Ordinal);
        var scopeContext = serviceProvider.GetService<CrdtScopeContext>();
        var hostReplicaId = scopeContext?.HostReplicaId;

        if (!string.IsNullOrWhiteSpace(localReplicaId) || !string.IsNullOrWhiteSpace(hostReplicaId))
        {
            var p2pRegistrations = serviceProvider.GetServices<DistributedCrdtP2pRegistration>();
            foreach (var registration in p2pRegistrations)
            {
                if ((string.Equals(registration.ReplicaId, localReplicaId, StringComparison.Ordinal) ||
                     (!string.IsNullOrWhiteSpace(hostReplicaId) && string.Equals(registration.ReplicaId, hostReplicaId, StringComparison.Ordinal))) &&
                    !string.IsNullOrWhiteSpace(registration.MeshId))
                {
                    meshIds.Add(registration.MeshId);
                }
            }
        }

        if (meshIds.Count == 0)
        {
            var meshes = serviceProvider.GetServices<P2pMeshMetadata>();
            foreach (var mesh in meshes)
            {
                if (!string.IsNullOrWhiteSpace(mesh.MeshId))
                {
                    meshIds.Add(mesh.MeshId);
                }
            }
        }

        return meshIds;
    }

    private string GetLogicalScopeId(string fallbackReplicaId)
    {
        var scopeContext = serviceProvider.GetService<CrdtScopeContext>();
        return !string.IsNullOrWhiteSpace(scopeContext?.ScopeId) ? scopeContext.ScopeId : fallbackReplicaId;
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

    private async ValueTask<bool> CanReplicateScopeToPeerAsync(
        string scopeId,
        string peerNetworkId,
        string messageType,
        CancellationToken cancellationToken)
    {
        var topologyProvider = serviceProvider.GetService<IScopeTopologyProvider>();
        if (topologyProvider != null)
        {
            return await topologyProvider.CanReplicateScopeToPeerAsync(
                new ScopeAccessContext(scopeId, peerNetworkId, null, messageType),
                cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    private async ValueTask<bool> CanCommunicateScopeWithPeerAsync(
        string scopeId,
        string peerNetworkId,
        string messageType,
        CancellationToken cancellationToken)
    {
        var topologyProvider = serviceProvider.GetService<IScopeTopologyProvider>();
        if (topologyProvider != null)
        {
            var ctx = new ScopeAccessContext(scopeId, peerNetworkId, null, messageType);
            if (await topologyProvider.CanReplicateScopeToPeerAsync(ctx, cancellationToken).ConfigureAwait(false))
            {
                return true;
            }

            return await topologyProvider.CanAcceptScopeFromPeerAsync(ctx, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    private bool IsPeerConnectedInBoundMeshes(string localReplicaId, PeerId targetPeerId, CancellationToken cancellationToken)
    {
        var peerRegistry = serviceProvider.GetService<IPeerRegistry>();
        if (peerRegistry == null)
        {
            return true;
        }

        var boundMeshIds = GetBoundMeshIds(localReplicaId);
        if (boundMeshIds.Count == 0)
        {
            return true;
        }

        var hasAnyRegisteredPeers = false;
        foreach (var meshId in boundMeshIds)
        {
            var peers = peerRegistry.GetPeersByStatusAsync(meshId, PeerStatus.Active, cancellationToken).GetAwaiter().GetResult();
            foreach (var peer in peers)
            {
                hasAnyRegisteredPeers = true;
                if (peer.Id.Value == targetPeerId.Value)
                {
                    return true;
                }
            }
        }

        return !hasAnyRegisteredPeers;
    }

    private async ValueTask<HashSet<string>> GetLocalAntiEntropyShardsAsync(
        ICrdtDocumentOrchestrator orchestrator,
        IShardVersionVectorManager? versionVectorManager,
        CancellationToken cancellationToken)
    {
        var activeShardIds = new HashSet<string>(StringComparer.Ordinal);
        if (orchestrator.Registry != null)
        {
            activeShardIds.Add(orchestrator.Registry.ShardId);
        }

        var activeDocs = orchestrator.GetActiveDocuments();
        for (int i = 0; i < activeDocs.Count; i++)
        {
            activeShardIds.Add(activeDocs[i].ShardId);
        }

        IReadOnlySet<string> knownShardIds = versionVectorManager != null
            ? versionVectorManager.EnsureShardsRegisteredAndGetAll(activeShardIds)
            : activeShardIds;

        var localShards = new HashSet<string>(StringComparer.Ordinal);
        if (orchestrator.Registry != null)
        {
            localShards.Add(orchestrator.Registry.ShardId);
        }

        foreach (var shardId in knownShardIds)
        {
            if (await distributionManager.IsShardLocalAsync(shardId, cancellationToken).ConfigureAwait(false))
            {
                localShards.Add(shardId);
            }
        }

        return localShards;
    }

    private async ValueTask<long> EstimatePendingAntiEntropyItemsAsync(CancellationToken cancellationToken)
    {
        try
        {
            var versionVectorManager = serviceProvider.GetService<IShardVersionVectorManager>();
            var clusterTracker = serviceProvider.GetService<IClusterStateTracker>();
            var replicaContext = serviceProvider.GetService<ReplicaContext>();
            var logicalScopeId = GetLogicalScopeId(replicaContext?.ReplicaId ?? string.Empty);
            if (versionVectorManager == null || clusterTracker == null)
            {
                return 0;
            }

            var localState = await versionVectorManager.GetFullSnapshotAsync(cancellationToken).ConfigureAwait(false);
            if (localState.Count == 0)
            {
                return 0;
            }

            var filteredStates = new List<ShardVersionVectorMap>();
            foreach (var replicaId in clusterTracker.GetActiveReplicaIds())
            {
                var peerNetworkId = clusterTracker.GetPeerIdByReplicaId(replicaId);
                if (!string.IsNullOrWhiteSpace(peerNetworkId))
                {
                    var isExpected = await CanReplicateScopeToPeerAsync(logicalScopeId, peerNetworkId, "CrdtSync", cancellationToken).ConfigureAwait(false);
                    if (!isExpected)
                    {
                        continue;
                    }
                }

                var state = clusterTracker.TryGetPeerState(replicaId);
                if (state != null)
                {
                    filteredStates.Add(state);
                }
            }

            IReadOnlyList<ShardVersionVectorMap> peerStates = filteredStates;

            if (peerStates.Count == 0)
            {
                return 0;
            }

            long maxEstimatedGap = 0;

            for (int i = 0; i < peerStates.Count; i++)
            {
                var peerState = peerStates[i];
                long peerGap = 0;

                foreach (var localShardKvp in localState)
                {
                    var shardId = localShardKvp.Key;
                    var localDvv = localShardKvp.Value;
                    peerState.TryGetValue(shardId, out var peerDvv);

                    if (localDvv.Versions != null)
                    {
                        foreach (var verKvp in localDvv.Versions)
                        {
                            long peerVer = 0;
                            if (peerDvv?.Versions != null)
                            {
                                peerDvv.Versions.TryGetValue(verKvp.Key, out peerVer);
                            }

                            if (verKvp.Value != peerVer)
                            {
                                peerGap += Math.Abs(verKvp.Value - peerVer);
                            }
                        }
                    }

                    if (peerDvv?.Versions != null)
                    {
                        foreach (var peerVerKvp in peerDvv.Versions)
                        {
                            if (localDvv.Versions == null || !localDvv.Versions.ContainsKey(peerVerKvp.Key))
                            {
                                peerGap += Math.Max(0, peerVerKvp.Value);
                            }
                        }
                    }
                }

                if (peerGap > maxEstimatedGap)
                {
                    maxEstimatedGap = peerGap;
                }
            }

            return maxEstimatedGap;
        }
        catch
        {
            return 0;
        }
    }

    private sealed class AntiEntropyWorkScope(CrdtAntiEntropyCoordinator owner) : IDisposable
    {
        private int disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
            {
                Interlocked.Decrement(ref owner.activeAntiEntropyCount);
            }
        }
    }

    private sealed class ShardAntiEntropyWorkScope(CrdtAntiEntropyCoordinator owner, string shardId) : IDisposable
    {
        private int disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
            {
                owner.activeAntiEntropyShards.TryRemove(shardId, out _);
            }
        }
    }
}