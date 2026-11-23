namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Observes network connections and triggers anti-entropy synchronization across meshes.
/// </summary>
public sealed class CrdtTopologyObserver : IPeerTopologyObserver, IDisposable
{
    private readonly string meshId;
    private readonly string replicaId;
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly IServiceProvider serviceProvider;
    private readonly ILogger<CrdtTopologyObserver> logger;
    private IPeerRegistry? peerRegistry;

    private readonly ConcurrentDictionary<Guid, PeerId> pendingPeers = new();
    private readonly Lock stabilizationLock = new();
    private CancellationTokenSource? stabilizationCts;
    private readonly CancellationTokenSource disposeCts = new();
    private int initialSyncTriggered;
    private int disposed;

    private readonly Meter meter;
    private readonly Counter<long> peersJoinedCounter;
    private readonly Counter<long> peersDepartedCounter;

    public CrdtTopologyObserver(
        string meshId,
        string replicaId,
        DistributedCrdtScopeManager scopeManager,
        IServiceProvider serviceProvider,
        ILogger<CrdtTopologyObserver> logger,
        IMeterFactory? meterFactory = null)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.replicaId = replicaId ?? throw new ArgumentNullException(nameof(replicaId));
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtTopologyObserver") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtTopologyObserver");
        this.peersJoinedCounter = this.meter.CreateCounter<long>("crdt.topology.peers_joined", "peers", "Total peer connections");
        this.peersDepartedCounter = this.meter.CreateCounter<long>("crdt.topology.peers_departed", "peers", "Total peers departed");
    }

    /// <inheritdoc />
    public async Task OnPeerJoinedAsync(string eventMeshId, PeerNode node, CancellationToken cancellationToken)
    {
        if (!string.Equals(meshId, eventMeshId, StringComparison.Ordinal))
        {
            return;
        }

        peersJoinedCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", eventMeshId));

        if (node.Id.Value == Guid.Empty || Volatile.Read(ref disposed) != 0)
        {
            return;
        }

        try
        {
            var scope = scopeManager.GetOrCreateScope(replicaId);
            var topologyProvider = scope.ServiceProvider.GetRequiredService<IScopeTopologyProvider>();
            var isExpected = await topologyProvider.IsPeerExpectedAsync(node.Id.Value.ToString(), cancellationToken).ConfigureAwait(false);
            if (!isExpected)
            {
                return;
            }

            var options = scope.ServiceProvider.GetService<IOptions<DistributedCrdtOptions>>()?.Value;
            var enableSharding = options?.EnableSharding ?? false;
            var stabilizationDelayMs = options?.TopologyStabilizationDelayMilliseconds ?? 500;

            if (!enableSharding || stabilizationDelayMs <= 0)
            {
                var registry = peerRegistry ??= serviceProvider.GetRequiredService<IPeerRegistry>();
                var activePeers = await registry.GetPeersByStatusAsync(eventMeshId, PeerStatus.Active, cancellationToken);
                if (activePeers.Count() <= 1 && Interlocked.CompareExchange(ref initialSyncTriggered, 1, 0) == 0)
                {
                    logger.LogInformation(
                        "[{MeshId}] Connected to first peer {PeerId}. Triggering immediate DVV state sync for replica {ReplicaId}.",
                        eventMeshId,
                        node.Id,
                        replicaId);
                    await scope.Orchestrator.DispatchAntiEntropyStateAsync(node.Id, cancellationToken).ConfigureAwait(false);
                }

                return;
            }

            // Phase 1: Immediately exchange replica identity and system registry DVV so the consistent hash ring
            // includes all joining peers before any user data shard ownership or snapshots are evaluated.
            logger.LogDebug(
                "[{MeshId}] Connected to peer {PeerId} in sharded mode. Sending immediate topology announcement for replica {ReplicaId} and scheduling stabilized shard sync in {DelayMs}ms.",
                eventMeshId,
                node.Id,
                replicaId,
                stabilizationDelayMs);

            await scope.Orchestrator.DispatchTopologyAnnouncementAsync(node.Id, cancellationToken).ConfigureAwait(false);

            // Phase 2: Debounce full data shard synchronization across joining peers so the hash ring stabilizes first.
            pendingPeers[node.Id.Value] = node.Id;
            ScheduleStabilizedDataSync(stabilizationDelayMs);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to dispatch state sync upon connecting to peer {PeerId}.", eventMeshId, node.Id);
        }
    }

    /// <inheritdoc />
    public Task OnPeerDepartedAsync(string eventMeshId, PeerId peerId, CancellationToken cancellationToken)
    {
        if (!string.Equals(meshId, eventMeshId, StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        if (peerId.Value != Guid.Empty)
        {
            pendingPeers.TryRemove(peerId.Value, out _);

            var stringId = peerId.Value.ToString();
            logger.LogInformation("[{MeshId}] Peer {PeerId} departed. Preserving localized replica {ReplicaId} CRDT states.", eventMeshId, stringId, replicaId);
            
            var scope = scopeManager.GetOrCreateScope(replicaId);
            scope.ClusterTracker.RemovePeerByNetworkId(stringId);
            peersDepartedCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", eventMeshId));
        }
        
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task OnPeerStatusChangedAsync(string eventMeshId, PeerId peerId, PeerStatus newStatus, CancellationToken cancellationToken)
    {
        if (!string.Equals(meshId, eventMeshId, StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        if (newStatus == PeerStatus.Dead)
        {
            if (peerId.Value != Guid.Empty)
            {
                pendingPeers.TryRemove(peerId.Value, out _);

                var stringId = peerId.Value.ToString();
                logger.LogInformation("[{MeshId}] Peer {PeerId} marked as Dead targeting localized replica {ReplicaId}.", eventMeshId, stringId, replicaId);
                
                var scope = scopeManager.GetOrCreateScope(replicaId);
                scope.ClusterTracker.RemovePeerByNetworkId(stringId);
                peersDepartedCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", eventMeshId));
            }
        }
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        CancellationTokenSource? ctsToCancel;
        lock (stabilizationLock)
        {
            ctsToCancel = stabilizationCts;
            stabilizationCts = null;
        }

        try
        {
            ctsToCancel?.Cancel();
            ctsToCancel?.Dispose();
        }
        catch (ObjectDisposedException)
        {
        }

        try
        {
            disposeCts.Cancel();
            disposeCts.Dispose();
        }
        catch (ObjectDisposedException)
        {
        }

        meter.Dispose();
    }

    private void ScheduleStabilizedDataSync(int delayMs)
    {
        if (Volatile.Read(ref disposed) != 0)
        {
            return;
        }

        CancellationTokenSource? previousCts;
        CancellationTokenSource currentCts;

        lock (stabilizationLock)
        {
            if (Volatile.Read(ref disposed) != 0)
            {
                return;
            }

            previousCts = stabilizationCts;
            currentCts = CancellationTokenSource.CreateLinkedTokenSource(disposeCts.Token);
            stabilizationCts = currentCts;
        }

        try
        {
            previousCts?.Cancel();
            previousCts?.Dispose();
        }
        catch (ObjectDisposedException)
        {
        }

        _ = ExecuteStabilizedDataSyncAsync(delayMs, currentCts);
    }

    private async Task ExecuteStabilizedDataSyncAsync(int delayMs, CancellationTokenSource currentCts)
    {
        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(delayMs), currentCts.Token).ConfigureAwait(false);

            var peersToSync = new List<PeerId>();
            foreach (var kvp in pendingPeers)
            {
                if (pendingPeers.TryRemove(kvp.Key, out var peerId))
                {
                    peersToSync.Add(peerId);
                }
            }

            if (peersToSync.Count == 0)
            {
                return;
            }

            var scope = scopeManager.GetOrCreateScope(replicaId);
            var topologyProvider = scope.ServiceProvider.GetRequiredService<IScopeTopologyProvider>();
            var expectedPeersToSync = new List<PeerId>(peersToSync.Count);
            for (int i = 0; i < peersToSync.Count; i++)
            {
                var candidatePeer = peersToSync[i];
                if (await topologyProvider.IsPeerExpectedAsync(candidatePeer.Value.ToString(), currentCts.Token).ConfigureAwait(false))
                {
                    expectedPeersToSync.Add(candidatePeer);
                }
            }

            if (expectedPeersToSync.Count == 0)
            {
                return;
            }

            await scope.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, currentCts.Token).ConfigureAwait(false);

            var inboundQueue = serviceProvider.GetKeyedService<IInboundMessageQueue<IMeshMessage>>(meshId);
            var commandPipeline = scope.ServiceProvider.GetService<IDocumentCommandPipeline>();
            var versionVectorManager = scope.ServiceProvider.GetService<IShardVersionVectorManager>();
            var options = scope.ServiceProvider.GetService<IOptions<DistributedCrdtOptions>>()?.Value;
            var snapshotActivityWindowMs = Math.Max(2000L, (options?.AntiEntropyStartupIntervalSeconds ?? 2) * 1500L);

            var hasInFlightBacklog =
                (inboundQueue is { PendingCount: > 0 }) ||
                (commandPipeline is { PendingCommandCount: > 0 }) ||
                (versionVectorManager != null && versionVectorManager.TryGetActiveSnapshotReceptionState(snapshotActivityWindowMs, out _));

            if (hasInFlightBacklog)
            {
                logger.LogDebug(
                    "[{MeshId}] Deferring stabilized shard DVV state sync for replica {ReplicaId} because inbound queues or snapshot reception are currently active. Periodic anti-entropy will resume once queues drain.",
                    meshId,
                    replicaId);
                return;
            }

            logger.LogInformation(
                "[{MeshId}] Topology stabilization window ({DelayMs}ms) elapsed with {PeerCount} joined peer(s). Triggering full shard DVV state sync for replica {ReplicaId}.",
                meshId,
                delayMs,
                expectedPeersToSync.Count,
                replicaId);

            for (int i = 0; i < expectedPeersToSync.Count; i++)
            {
                currentCts.Token.ThrowIfCancellationRequested();
                await scope.Orchestrator.DispatchAntiEntropyStateAsync(expectedPeersToSync[i], currentCts.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Debounce reset or observer disposed
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to execute stabilized data shard sync for replica {ReplicaId}.", meshId, replicaId);
        }
    }
}