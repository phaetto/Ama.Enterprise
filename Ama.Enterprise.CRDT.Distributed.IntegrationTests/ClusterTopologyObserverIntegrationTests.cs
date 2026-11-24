namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Attributes;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services.Providers;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

[CrdtAotType(typeof(ClusterTopologyObserverIntegrationTests.TopologyObserverDocState))]
[CrdtAotType(typeof(Dictionary<string, string>))]
public sealed partial class ClusterTopologyObserverTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(ClusterTopologyObserverIntegrationTests.TopologyObserverDocState))]
[JsonSerializable(typeof(CrdtDocument<ClusterTopologyObserverIntegrationTests.TopologyObserverDocState>))]
public sealed partial class ClusterTopologyObserverTestJsonContext : JsonSerializerContext
{
}

/// <summary>
/// Integration tests verifying that <see cref="IClusterStateObserver"/>, <see cref="OrchestratorClusterStateObserver"/>,
/// and <see cref="ICrdtOrchestratorObserver"/> do not fire repeatedly or cause anti-entropy resync loops when
/// non-local shard DVVs are trimmed from memory to keep memory footprint low.
/// </summary>
public sealed class ClusterTopologyObserverIntegrationTests : IAsyncDisposable
{
    private const string MeshId = "TopologyObserverMesh";
    private const string DocTypeAlias = "topo-obs-doc";

    public sealed class TopologyObserverDocState : IEquatable<TopologyObserverDocState>
    {
        public string Id { get; set; } = "topo-obs-doc";

        public Dictionary<string, string> DataMap { get; set; } = new(StringComparer.Ordinal);

        public bool Equals(TopologyObserverDocState? other)
        {
            if (other is null)
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            if (!string.Equals(Id, other.Id, StringComparison.Ordinal) || DataMap.Count != other.DataMap.Count)
            {
                return false;
            }

            foreach (var kvp in DataMap)
            {
                if (!other.DataMap.TryGetValue(kvp.Key, out var otherVal) || !string.Equals(kvp.Value, otherVal, StringComparison.Ordinal))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj) => Equals(obj as TopologyObserverDocState);

        public override int GetHashCode() => HashCode.Combine(Id, DataMap.Count);
    }

    public sealed class CountingClusterStateObserver : IClusterStateObserver
    {
        public int TriggerCount;

        public void OnClusterTopologyChanged()
        {
            Interlocked.Increment(ref TriggerCount);
        }
    }

    public sealed class CountingOrchestratorObserver : ICrdtOrchestratorObserver
    {
        public int TriggerCount;

        public Task OnTopologyChangedAsync(CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref TriggerCount);
            return Task.CompletedTask;
        }
    }

    private sealed class PerDocumentShardIdProvider : IShardIdProvider
    {
        public string GetShardId<T>(CrdtDocument<T> document) where T : class
        {
            if (document.Data is TopologyObserverDocState state && !string.IsNullOrWhiteSpace(state.Id))
            {
                return $"shard-{state.Id}";
            }

            return "default";
        }

        public string GetShardId<T>(IComparable documentId) where T : class
        {
            ArgumentNullException.ThrowIfNull(documentId);
            return $"shard-{documentId}";
        }

        public string GetShardId(IComparable documentId, Type documentType)
        {
            ArgumentNullException.ThrowIfNull(documentId);
            ArgumentNullException.ThrowIfNull(documentType);
            return $"shard-{documentId}";
        }
    }

    private readonly List<TopologyTestNode> nodes = new();

    [IntegrationFact]
    public async Task ShardedCluster_WhenDemotedShardsRemoveLocalDvv_DoesNotFireTopologyObserversRepeatedlyOrResendSnapshotsOnAntiEntropy()
    {
        // Arrange - 3-node sharded cluster with ReplicationFactor = 1 so each node only owns ~1/3 of shards
        // and demotes the remaining ~2/3 (removing their DVVs from local ShardVersionVectorManager to keep memory footprint low).
        using var fabric = new TopologyTestMeshFabric();
        var node1 = await CreateNodeAsync("Replica-1", replicationFactor: 1);
        var node2 = await CreateNodeAsync("Replica-2", replicationFactor: 1);
        var node3 = await CreateNodeAsync("Replica-3", replicationFactor: 1);

        // Start with Node 1 alone and create 15 documents (each in its own shard "shard-doc-{i}")
        await fabric.JoinNodeAsync(node1);

        var docIds = Enumerable.Range(1, 15).Select(i => $"doc-{i}").ToArray();
        foreach (var docId in docIds)
        {
            await node1.Orchestrator.CreateDocumentAsync(docId, DocTypeAlias, CancellationToken.None);
            await node1.Orchestrator.ApplyIntentAsync<TopologyObserverDocState>(
                docId,
                new MapSetIntent("Key1", $"Val-{docId}"),
                nameof(TopologyObserverDocState.DataMap),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
        }

        // Trim journals on Node 1 so any missing shard state during anti-entropy would force a CrdtSnapshot
        foreach (var docId in docIds)
        {
            await node1.Scope.Storage.TrimAsync($"shard-{docId}", new Dictionary<string, long> { ["Replica-1"] = 10 }, CancellationToken.None);
        }

        // Now Node 2 and Node 3 join the cluster -> Node 1 demotes non-local shards and removes their DVVs from memory
        await fabric.JoinNodeAsync(node2);
        await fabric.JoinNodeAsync(node3);

        await SyncClusterRoundsAsync(fabric, rounds: 3);

        // Verify that every node only keeps DVVs in memory for shards it actually owns (plus system-document-registry)
        foreach (var node in new[] { node1, node2, node3 })
        {
            var localDvvSnapshot = await node.VersionVectorManager.GetFullSnapshotAsync(CancellationToken.None);
            var activeDocCount = node.Orchestrator.GetActiveDocuments().Count;
            localDvvSnapshot.Count.ShouldBe(activeDocCount);
            localDvvSnapshot.Count.ShouldBeLessThan(docIds.Length + 1);
        }

        // Reset observer counters and message logs after initial cluster convergence
        foreach (var node in new[] { node1, node2, node3 })
        {
            Interlocked.Exchange(ref node.ClusterObserver.TriggerCount, 0);
            Interlocked.Exchange(ref node.OrchestratorObserver.TriggerCount, 0);
        }
        fabric.RecordedMessages.Clear();

        // Act - Run 5 full-mesh anti-entropy rounds across all 3 nodes
        await SyncClusterRoundsAsync(fabric, rounds: 5);

        // Assert 1 - ClusterStateObserver and OrchestratorObserver must NOT fire at all during steady-state DVV exchanges
        foreach (var node in new[] { node1, node2, node3 })
        {
            Volatile.Read(ref node.ClusterObserver.TriggerCount).ShouldBe(
                0,
                $"ClusterStateObserver on {node.ReplicaId} should not fire again once peers are mapped.");
            Volatile.Read(ref node.OrchestratorObserver.TriggerCount).ShouldBe(
                0,
                $"OrchestratorObserver on {node.ReplicaId} should not fire again when no documents are promoted or demoted.");
        }

        // Assert 2 - Only CrdtSync messages were exchanged; zero CrdtSnapshot or CrdtPatch messages were sent for demoted shards
        var messages = fabric.RecordedMessages.ToList();
        messages.ShouldNotBeEmpty();
        messages.ShouldAllBe(m => m.MessageType == "CrdtSync");
    }

    [IntegrationFact]
    public async Task ShardedCluster_WhenShardIsDemotedAndLaterRePromoted_RestoresDvvFromCheckpointAndDoesNotLoop()
    {
        // Arrange - Node 1 starts alone, creates documents, then Node 2 joins (demoting ~half of shards on Node 1 and removing their DVVs).
        // Later Node 2 departs (re-promoting those shards on Node 1) and rejoins.
        using var fabric = new TopologyTestMeshFabric();
        var node1 = await CreateNodeAsync("Replica-1", replicationFactor: 1);
        var node2 = await CreateNodeAsync("Replica-2", replicationFactor: 1);

        await fabric.JoinNodeAsync(node1);

        var docIds = Enumerable.Range(1, 10).Select(i => $"repromote-doc-{i}").ToArray();
        foreach (var docId in docIds)
        {
            await node1.Orchestrator.CreateDocumentAsync(docId, DocTypeAlias, CancellationToken.None);
            await node1.Orchestrator.ApplyIntentAsync<TopologyObserverDocState>(
                docId,
                new MapSetIntent("Initial", $"V1-{docId}"),
                nameof(TopologyObserverDocState.DataMap),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
        }

        // Node 2 joins -> shards owned by Node 2 are demoted on Node 1 and their DVVs are removed from Node 1's ShardVersionVectorManager
        await fabric.JoinNodeAsync(node2);
        await SyncClusterRoundsAsync(fabric, rounds: 2);

        var demotedOnNode1 = new List<string>();
        foreach (var docId in docIds)
        {
            var shardId = $"shard-{docId}";
            if (!await node1.DistributionManager.IsShardLocalAsync(shardId, CancellationToken.None))
            {
                demotedOnNode1.Add(docId);
            }
        }

        demotedOnNode1.ShouldNotBeEmpty();

        var dvvOnNode1WhileDemoted = await node1.VersionVectorManager.GetFullSnapshotAsync(CancellationToken.None);
        foreach (var demotedDocId in demotedOnNode1)
        {
            dvvOnNode1WhileDemoted.ContainsKey($"shard-{demotedDocId}").ShouldBeFalse();
        }

        // Act 1 - Node 2 departs -> Node 1 re-promotes the demoted shards from its local storage checkpoints
        await fabric.DepartNodeAsync(node2);
        await node1.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);

        // Assert 1 - Re-promoted documents restored their non-zero causal version vector into ShardVersionVectorManager
        var dvvOnNode1AfterRePromotion = await node1.VersionVectorManager.GetFullSnapshotAsync(CancellationToken.None);
        foreach (var rePromotedDocId in demotedOnNode1)
        {
            var shardId = $"shard-{rePromotedDocId}";
            dvvOnNode1AfterRePromotion.ShouldContainKey(shardId);
            dvvOnNode1AfterRePromotion[shardId].Versions.ShouldContainKeyAndValue("Replica-1", 1L);
        }

        // Act 2 - Node 2 rejoins the cluster and syncs with Node 1
        await fabric.JoinNodeAsync(node2);
        await SyncClusterRoundsAsync(fabric, rounds: 2);

        Interlocked.Exchange(ref node1.ClusterObserver.TriggerCount, 0);
        Interlocked.Exchange(ref node2.ClusterObserver.TriggerCount, 0);
        Interlocked.Exchange(ref node1.OrchestratorObserver.TriggerCount, 0);
        Interlocked.Exchange(ref node2.OrchestratorObserver.TriggerCount, 0);
        fabric.RecordedMessages.Clear();

        // Run 4 steady-state anti-entropy rounds
        await SyncClusterRoundsAsync(fabric, rounds: 4);

        // Assert 2 - Zero observer re-triggers and zero snapshot/patch oscillations
        Volatile.Read(ref node1.ClusterObserver.TriggerCount).ShouldBe(0);
        Volatile.Read(ref node2.ClusterObserver.TriggerCount).ShouldBe(0);
        Volatile.Read(ref node1.OrchestratorObserver.TriggerCount).ShouldBe(0);
        Volatile.Read(ref node2.OrchestratorObserver.TriggerCount).ShouldBe(0);

        var steadyMessages = fabric.RecordedMessages.ToList();
        steadyMessages.ShouldNotBeEmpty();
        steadyMessages.ShouldAllBe(m => m.MessageType == "CrdtSync");
    }

    [IntegrationFact]
    public async Task ShardedCluster_WhenGossipPatchArrivesBeforeStateSync_RegistersPeerBeforeLocalityCheckAndPromotesOnlyOnce()
    {
        // Arrange - Node 1 and Node 2 are connected in IPeerRegistry, but Node 2 has NOT received a CrdtSync from Node 1 yet.
        // Node 1 creates a document and gossips CrdtPatch (with wrapper.SenderReplicaId = "Replica-1").
        // Node 2 must register "Replica-1" in ClusterTracker BEFORE checking IsShardLocalAsync so its consistent hash ring
        // already includes both Replica-1 and Replica-2 on the very first check, avoiding a false promotion + immediate demotion cycle.
        using var fabric = new TopologyTestMeshFabric { DropDirectSyncMessages = true };
        var node1 = await CreateNodeAsync("Replica-1", replicationFactor: 1);
        var node2 = await CreateNodeAsync("Replica-2", replicationFactor: 1);

        // Register peers in IPeerRegistry while direct CrdtSync topology announcements are held back
        await fabric.ConnectTransportOnlyAsync(node1, node2);

        // Find a document ID whose shard is owned by Replica-1 (and NOT local to Replica-2) when both nodes are on the ring
        node1.ClusterTracker.UpdatePeerState("Replica-2", node2.PeerId.Value.ToString(), new ShardVersionVectorMap());
        string? docOwnedByNode1Only = null;
        for (int i = 1; i <= 100; i++)
        {
            var candidateDocId = $"gossip-owner-doc-{i}";
            var targets = await node1.DistributionManager.GetTargetNodesForShardAsync($"shard-{candidateDocId}", CancellationToken.None);
            if (targets.Count == 1 && string.Equals(targets[0], "Replica-1", StringComparison.Ordinal))
            {
                docOwnedByNode1Only = candidateDocId;
                break;
            }
        }

        docOwnedByNode1Only.ShouldNotBeNull();

        // Confirm Node 2's ClusterTracker does not yet know Replica-1's PeerId->ReplicaId mapping
        await fabric.DrainNetworkAsync();

        // Node 2 has not yet received any CrdtSync from Node 1, so ClusterTracker does not know Node 1's ReplicaId yet
        node2.ClusterTracker.GetReplicaIdByPeerId(node1.PeerId.Value.ToString()).ShouldBeNull();

        Interlocked.Exchange(ref node2.OrchestratorObserver.TriggerCount, 0);

        // Act - Node 1 creates a document owned by Node 1; CrdtPatchBroadcaster dispatches CrdtPatch to Node 2
        await node1.Orchestrator.CreateDocumentAsync(docOwnedByNode1Only!, DocTypeAlias, CancellationToken.None);
        await node1.Orchestrator.ApplyIntentAsync<TopologyObserverDocState>(
            docOwnedByNode1Only!,
            new MapSetIntent("Key1", "from-node-1"),
            nameof(TopologyObserverDocState.DataMap),
            IntentDeliveryMode.Acknowledged,
            CancellationToken.None);

        foreach (var patchObserver in node1.Scope.ServiceProvider.GetServices<IDocumentPatchObserver>())
        {
            await patchObserver.FlushAsync(CancellationToken.None);
        }

        await fabric.DrainNetworkAsync();
        await node2.Orchestrator.FlushAsync(CancellationToken.None);

        // Assert - Node 2 registered Replica-1 from the CrdtPatch wrapper BEFORE checking shard locality,
        // so it immediately saw that docOwnedByNode1Only belongs to Replica-1 and never promoted/demoted it.
        node2.ClusterTracker.GetReplicaIdByPeerId(node1.PeerId.Value.ToString()).ShouldBe("Replica-1");
        node2.Orchestrator.GetActiveDocuments().Select(x => x.TypeAlias).ShouldNotContain(CrdtRegistryEntry.GetRegistryKey(DocTypeAlias, docOwnedByNode1Only!));
        Volatile.Read(ref node2.OrchestratorObserver.TriggerCount).ShouldBe(0);
    }

    private async Task<TopologyTestNode> CreateNodeAsync(string replicaId, int replicationFactor)
    {
        var senderMock = new Mock<IDirectMessageSender>();
        var gossipMock = new Mock<IP2pAlgorithm>();
        var peerId = new PeerId(Guid.NewGuid());
        var clusterObserver = new CountingClusterStateObserver();
        var orchestratorObserver = new CountingOrchestratorObserver();

        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            opt.EnableSharding = true;
            opt.ShardReplicationFactor = replicationFactor;
            opt.VirtualNodesPerReplica = 32;
            opt.ActiveSyncEnabled = true;
            opt.ActiveSyncDebounceMilliseconds = 0;
            opt.AntiEntropyInitialDelaySeconds = 3600;
            opt.AntiEntropyIntervalSeconds = 3600;
            opt.AntiEntropyMaxAdaptiveIntervalSeconds = 3600;
            opt.CheckpointIntervalSeconds = 3600;
            opt.MaintenanceIntervalSeconds = 3600;
            opt.PeerEvictionTtlSeconds = 0;
            opt.EnableGracefulShutdown = false;
        });

        services.AddCrdt()
                .AddCrdtAotContext(new ClusterTopologyObserverTestAotContext())
                .AddCrdtJsonTypeInfoResolver(ClusterTopologyObserverTestJsonContext.Default)
                .AddCrdtShardIdProvider<PerDocumentShardIdProvider>();

        services.AddDistributedDocumentType<TopologyObserverDocState>(DocTypeAlias);
        services.AddDistributedCrdtReplica(replicaId, MeshId);

        services.AddScoped<IClusterStateObserver>(_ => clusterObserver);
        services.AddScoped<ICrdtOrchestratorObserver>(_ => orchestratorObserver);

        services.AddSingleton(senderMock.Object);
        services.AddSingleton(gossipMock.Object);
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        services.AddSingleton<IP2pRpcCorrelator, P2pRpcCorrelator>();
        services.AddSingleton(new P2pMeshMetadata(MeshId));

        var sp = services.BuildServiceProvider();
        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(replicaId);
        await scope.Orchestrator.InitializeAsync(CancellationToken.None);

        var node = new TopologyTestNode(
            replicaId,
            peerId,
            sp,
            scope,
            scope.Orchestrator,
            scope.ClusterTracker,
            scope.ServiceProvider.GetRequiredService<IShardDistributionManager>(),
            scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>(),
            sp.GetRequiredService<IPeerRegistry>(),
            sp.GetRequiredKeyedService<IApplicationPayloadHandler>(MeshId),
            senderMock,
            gossipMock,
            clusterObserver,
            orchestratorObserver,
            hostedServices);

        nodes.Add(node);
        return node;
    }

    private static async Task SyncClusterRoundsAsync(TopologyTestMeshFabric fabric, int rounds)
    {
        var activeNodes = fabric.GetActiveNodes();
        for (int r = 0; r < rounds; r++)
        {
            foreach (var source in activeNodes)
            {
                foreach (var target in activeNodes)
                {
                    if (source.PeerId == target.PeerId)
                    {
                        continue;
                    }

                    await source.Orchestrator.DispatchAntiEntropyStateAsync(target.PeerId, CancellationToken.None);
                }
            }

            await fabric.DrainNetworkAsync();

            foreach (var node in activeNodes)
            {
                await node.Orchestrator.FlushAsync(CancellationToken.None);
            }

            await fabric.DrainNetworkAsync();
        }
    }

    public readonly record struct RecordedWireMessageDto(PeerId Sender, PeerId Target, string DocumentId, string MessageType);

    private sealed record TopologyTestNode(
        string ReplicaId,
        PeerId PeerId,
        IServiceProvider ServiceProvider,
        IDistributedCrdtScope Scope,
        ICrdtDocumentOrchestrator Orchestrator,
        IClusterStateTracker ClusterTracker,
        IShardDistributionManager DistributionManager,
        IShardVersionVectorManager VersionVectorManager,
        IPeerRegistry PeerRegistry,
        IApplicationPayloadHandler PayloadHandler,
        Mock<IDirectMessageSender> SenderMock,
        Mock<IP2pAlgorithm> GossipMock,
        CountingClusterStateObserver ClusterObserver,
        CountingOrchestratorObserver OrchestratorObserver,
        IReadOnlyList<IHostedService> HostedServices);

    private sealed class TopologyTestMeshFabric : IDisposable
    {
        private readonly ConcurrentDictionary<PeerId, TopologyTestNode> activeNodes = new();
        private readonly ConcurrentBag<Task> inFlightPackets = new();
        private volatile bool isDisposed;

        public bool DropDirectSyncMessages { get; set; }

        public ConcurrentQueue<RecordedWireMessageDto> RecordedMessages { get; } = new();

        public IReadOnlyList<TopologyTestNode> GetActiveNodes() => activeNodes.Values.ToList();

        public async Task ConnectTransportOnlyAsync(params TopologyTestNode[] nodesToConnect)
        {
            int port = 9100;
            foreach (var node in nodesToConnect)
            {
                WireNodeTransport(node);
                activeNodes[node.PeerId] = node;
            }

            foreach (var nodeA in nodesToConnect)
            {
                foreach (var nodeB in nodesToConnect)
                {
                    if (nodeA.PeerId == nodeB.PeerId)
                    {
                        continue;
                    }

                    var peerNodeB = new PeerNode(nodeB.PeerId, new TcpPeerEndpoint("127.0.0.1", ++port));
                    await nodeA.PeerRegistry.AddOrUpdatePeerAsync(MeshId, peerNodeB, PeerStatus.Active, CancellationToken.None);
                }
            }
        }

        public async Task JoinNodeAsync(TopologyTestNode joiningNode)
        {
            WireNodeTransport(joiningNode);
            activeNodes[joiningNode.PeerId] = joiningNode;

            var allNodes = activeNodes.Values.ToList();
            int port = 9000;

            foreach (var nodeA in allNodes)
            {
                foreach (var nodeB in allNodes)
                {
                    if (nodeA.PeerId == nodeB.PeerId)
                    {
                        continue;
                    }

                    var peerNodeB = new PeerNode(nodeB.PeerId, new TcpPeerEndpoint("127.0.0.1", ++port));
                    await nodeA.PeerRegistry.AddOrUpdatePeerAsync(MeshId, peerNodeB, PeerStatus.Active, CancellationToken.None);
                    nodeA.ClusterTracker.UpdatePeerState(nodeB.ReplicaId, nodeB.PeerId.Value.ToString(), new ShardVersionVectorMap());
                }
            }

            foreach (var node in allNodes)
            {
                await node.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
            }

            await DrainNetworkAsync();
        }

        public async Task DepartNodeAsync(TopologyTestNode departingNode)
        {
            activeNodes.TryRemove(departingNode.PeerId, out _);

            foreach (var node in activeNodes.Values)
            {
                await node.PeerRegistry.RemovePeerAsync(MeshId, departingNode.PeerId, CancellationToken.None);
                node.ClusterTracker.RemovePeerByNetworkId(departingNode.PeerId.Value.ToString());
                await node.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
            }

            await DrainNetworkAsync();
        }

        public async Task DrainNetworkAsync()
        {
            while (inFlightPackets.TryTake(out var task))
            {
                await task.ConfigureAwait(false);
            }
        }

        public void Dispose()
        {
            isDisposed = true;
        }

        private void WireNodeTransport(TopologyTestNode node)
        {
            var serializer = node.ServiceProvider.GetRequiredService<ICrdtSerializer>();

            node.SenderMock
                .Setup(s => s.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns((PeerId targetPeerId, ReadOnlyMemory<byte> payload, CancellationToken _) =>
                {
                    if (isDisposed)
                    {
                        return Task.CompletedTask;
                    }

                    var copy = payload.ToArray();
                    var wrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(copy);
                    if (DropDirectSyncMessages && string.Equals(wrapper.MessageType, "CrdtSync", StringComparison.Ordinal))
                    {
                        return Task.CompletedTask;
                    }

                    RecordedMessages.Enqueue(new RecordedWireMessageDto(node.PeerId, targetPeerId, wrapper.DocumentId, wrapper.MessageType));

                    inFlightPackets.Add(Task.Run(async () =>
                    {
                        if (!isDisposed && activeNodes.TryGetValue(targetPeerId, out var targetNode))
                        {
                            await targetNode.PayloadHandler.HandlePayloadAsync(MeshId, node.PeerId, copy, CancellationToken.None).ConfigureAwait(false);
                        }
                    }));

                    return Task.CompletedTask;
                });

            node.GossipMock
                .Setup(g => g.BroadcastAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns((ReadOnlyMemory<byte> payload, CancellationToken _) =>
                {
                    if (isDisposed)
                    {
                        return Task.CompletedTask;
                    }

                    var copy = payload.ToArray();
                    var wrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(copy);
                    var recipients = activeNodes.Values.Where(other => other.PeerId != node.PeerId).ToList();

                    foreach (var recipient in recipients)
                    {
                        RecordedMessages.Enqueue(new RecordedWireMessageDto(node.PeerId, recipient.PeerId, wrapper.DocumentId, wrapper.MessageType));
                    }

                    inFlightPackets.Add(Task.Run(async () =>
                    {
                        var tasks = recipients.Select(r =>
                            r.PayloadHandler.HandlePayloadAsync(MeshId, node.PeerId, copy, CancellationToken.None));
                        await Task.WhenAll(tasks).ConfigureAwait(false);
                    }));

                    return Task.CompletedTask;
                });
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var node in nodes)
        {
            foreach (var hs in node.HostedServices)
            {
                await hs.StopAsync(CancellationToken.None);
            }

            if (node.ServiceProvider is IAsyncDisposable asyncSp)
            {
                await asyncSp.DisposeAsync();
            }
            else if (node.ServiceProvider is IDisposable dispSp)
            {
                dispSp.Dispose();
            }
        }

        nodes.Clear();
    }
}