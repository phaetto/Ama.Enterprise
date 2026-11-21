namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

[CrdtAotType(typeof(DynamicShardingTopologyIntegrationTests.ShardedDocState))]
[CrdtAotType(typeof(Dictionary<string, string>))]
public sealed partial class DynamicShardingTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(DynamicShardingTopologyIntegrationTests.ShardedDocState))]
[JsonSerializable(typeof(CrdtDocument<DynamicShardingTopologyIntegrationTests.ShardedDocState>))]
public sealed partial class DynamicShardingTestJsonContext : JsonSerializerContext
{
}

/// <summary>
/// Integration tests verifying zero-downtime sharding scenarios while the P2P network topology
/// is actively changing (nodes joining, departing, failing over, and flapping under continuous load).
/// </summary>
public sealed class DynamicShardingTopologyIntegrationTests : IAsyncDisposable
{
    private const string MeshId = "DynamicShardMesh";
    private const string DocTypeAlias = "sharded-doc";

    public sealed class ShardedDocState : IEquatable<ShardedDocState>
    {
        public string Id { get; set; } = "sharded-doc";

        public Dictionary<string, string> DataMap { get; set; } = new(StringComparer.Ordinal);

        public bool Equals(ShardedDocState? other)
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
                if (!other.DataMap.TryGetValue(kvp.Key, out var val) || !string.Equals(kvp.Value, val, StringComparison.Ordinal))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj) => Equals(obj as ShardedDocState);

        public override int GetHashCode() => HashCode.Combine(Id, DataMap.Count);
    }

    private sealed class DocumentIdShardProvider : IShardIdProvider
    {
        public string GetShardId<T>(CrdtDocument<T> document) where T : class
        {
            if (document.Data is ShardedDocState state && !string.IsNullOrWhiteSpace(state.Id))
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

    private readonly List<ShardedTestNode> nodes = new();

    [IntegrationFact]
    public async Task ClusterScaleOut_UnderContinuousReadsAndWrites_MaintainsZeroDowntimeAndZeroDataLoss()
    {
        // Arrange - Start with a 2-node cluster (ReplicationFactor = 2) and scale out to 4 nodes mid-flight
        using var fabric = new DynamicMeshFabric();
        var node1 = await CreateAndStartNodeAsync("Replica-1", replicationFactor: 2);
        var node2 = await CreateAndStartNodeAsync("Replica-2", replicationFactor: 2);

        await fabric.JoinNodeAsync(node1);
        await fabric.JoinNodeAsync(node2);

        const int DocumentCount = 12;
        var docIds = Enumerable.Range(1, DocumentCount).Select(i => $"scaleout-doc-{i}").ToArray();

        for (int i = 0; i < docIds.Length; i++)
        {
            var entryNode = i % 2 == 0 ? node1 : node2;
            await entryNode.Orchestrator.CreateDocumentAsync(docIds[i], DocTypeAlias, CancellationToken.None);
        }

        await SyncAllNodesAsync(fabric);

        var expectedWrites = new ConcurrentDictionary<string, ConcurrentDictionary<string, string>>(StringComparer.Ordinal);
        foreach (var docId in docIds)
        {
            expectedWrites[docId] = new ConcurrentDictionary<string, string>(StringComparer.Ordinal);
        }

        var failedWrites = 0;
        var failedReads = 0;
        var totalWrites = 0;
        var totalReads = 0;

        using var workloadCts = new CancellationTokenSource();

        // Start continuous concurrent writers and readers hitting random active nodes
        var writerTasks = Enumerable.Range(0, 4).Select(workerId => Task.Run(async () =>
        {
            int seq = 0;
            while (!workloadCts.Token.IsCancellationRequested)
            {
                var currentNodes = fabric.GetActiveNodes();
                var targetNode = currentNodes[(workerId + seq) % currentNodes.Count];
                var docId = docIds[(workerId + seq) % docIds.Length];
                var key = $"w{workerId}-k{seq}";
                var val = $"val-{workerId}-{seq}";
                seq++;

                var applied = await targetNode.Orchestrator.ApplyIntentAsync<ShardedDocState>(
                    docId,
                    new MapSetIntent(key, val),
                    nameof(ShardedDocState.DataMap),
                    IntentDeliveryMode.Acknowledged,
                    CancellationToken.None);

                if (applied)
                {
                    expectedWrites[docId][key] = val;
                    Interlocked.Increment(ref totalWrites);
                }
                else
                {
                    Interlocked.Increment(ref failedWrites);
                }

                await Task.Delay(5);
            }
        })).ToArray();

        var readerTasks = Enumerable.Range(0, 2).Select(readerId => Task.Run(async () =>
        {
            int seq = 0;
            while (!workloadCts.Token.IsCancellationRequested)
            {
                var currentNodes = fabric.GetActiveNodes();
                var targetNode = currentNodes[(readerId + seq) % currentNodes.Count];
                var docId = docIds[(readerId + seq) % docIds.Length];
                seq++;

                try
                {
                    var doc = await targetNode.Orchestrator.GetDocumentStateAsync<ShardedDocState>(docId, CancellationToken.None);
                    if (doc.Data is not null)
                    {
                        Interlocked.Increment(ref totalReads);
                    }
                    else
                    {
                        Interlocked.Increment(ref failedReads);
                    }
                }
                catch
                {
                    Interlocked.Increment(ref failedReads);
                }

                await Task.Delay(8);
            }
        })).ToArray();

        // Let baseline load run on the 2-node cluster
        await Task.Delay(120);

        // Act - Dynamically add Replica-3 and Replica-4 while writes and reads are actively in-flight
        var node3 = await CreateAndStartNodeAsync("Replica-3", replicationFactor: 2);
        await fabric.JoinNodeAsync(node3);
        await SyncAllNodesAsync(fabric);

        await Task.Delay(120);

        var node4 = await CreateAndStartNodeAsync("Replica-4", replicationFactor: 2);
        await fabric.JoinNodeAsync(node4);
        await SyncAllNodesAsync(fabric);

        await Task.Delay(150);

        await workloadCts.CancelAsync();
        await Task.WhenAll(writerTasks);
        await Task.WhenAll(readerTasks);

        // Final anti-entropy convergence across the expanded 4-node ring
        await SyncAllNodesAsync(fabric);
        await SyncAllNodesAsync(fabric);

        // Assert - Zero failed writes, zero failed reads, and every acknowledged write is readable from any node in the cluster
        totalWrites.ShouldBeGreaterThan(20);
        totalReads.ShouldBeGreaterThan(10);
        failedWrites.ShouldBe(0);
        failedReads.ShouldBe(0);

        var allNodes = fabric.GetActiveNodes();
        allNodes.Count.ShouldBe(4);

        foreach (var docId in docIds)
        {
            var expectedDocEntries = expectedWrites[docId];
            foreach (var node in allNodes)
            {
                var docState = await node.Orchestrator.GetDocumentStateAsync<ShardedDocState>(docId, CancellationToken.None);
                docState.Data.ShouldNotBeNull();
                foreach (var kvp in expectedDocEntries)
                {
                    docState.Data.DataMap.ShouldContainKeyAndValue(kvp.Key, kvp.Value);
                }
            }
        }
    }

    [IntegrationFact]
    public async Task SuddenNodeCrash_UnderContinuousLoad_FailsOverImmediatelyViaRpcAndRebalancesWithZeroDowntime()
    {
        // Arrange - 4-node cluster with ReplicationFactor = 2.
        // We will abruptly sever Replica-4 mid-flight BEFORE the peer registry even detects its departure
        // (testing RPC retry failover), and then remove it from the topology (testing ring rebalancing).
        using var fabric = new DynamicMeshFabric();
        var node1 = await CreateAndStartNodeAsync("Replica-1", replicationFactor: 2);
        var node2 = await CreateAndStartNodeAsync("Replica-2", replicationFactor: 2);
        var node3 = await CreateAndStartNodeAsync("Replica-3", replicationFactor: 2);
        var node4 = await CreateAndStartNodeAsync("Replica-4", replicationFactor: 2);

        await fabric.JoinNodeAsync(node1);
        await fabric.JoinNodeAsync(node2);
        await fabric.JoinNodeAsync(node3);
        await fabric.JoinNodeAsync(node4);

        const int DocumentCount = 10;
        var docIds = Enumerable.Range(1, DocumentCount).Select(i => $"crash-doc-{i}").ToArray();

        for (int i = 0; i < docIds.Length; i++)
        {
            await node1.Orchestrator.CreateDocumentAsync(docIds[i], DocTypeAlias, CancellationToken.None);
        }

        await SyncAllNodesAsync(fabric);

        var survivingNodes = new[] { node1, node2, node3 };
        var expectedWrites = new ConcurrentDictionary<string, ConcurrentDictionary<string, string>>(StringComparer.Ordinal);
        foreach (var docId in docIds)
        {
            expectedWrites[docId] = new ConcurrentDictionary<string, string>(StringComparer.Ordinal);
        }

        var failedWrites = 0;
        var failedReads = 0;
        var totalWrites = 0;

        using var workloadCts = new CancellationTokenSource();

        var writerTasks = Enumerable.Range(0, 3).Select(workerId => Task.Run(async () =>
        {
            int seq = 0;
            while (!workloadCts.Token.IsCancellationRequested)
            {
                var entryNode = survivingNodes[(workerId + seq) % survivingNodes.Length];
                var docId = docIds[(workerId + seq) % docIds.Length];
                var key = $"surv-{workerId}-{seq}";
                var val = $"v-{workerId}-{seq}";
                seq++;

                var applied = await entryNode.Orchestrator.ApplyIntentAsync<ShardedDocState>(
                    docId,
                    new MapSetIntent(key, val),
                    nameof(ShardedDocState.DataMap),
                    IntentDeliveryMode.Acknowledged,
                    CancellationToken.None);

                if (applied)
                {
                    expectedWrites[docId][key] = val;
                    Interlocked.Increment(ref totalWrites);
                }
                else
                {
                    Interlocked.Increment(ref failedWrites);
                }

                await Task.Delay(5);
            }
        })).ToArray();

        var readerTask = Task.Run(async () =>
        {
            int seq = 0;
            while (!workloadCts.Token.IsCancellationRequested)
            {
                var entryNode = survivingNodes[seq % survivingNodes.Length];
                var docId = docIds[seq % docIds.Length];
                seq++;

                try
                {
                    var state = await entryNode.Orchestrator.GetDocumentStateAsync<ShardedDocState>(docId, CancellationToken.None);
                    if (state.Data is null)
                    {
                        Interlocked.Increment(ref failedReads);
                    }
                }
                catch
                {
                    Interlocked.Increment(ref failedReads);
                }

                await Task.Delay(8);
            }
        });

        await Task.Delay(100);

        // Act Phase 1: Abruptly blackhole Replica-4's network while it is still listed as Active in IPeerRegistry.
        // Requests targeting shards where Replica-4 was primary must automatically fail over to the secondary replica.
        fabric.IsolateNodeNetwork(node4);
        await Task.Delay(150);

        // Act Phase 2: Failure detector marks Replica-4 departed so the consistent hash ring rebalances to [Replica-1, Replica-2, Replica-3].
        await fabric.DepartNodeAsync(node4);
        await SyncAllNodesAsync(fabric);
        await Task.Delay(120);

        await workloadCts.CancelAsync();
        await Task.WhenAll(writerTasks);
        await readerTask;

        await SyncAllNodesAsync(fabric);
        await SyncAllNodesAsync(fabric);

        // Assert - Zero downtime during both the unannounced crash window and the ring rebalancing window
        totalWrites.ShouldBeGreaterThan(20);
        failedWrites.ShouldBe(0);
        failedReads.ShouldBe(0);

        foreach (var docId in docIds)
        {
            var expectedDocEntries = expectedWrites[docId];
            foreach (var survivor in survivingNodes)
            {
                var docState = await survivor.Orchestrator.GetDocumentStateAsync<ShardedDocState>(docId, CancellationToken.None);
                docState.Data.ShouldNotBeNull();
                foreach (var kvp in expectedDocEntries)
                {
                    docState.Data.DataMap.ShouldContainKeyAndValue(kvp.Key, kvp.Value);
                }
            }
        }
    }

    [IntegrationFact]
    public async Task RollingTopologyChurn_NodeFlappingAndOwnershipHandover_PreservesCheckpointAndJournalState()
    {
        // Arrange - Verify that when a shard is demoted on a node (due to scale-out) and later re-promoted
        // on that same node (due to another node departing), its prior state was checkpointed on demotion
        // and merges cleanly with any new mutations that happened while it was away.
        using var fabric = new DynamicMeshFabric();
        var node1 = await CreateAndStartNodeAsync("Replica-1", replicationFactor: 1);
        var node2 = await CreateAndStartNodeAsync("Replica-2", replicationFactor: 1);

        await fabric.JoinNodeAsync(node1);

        // Create 8 documents while Replica-1 is the sole node in the cluster (ReplicationFactor = 1 -> Replica-1 owns 100% of shards)
        var docIds = Enumerable.Range(1, 8).Select(i => $"handover-doc-{i}").ToArray();
        foreach (var docId in docIds)
        {
            await node1.Orchestrator.CreateDocumentAsync(docId, DocTypeAlias, CancellationToken.None);
            await node1.Orchestrator.ApplyIntentAsync<ShardedDocState>(
                docId,
                new MapSetIntent("Phase1", $"FromNode1-{docId}"),
                nameof(ShardedDocState.DataMap),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
        }

        // Act 1: Replica-2 joins. Because ReplicationFactor = 1, a subset of shards will be demoted on Replica-1 and promoted on Replica-2.
        await fabric.JoinNodeAsync(node2);
        await SyncAllNodesAsync(fabric);
        await SyncAllNodesAsync(fabric);

        // Find at least one document that moved to Replica-2 (demoted on Replica-1)
        var movedDocIds = new List<string>();
        foreach (var docId in docIds)
        {
            var shardId = $"shard-{docId}";
            var isLocalOn1 = await node1.DistributionManager.IsShardLocalAsync(shardId, CancellationToken.None);
            var isLocalOn2 = await node2.DistributionManager.IsShardLocalAsync(shardId, CancellationToken.None);

            if (!isLocalOn1 && isLocalOn2)
            {
                movedDocIds.Add(docId);
            }
        }

        movedDocIds.ShouldNotBeEmpty();

        // Verify Replica-1 demoted the moved documents from active memory, but saved their checkpoints to storage before demoting
        foreach (var movedDocId in movedDocIds)
        {
            node1.Orchestrator.TryGetLocalDocument<ShardedDocState>(movedDocId).ShouldBeNull();

            // Mutate the document via Replica-1 (which proxies via RPC to Replica-2) and directly via Replica-2
            var appliedViaProxy = await node1.Orchestrator.ApplyIntentAsync<ShardedDocState>(
                movedDocId,
                new MapSetIntent("Phase2_Proxy", $"ViaProxy-{movedDocId}"),
                nameof(ShardedDocState.DataMap),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
            appliedViaProxy.ShouldBeTrue();
        }

        // Sync before Replica-2 gracefully hands back ownership by syncing and departing
        await SyncAllNodesAsync(fabric);

        // Act 2: Replica-2 departs. Replica-1 re-promotes the shards back into active memory and loads its checkpointed state.
        await fabric.DepartNodeAsync(node2);
        await node1.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);

        // Apply a Phase 3 mutation on Replica-1 after re-promotion
        foreach (var movedDocId in movedDocIds)
        {
            node1.Orchestrator.TryGetLocalDocument<ShardedDocState>(movedDocId).ShouldNotBeNull();

            var appliedPhase3 = await node1.Orchestrator.ApplyIntentAsync<ShardedDocState>(
                movedDocId,
                new MapSetIntent("Phase3_RePromoted", $"BackOnNode1-{movedDocId}"),
                nameof(ShardedDocState.DataMap),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
            appliedPhase3.ShouldBeTrue();

            // Assert - Phase 1 (pre-demotion checkpoint on Node 1) and Phase 3 (post-re-promotion on Node 1) are both preserved
            var finalDoc = await node1.Orchestrator.GetDocumentStateAsync<ShardedDocState>(movedDocId, CancellationToken.None);
            finalDoc.Data.DataMap.ShouldContainKeyAndValue("Phase1", $"FromNode1-{movedDocId}");
            finalDoc.Data.DataMap.ShouldContainKeyAndValue("Phase3_RePromoted", $"BackOnNode1-{movedDocId}");
        }

        // Act 3: Replica-2 rejoins the cluster and syncs with Replica-1 -> Phase 1, Phase 2, and Phase 3 all union without data loss
        await fabric.JoinNodeAsync(node2);
        await SyncAllNodesAsync(fabric);
        await SyncAllNodesAsync(fabric);

        foreach (var movedDocId in movedDocIds)
        {
            var stateFrom1 = await node1.Orchestrator.GetDocumentStateAsync<ShardedDocState>(movedDocId, CancellationToken.None);
            var stateFrom2 = await node2.Orchestrator.GetDocumentStateAsync<ShardedDocState>(movedDocId, CancellationToken.None);

            foreach (var state in new[] { stateFrom1, stateFrom2 })
            {
                state.Data.DataMap.ShouldContainKeyAndValue("Phase1", $"FromNode1-{movedDocId}");
                state.Data.DataMap.ShouldContainKeyAndValue("Phase2_Proxy", $"ViaProxy-{movedDocId}");
                state.Data.DataMap.ShouldContainKeyAndValue("Phase3_RePromoted", $"BackOnNode1-{movedDocId}");
            }
        }
    }

    [IntegrationFact]
    public async Task ShardHandover_WhenNewNodeJoinsAndReturningNodeRePromotes_HydratesLatestVersionImmediatelyWithoutAntiEntropy()
    {
        // Arrange - ReplicationFactor = 1 so shard ownership moves completely between nodes.
        // Verify that when a new node joins or an old node re-promotes a shard that was mutated elsewhere,
        // round-robin reads hitting any node immediately return the latest version BEFORE any background anti-entropy runs.
        using var fabric = new DynamicMeshFabric();
        var node1 = await CreateAndStartNodeAsync("Replica-1", replicationFactor: 1);
        var node2 = await CreateAndStartNodeAsync("Replica-2", replicationFactor: 1);

        await fabric.JoinNodeAsync(node1);

        var docIds = Enumerable.Range(1, 10).Select(i => $"immediate-hydrate-doc-{i}").ToArray();
        foreach (var docId in docIds)
        {
            await node1.Orchestrator.CreateDocumentAsync(docId, DocTypeAlias, CancellationToken.None);
            await node1.Orchestrator.ApplyIntentAsync<ShardedDocState>(
                docId,
                new MapSetIntent("v1", $"value1-{docId}"),
                nameof(ShardedDocState.DataMap),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
        }

        // Act 1: Node 2 joins. Sync ONLY the registry so Node 2 knows the documents exist,
        // then trigger topology re-evaluation on Node 2 WITHOUT running anti-entropy on the document shards.
        await fabric.JoinNodeAsync(node2);
        await node1.Orchestrator.ProvideSnapshotAsync(
            node1.Orchestrator.Registry.DocumentId,
            node1.Orchestrator.Registry.TypeAlias,
            node2.ReplicaId,
            node2.PeerId,
            CancellationToken.None);
        await fabric.DrainNetworkAsync();

        await node2.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
        await node1.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
        await fabric.DrainNetworkAsync();

        var movedDocIds = new List<string>();
        foreach (var docId in docIds)
        {
            var shardId = $"shard-{docId}";
            if (await node2.DistributionManager.IsShardLocalAsync(shardId, CancellationToken.None))
            {
                movedDocIds.Add(docId);
            }
        }

        movedDocIds.ShouldNotBeEmpty();

        // Assert 1: Immediately reading from Node 2 or Node 1 returns v1 (no V0 blank state bounce!)
        foreach (var movedDocId in movedDocIds)
        {
            var readFromNode2 = await node2.Orchestrator.GetDocumentStateAsync<ShardedDocState>(movedDocId, CancellationToken.None);
            readFromNode2.Data.DataMap.ShouldContainKeyAndValue("v1", $"value1-{movedDocId}");

            var readFromNode1 = await node1.Orchestrator.GetDocumentStateAsync<ShardedDocState>(movedDocId, CancellationToken.None);
            readFromNode1.Data.DataMap.ShouldContainKeyAndValue("v1", $"value1-{movedDocId}");

            // Advance the document to v2 on Node 2 while Node 1 does not own the shard
            var appliedV2 = await node2.Orchestrator.ApplyIntentAsync<ShardedDocState>(
                movedDocId,
                new MapSetIntent("v2", $"value2-{movedDocId}"),
                nameof(ShardedDocState.DataMap),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
            appliedV2.ShouldBeTrue();
        }

        // Act 2: Node 1 re-promotes the shard while Node 1's local storage still has stale checkpoint v1.
        // Before Node 2's network is disconnected, Node 1 re-evaluates or promotes and hydrates v2 from Node 2 (or Node 2 pushes on demotion).
        var node3 = await CreateAndStartNodeAsync("Replica-3", replicationFactor: 1);
        await fabric.JoinNodeAsync(node3);
        await node1.Orchestrator.ProvideSnapshotAsync(
            node1.Orchestrator.Registry.DocumentId,
            node1.Orchestrator.Registry.TypeAlias,
            node3.ReplicaId,
            node3.PeerId,
            CancellationToken.None);
        await fabric.DrainNetworkAsync();

        await node1.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
        await node2.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
        await node3.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
        await fabric.DrainNetworkAsync();

        // Assert 2: Every active node in the 3-node cluster immediately returns both v1 and v2 on round-robin reads
        foreach (var movedDocId in movedDocIds)
        {
            foreach (var activeNode in fabric.GetActiveNodes())
            {
                var docState = await activeNode.Orchestrator.GetDocumentStateAsync<ShardedDocState>(movedDocId, CancellationToken.None);
                docState.Data.DataMap.ShouldContainKeyAndValue("v1", $"value1-{movedDocId}");
                docState.Data.DataMap.ShouldContainKeyAndValue("v2", $"value2-{movedDocId}");
            }
        }
    }

    [IntegrationFact]
    public async Task CreateDocumentAndApplyIntent_WithLongRpcTimeoutAndMultipleReplicas_CompletesImmediatelyWithoutCircularHydrationDeadlock()
    {
        // Arrange - 2-node cluster with ReplicationFactor = 2 (both nodes own every shard)
        // and a 5-second RpcTimeoutMilliseconds.
        // Regression test ensuring that creating a brand-new document does NOT trigger a circular
        // CrdtReadReq -> ResolveOrPromoteLocalDocumentAsync -> HydratePromotedDocumentFromPeersAsync -> CrdtReadReq
        // 5-second RPC timeout deadlock, nor falsely promote uncreated documents on remote peers on read probes.
        using var fabric = new DynamicMeshFabric();
        var node1 = await CreateAndStartNodeAsync("Replica-1", replicationFactor: 2, rpcTimeoutMilliseconds: 5000);
        var node2 = await CreateAndStartNodeAsync("Replica-2", replicationFactor: 2, rpcTimeoutMilliseconds: 5000);

        await fabric.JoinNodeAsync(node1);
        await fabric.JoinNodeAsync(node2);

        // Drop gossip broadcasts temporarily to simulate the real-world race where the direct
        // CrdtReadReq hydration probe arrives at Replica-2 BEFORE the registry creation gossip packet.
        fabric.IsolateNodeGossip(node1);

        const string newDocId = "instant-feature-flag-doc";
        var sw = Stopwatch.StartNew();

        // Act - Create the document and immediately apply an intent on Replica-1
        await node1.Orchestrator.CreateDocumentAsync(newDocId, DocTypeAlias, CancellationToken.None);
        var applied = await node1.Orchestrator.ApplyIntentAsync<ShardedDocState>(
            newDocId,
            new MapSetIntent("flag-1", "enabled"),
            nameof(ShardedDocState.DataMap),
            IntentDeliveryMode.Acknowledged,
            CancellationToken.None);

        sw.Stop();

        // Assert - Must complete well under the 5000ms RPC timeout (no circular hydration deadlock)
        applied.ShouldBeTrue();
        sw.ElapsedMilliseconds.ShouldBeLessThan(1500);

        // And because the registry gossip was delayed and Replica-2 has no stored checkpoint for newDocId,
        // Replica-1's hydration probe must NOT have falsely promoted an empty document on Replica-2.
        node2.Orchestrator.TryGetLocalDocument<ShardedDocState>(newDocId).ShouldBeNull();
    }

    [IntegrationFact]
    public async Task ConcurrentDocumentCreation_OnMultipleReplicasWithLongRpcTimeout_CompletesImmediatelyWithoutDeadlock()
    {
        // Arrange - Both Replica-1 and Replica-2 simultaneously create and mutate the exact same new document
        // while a 5-second RPC timeout is configured.
        using var fabric = new DynamicMeshFabric();
        var node1 = await CreateAndStartNodeAsync("Replica-1", replicationFactor: 2, rpcTimeoutMilliseconds: 5000);
        var node2 = await CreateAndStartNodeAsync("Replica-2", replicationFactor: 2, rpcTimeoutMilliseconds: 5000);

        await fabric.JoinNodeAsync(node1);
        await fabric.JoinNodeAsync(node2);

        const string concurrentDocId = "concurrent-creation-doc";
        var sw = Stopwatch.StartNew();

        // Act
        var task1 = Task.Run(async () =>
        {
            await node1.Orchestrator.CreateDocumentAsync(concurrentDocId, DocTypeAlias, CancellationToken.None);
            return await node1.Orchestrator.ApplyIntentAsync<ShardedDocState>(
                concurrentDocId,
                new MapSetIntent("from-node-1", "v1"),
                nameof(ShardedDocState.DataMap),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
        });

        var task2 = Task.Run(async () =>
        {
            await node2.Orchestrator.CreateDocumentAsync(concurrentDocId, DocTypeAlias, CancellationToken.None);
            return await node2.Orchestrator.ApplyIntentAsync<ShardedDocState>(
                concurrentDocId,
                new MapSetIntent("from-node-2", "v2"),
                nameof(ShardedDocState.DataMap),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
        });

        var results = await Task.WhenAll(task1, task2);
        sw.Stop();

        // Assert - Neither node blocks on the 5000ms RPC timeout
        results[0].ShouldBeTrue();
        results[1].ShouldBeTrue();
        sw.ElapsedMilliseconds.ShouldBeLessThan(1500);

        await SyncAllNodesAsync(fabric);

        var state1 = await node1.Orchestrator.GetDocumentStateAsync<ShardedDocState>(concurrentDocId, CancellationToken.None);
        var state2 = await node2.Orchestrator.GetDocumentStateAsync<ShardedDocState>(concurrentDocId, CancellationToken.None);

        state1.Data.DataMap.ShouldContainKeyAndValue("from-node-1", "v1");
        state1.Data.DataMap.ShouldContainKeyAndValue("from-node-2", "v2");
        state2.Data.DataMap.ShouldContainKeyAndValue("from-node-1", "v1");
        state2.Data.DataMap.ShouldContainKeyAndValue("from-node-2", "v2");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var node in nodes)
        {
            foreach (var hs in node.HostedServices)
            {
                await hs.StopAsync(CancellationToken.None);
            }

            if (node.ServiceProvider is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync();
            }
            else if (node.ServiceProvider is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
    }

    private async Task<ShardedTestNode> CreateAndStartNodeAsync(string replicaId, int replicationFactor, int rpcTimeoutMilliseconds = 150)
    {
        var senderMock = new Mock<IDirectMessageSender>();
        var gossipMock = new Mock<IP2pAlgorithm>();
        var peerId = new PeerId(Guid.NewGuid());

        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            opt.EnableSharding = true;
            opt.ShardReplicationFactor = replicationFactor;
            opt.VirtualNodesPerReplica = 64;
            opt.ActiveSyncEnabled = true;
            opt.ActiveSyncDebounceMilliseconds = 0;
            opt.AntiEntropyInitialDelaySeconds = 3600;
            opt.AntiEntropyIntervalSeconds = 3600;
            opt.CheckpointIntervalSeconds = 3600;
            opt.MaintenanceIntervalSeconds = 3600;
            opt.PeerEvictionTtlSeconds = 0;
            opt.RpcTimeoutMilliseconds = rpcTimeoutMilliseconds;
            opt.RpcMaxRetries = 3;
            opt.RpcRetryDelayMilliseconds = 15;
            opt.EnableGracefulShutdown = false;
        });

        services.AddCrdt()
                .AddCrdtAotContext(new DynamicShardingTestAotContext())
                .AddCrdtJsonTypeInfoResolver(DynamicShardingTestJsonContext.Default)
                .AddCrdtShardIdProvider<DocumentIdShardProvider>();

        services.AddDistributedDocumentType<ShardedDocState>(DocTypeAlias);
        services.AddDistributedCrdtReplica(replicaId, MeshId);

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

        var node = new ShardedTestNode(
            replicaId,
            peerId,
            sp,
            scope,
            scope.Orchestrator,
            scope.ClusterTracker,
            scope.ServiceProvider.GetRequiredService<IShardDistributionManager>(),
            sp.GetRequiredService<IPeerRegistry>(),
            sp.GetRequiredKeyedService<IApplicationPayloadHandler>(MeshId),
            senderMock,
            gossipMock,
            hostedServices);

        nodes.Add(node);
        return node;
    }

    private static async Task SyncAllNodesAsync(DynamicMeshFabric fabric)
    {
        var currentNodes = fabric.GetActiveNodes();
        for (int round = 0; round < 2; round++)
        {
            foreach (var source in currentNodes)
            {
                foreach (var target in currentNodes)
                {
                    if (source.PeerId == target.PeerId)
                    {
                        continue;
                    }

                    await source.Orchestrator.DispatchAntiEntropyStateAsync(target.PeerId, CancellationToken.None);
                }
            }

            await fabric.DrainNetworkAsync();

            foreach (var node in currentNodes)
            {
                await node.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
            }

            await fabric.DrainNetworkAsync();
        }
    }

    private sealed record ShardedTestNode(
        string ReplicaId,
        PeerId PeerId,
        IServiceProvider ServiceProvider,
        IDistributedCrdtScope Scope,
        ICrdtDocumentOrchestrator Orchestrator,
        IClusterStateTracker ClusterTracker,
        IShardDistributionManager DistributionManager,
        IPeerRegistry PeerRegistry,
        IApplicationPayloadHandler PayloadHandler,
        Mock<IDirectMessageSender> SenderMock,
        Mock<IP2pAlgorithm> GossipMock,
        IReadOnlyList<IHostedService> HostedServices);

    private sealed class DynamicMeshFabric : IDisposable
    {
        private readonly ConcurrentDictionary<PeerId, ShardedTestNode> activeNodes = new();
        private readonly ConcurrentDictionary<PeerId, byte> networkIsolatedPeers = new();
        private readonly ConcurrentDictionary<PeerId, byte> gossipIsolatedPeers = new();
        private readonly ConcurrentBag<Task> inFlightPackets = new();
        private volatile bool isDisposed;

        public IReadOnlyList<ShardedTestNode> GetActiveNodes() => activeNodes.Values.ToList();

        public async Task JoinNodeAsync(ShardedTestNode joiningNode)
        {
            ArgumentNullException.ThrowIfNull(joiningNode);

            networkIsolatedPeers.TryRemove(joiningNode.PeerId, out _);
            WireNodeTransport(joiningNode);
            activeNodes[joiningNode.PeerId] = joiningNode;

            var allNodes = activeNodes.Values.ToList();
            int port = 6000;

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
        }

        public void IsolateNodeNetwork(ShardedTestNode node)
        {
            ArgumentNullException.ThrowIfNull(node);
            networkIsolatedPeers[node.PeerId] = 1;
        }

        public void IsolateNodeGossip(ShardedTestNode node)
        {
            ArgumentNullException.ThrowIfNull(node);
            gossipIsolatedPeers[node.PeerId] = 1;
        }

        public async Task DepartNodeAsync(ShardedTestNode departingNode)
        {
            ArgumentNullException.ThrowIfNull(departingNode);

            networkIsolatedPeers[departingNode.PeerId] = 1;
            activeNodes.TryRemove(departingNode.PeerId, out _);

            var remainingNodes = activeNodes.Values.ToList();
            foreach (var node in remainingNodes)
            {
                await node.PeerRegistry.RemovePeerAsync(MeshId, departingNode.PeerId, CancellationToken.None);
                node.ClusterTracker.RemovePeerByNetworkId(departingNode.PeerId.Value.ToString());
                await node.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
            }
        }

        public async Task DrainNetworkAsync()
        {
            while (inFlightPackets.TryTake(out var packetTask))
            {
                await packetTask.ConfigureAwait(false);
            }
        }

        public void Dispose()
        {
            isDisposed = true;
        }

        private void WireNodeTransport(ShardedTestNode node)
        {
            node.SenderMock
                .Setup(s => s.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns((PeerId targetPeerId, ReadOnlyMemory<byte> payload, CancellationToken _) =>
                {
                    if (isDisposed ||
                        networkIsolatedPeers.ContainsKey(node.PeerId) ||
                        networkIsolatedPeers.ContainsKey(targetPeerId))
                    {
                        return Task.CompletedTask;
                    }

                    var copy = payload.ToArray();
                    TrackPacket(async () =>
                    {
                        if (activeNodes.TryGetValue(targetPeerId, out var targetNode) &&
                            !networkIsolatedPeers.ContainsKey(targetPeerId))
                        {
                            await targetNode.PayloadHandler.HandlePayloadAsync(MeshId, node.PeerId, copy, CancellationToken.None).ConfigureAwait(false);
                        }
                    });

                    return Task.CompletedTask;
                });

            node.GossipMock
                .Setup(g => g.BroadcastAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns((ReadOnlyMemory<byte> payload, CancellationToken _) =>
                {
                    if (isDisposed ||
                        networkIsolatedPeers.ContainsKey(node.PeerId) ||
                        gossipIsolatedPeers.ContainsKey(node.PeerId))
                    {
                        return Task.CompletedTask;
                    }

                    var copy = payload.ToArray();
                    var recipients = activeNodes.Values
                        .Where(other => other.PeerId != node.PeerId && !networkIsolatedPeers.ContainsKey(other.PeerId))
                        .ToList();

                    TrackPacket(async () =>
                    {
                        var tasks = recipients.Select(r =>
                            r.PayloadHandler.HandlePayloadAsync(MeshId, node.PeerId, copy, CancellationToken.None));
                        await Task.WhenAll(tasks).ConfigureAwait(false);
                    });

                    return Task.CompletedTask;
                });
        }

        private void TrackPacket(Func<Task> packetWork)
        {
            if (isDisposed)
            {
                return;
            }

            var task = Task.Run(async () =>
            {
                if (isDisposed)
                {
                    return;
                }

                try
                {
                    await packetWork().ConfigureAwait(false);
                }
                catch
                {
                }
            });

            inFlightPackets.Add(task);
        }
    }
}