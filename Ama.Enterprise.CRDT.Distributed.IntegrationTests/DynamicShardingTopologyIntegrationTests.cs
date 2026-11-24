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

    private sealed class LegacyBucketShardProvider : IShardIdProvider
    {
        public string GetShardId<T>(CrdtDocument<T> document) where T : class
        {
            if (document.Data is ShardedDocState state && !string.IsNullOrWhiteSpace(state.Id))
            {
                return $"legacy-bucket-{Math.Abs(state.Id.GetHashCode(StringComparison.Ordinal)) % 2}";
            }

            return "legacy-default";
        }

        public string GetShardId<T>(IComparable documentId) where T : class
        {
            ArgumentNullException.ThrowIfNull(documentId);
            var idStr = documentId.ToString() ?? string.Empty;
            return $"legacy-bucket-{Math.Abs(idStr.GetHashCode(StringComparison.Ordinal)) % 2}";
        }

        public string GetShardId(IComparable documentId, Type documentType)
        {
            ArgumentNullException.ThrowIfNull(documentId);
            ArgumentNullException.ThrowIfNull(documentType);
            var idStr = documentId.ToString() ?? string.Empty;
            return $"legacy-bucket-{Math.Abs(idStr.GetHashCode(StringComparison.Ordinal)) % 2}";
        }
    }

    private sealed class SharedPersistentCrdtStorage : IDistributedCrdtStorage
    {
        private readonly MemoryCrdtStorage inner;

        public SharedPersistentCrdtStorage(MemoryCrdtStorage inner)
        {
            this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public Task<ShardVersionVectorMap?> LoadShardVersionVectorMapAsync(string replicaId, CancellationToken cancellationToken = default)
            => inner.LoadShardVersionVectorMapAsync(replicaId, cancellationToken);

        public Task SaveShardVersionVectorMapAsync(string replicaId, ShardVersionVectorMap shardVersionVectorMap, CancellationToken cancellationToken = default)
            => inner.SaveShardVersionVectorMapAsync(replicaId, shardVersionVectorMap, cancellationToken);

        public Task<ClusterStateSnapshotDto?> LoadClusterStateAsync(string replicaId, CancellationToken cancellationToken = default)
            => inner.LoadClusterStateAsync(replicaId, cancellationToken);

        public Task SaveClusterStateAsync(string replicaId, ClusterStateSnapshotDto state, CancellationToken cancellationToken = default)
            => inner.SaveClusterStateAsync(replicaId, state, cancellationToken);

        public Task<CrdtDocument<TState>?> LoadDocumentAsync<TState>(string shardId, string documentId, CancellationToken cancellationToken = default) where TState : class, new()
            => inner.LoadDocumentAsync<TState>(shardId, documentId, cancellationToken);

        public Task<CrdtDocument<TState>?> LoadDocumentFromAnyShardAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
            => inner.LoadDocumentFromAnyShardAsync<TState>(documentId, cancellationToken);

        public Task<CrdtDocument<TState>?> LoadOrphanedDocumentAsync<TState>(string shardId, string documentId, CancellationToken cancellationToken = default) where TState : class, new()
            => inner.LoadOrphanedDocumentAsync<TState>(shardId, documentId, cancellationToken);

        public Task SaveDocumentAsync<TState>(string shardId, string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default) where TState : class, new()
            => inner.SaveDocumentAsync(shardId, documentId, document, cancellationToken);

        public Task SaveSerializedDocumentAsync<TState>(string shardId, string documentId, byte[] serializedDocument, CancellationToken cancellationToken = default) where TState : class, new()
            => inner.SaveSerializedDocumentAsync<TState>(shardId, documentId, serializedDocument, cancellationToken);

        public Task DeleteDocumentAsync(string shardId, string documentId, CancellationToken cancellationToken = default)
            => inner.DeleteDocumentAsync(shardId, documentId, cancellationToken);

        public Task<long> GetJournalCountAsync(string shardId, CancellationToken cancellationToken = default)
            => inner.GetJournalCountAsync(shardId, cancellationToken);

        public Task TrimAsync(string shardId, IReadOnlyDictionary<string, long> globalMinimumVersionVector, CancellationToken cancellationToken = default)
            => inner.TrimAsync(shardId, globalMinimumVersionVector, cancellationToken);

        public IAsyncEnumerable<JournaledOperation> GetAllJournaledOperationsAsync(CancellationToken cancellationToken = default)
            => inner.GetAllJournaledOperationsAsync(cancellationToken);

        public Task AppendBatchAsync(IReadOnlyList<JournaledOperation> operations, CancellationToken cancellationToken = default)
            => inner.AppendBatchAsync(operations, cancellationToken);

        public void Append(IComparable documentId, IReadOnlyList<CrdtOperation> operations)
            => inner.Append(documentId, operations);

        public Task AppendAsync(IComparable documentId, IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default)
            => inner.AppendAsync(documentId, operations, cancellationToken);

        public IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string shardId, string originReplicaId, long minShardClock, long maxShardClock, CancellationToken cancellationToken = default)
            => inner.GetOperationsByRangeAsync(shardId, originReplicaId, minShardClock, maxShardClock, cancellationToken);

        public IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string shardId, string originReplicaId, IEnumerable<long> shardClocks, CancellationToken cancellationToken = default)
            => inner.GetOperationsByDotsAsync(shardId, originReplicaId, shardClocks, cancellationToken);
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
    public async Task ShardIdProviderImplementationChange_AcrossDeploymentRestart_LoadsCheckpointsAndReplaysJournalsFromOldShards()
    {
        // Arrange - Simulate Deployment V1 using LegacyBucketShardProvider ("legacy-bucket-0", "legacy-bucket-1")
        // persisting both checkpoints and uncheckpointed journal operations into storage.
        var sharedStorage = new SharedPersistentCrdtStorage(new MemoryCrdtStorage());
        const string ReplicaId = "Upgrade-Replica-1";
        var docIds = new[] { "upgrade-doc-1", "upgrade-doc-2", "upgrade-doc-3", "upgrade-doc-4" };

        {
            await using var spV1 = await CreateAndStartNodeWithCustomProviderAsync<LegacyBucketShardProvider>(
                ReplicaId,
                sharedStorage);

            var scopeV1 = spV1.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(ReplicaId);
            var dvvManagerV1 = scopeV1.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();

            foreach (var docId in docIds)
            {
                await scopeV1.Orchestrator.CreateDocumentAsync(docId, DocTypeAlias, CancellationToken.None);
                await scopeV1.Orchestrator.ApplyIntentAsync<ShardedDocState>(
                    docId,
                    new MapSetIntent("CheckpointedKey", $"V1-Checkpointed-{docId}"),
                    nameof(ShardedDocState.DataMap),
                    IntentDeliveryMode.Acknowledged,
                    CancellationToken.None);
            }

            // Persist checkpoints and Global DVV under the V1 legacy shard IDs
            foreach (var activeDocument in scopeV1.Orchestrator.GetActiveDocuments())
            {
                await activeDocument.CheckpointAsync(CancellationToken.None);
            }

            await scopeV1.Storage.SaveShardVersionVectorMapAsync(
                scopeV1.ReplicaId,
                dvvManagerV1.GetFullSnapshot(),
                CancellationToken.None);

            // Apply additional mutations that remain ONLY in the operation journal (uncheckpointed) under the V1 legacy shard IDs
            foreach (var docId in docIds)
            {
                await scopeV1.Orchestrator.ApplyIntentAsync<ShardedDocState>(
                    docId,
                    new MapSetIntent("UncheckpointedWalKey", $"V1-WalOnly-{docId}"),
                    nameof(ShardedDocState.DataMap),
                    IntentDeliveryMode.Acknowledged,
                    CancellationToken.None);
            }

            var v1Shards = dvvManagerV1.GetShardIds();
            v1Shards.ShouldContain(s => s.StartsWith("legacy-bucket-", StringComparison.Ordinal));
        }

        // Act - Simulate Deployment V2 where the developer changed IShardIdProvider to DocumentIdShardProvider ("shard-{docId}")
        // running against the existing persisted storage data from V1.
        await using var spV2 = await CreateAndStartNodeWithCustomProviderAsync<DocumentIdShardProvider>(
            ReplicaId,
            sharedStorage);

        var scopeV2 = spV2.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(ReplicaId);
        var dvvManagerV2 = scopeV2.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();

        // Assert 1 - Every document recovered both its V1 checkpointed state AND its V1 uncheckpointed journaled operations,
        // and is now mapped under the new V2 shard ID ("shard-{docId}") and accepts new V2 writes seamlessly.
        foreach (var docId in docIds)
        {
            var localDoc = scopeV2.Orchestrator.TryGetLocalDocument<ShardedDocState>(docId);
            localDoc.ShouldNotBeNull();
            localDoc.ShardId.ShouldBe($"shard-{docId}");

            var state = await scopeV2.Orchestrator.GetDocumentStateAsync<ShardedDocState>(docId, CancellationToken.None);
            state.Data.DataMap.ShouldContainKeyAndValue("CheckpointedKey", $"V1-Checkpointed-{docId}");
            state.Data.DataMap.ShouldContainKeyAndValue("UncheckpointedWalKey", $"V1-WalOnly-{docId}");

            var appliedV2 = await scopeV2.Orchestrator.ApplyIntentAsync<ShardedDocState>(
                docId,
                new MapSetIntent("PostUpgradeKey", $"V2-NewShard-{docId}"),
                nameof(ShardedDocState.DataMap),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
            appliedV2.ShouldBeTrue();

            var updatedState = await scopeV2.Orchestrator.GetDocumentStateAsync<ShardedDocState>(docId, CancellationToken.None);
            updatedState.Data.DataMap.ShouldContainKeyAndValue("PostUpgradeKey", $"V2-NewShard-{docId}");
        }

        // Assert 2 - Stale V1 shard IDs ("legacy-bucket-*") were pruned from the active ShardVersionVectorManager so they do not bloat anti-entropy
        var v2Shards = dvvManagerV2.GetShardIds();
        v2Shards.ShouldNotContain(s => s.StartsWith("legacy-bucket-", StringComparison.Ordinal));
        foreach (var docId in docIds)
        {
            v2Shards.ShouldContain($"shard-{docId}");
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
            opt.AntiEntropyMaxAdaptiveIntervalSeconds = 3600;
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

    private static async Task<ServiceProvider> CreateAndStartNodeWithCustomProviderAsync<TShardProvider>(
        string replicaId,
        IDistributedCrdtStorage sharedStorage)
        where TShardProvider : class, IShardIdProvider
    {
        var senderMock = new Mock<IDirectMessageSender>();
        var gossipMock = new Mock<IP2pAlgorithm>();

        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            opt.EnableSharding = true;
            opt.ShardReplicationFactor = 1;
            opt.VirtualNodesPerReplica = 64;
            opt.ActiveSyncEnabled = false;
            opt.AntiEntropyInitialDelaySeconds = 3600;
            opt.AntiEntropyMaxAdaptiveIntervalSeconds = 3600;
            opt.AntiEntropyIntervalSeconds = 3600;
            opt.CheckpointIntervalSeconds = 3600;
            opt.MaintenanceIntervalSeconds = 3600;
            opt.EnableGracefulShutdown = false;
            opt.JournalMode = CrdtJournalMode.Direct;
        });

        services.AddCrdt()
                .AddCrdtAotContext(new DynamicShardingTestAotContext())
                .AddCrdtJsonTypeInfoResolver(DynamicShardingTestJsonContext.Default)
                .AddCrdtShardIdProvider<TShardProvider>();

        services.AddSingleton(sharedStorage);
        services.AddDistributedCrdtStorage<SharedPersistentCrdtStorage>();
        services.AddDistributedDocumentType<ShardedDocState>(DocTypeAlias);
        services.AddDistributedCrdtReplica(replicaId, MeshId);

        services.AddSingleton(senderMock.Object);
        services.AddSingleton(gossipMock.Object);
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        services.AddSingleton<IP2pRpcCorrelator, P2pRpcCorrelator>();
        services.AddSingleton(new P2pMeshMetadata(MeshId));

        var sp = services.BuildServiceProvider();
        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().Single();
        await initService.StartAsync(CancellationToken.None);

        return sp;
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
                        networkIsolatedPeers.ContainsKey(node.PeerId))
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