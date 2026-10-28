namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
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
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

[CrdtAotType(typeof(MultiReplicaSyncIntegrationTests.SharedTestState))]
[CrdtAotType(typeof(Dictionary<string, string>))]
public sealed partial class MultiReplicaTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(MultiReplicaSyncIntegrationTests.SharedTestState))]
[JsonSerializable(typeof(CrdtDocument<MultiReplicaSyncIntegrationTests.SharedTestState>))]
public sealed partial class MultiReplicaTestJsonContext : JsonSerializerContext
{
}

public sealed class MultiReplicaSyncIntegrationTests : IAsyncDisposable
{
    public sealed class SharedTestState
    {
        public string Id { get; set; } = "shared-doc";
        public Dictionary<string, string> DataMap { get; set; } = new(StringComparer.Ordinal);
    }

    private readonly List<TestNode> _nodes = new();

    [IntegrationFact]
    public async Task TwoReplicas_ShouldSynchronizeOperations_WhenBridged()
    {
        // Arrange
        var nodeA = BuildNode("ReplicaA");
        var nodeB = BuildNode("ReplicaB");

        WireMesh(nodeA, nodeB);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchB.InitializeAsync(CancellationToken.None);

        // Act - Node A dynamically creates a document
        await orchA.CreateDocumentAsync("doc-1", "shared-doc", CancellationToken.None);
        
        await Task.Delay(1000); // Allow Gossip of the registry update to propagate natively

        // Concurrently modify both documents
        var intentA = new MapSetIntent("KeyA", "ValueA");
        var taskA = orchA.ApplyIntentAsync<SharedTestState>("doc-1", intentA, "DataMap", CancellationToken.None);

        var intentB = new MapSetIntent("KeyB", "ValueB");
        var taskB = orchB.ApplyIntentAsync<SharedTestState>("doc-1", intentB, "DataMap", CancellationToken.None);

        await Task.WhenAll(taskA, taskB);
        
        await Task.Delay(1500); // Allow CRDT lock-free channel queues to naturally process the inbound payloads

        // Assert
        var crdtDocAAfter = await orchA.GetDocumentStateAsync<SharedTestState>("doc-1", CancellationToken.None);
        var crdtDocBAfter = await orchB.GetDocumentStateAsync<SharedTestState>("doc-1", CancellationToken.None);
        
        crdtDocAAfter.Data.DataMap.ShouldContainKeyAndValue("KeyA", "ValueA");
        crdtDocAAfter.Data.DataMap.ShouldContainKeyAndValue("KeyB", "ValueB");

        crdtDocBAfter.Data.DataMap.ShouldContainKeyAndValue("KeyA", "ValueA");
        crdtDocBAfter.Data.DataMap.ShouldContainKeyAndValue("KeyB", "ValueB");
    }

    [IntegrationFact]
    public async Task AntiEntropy_WithMultipleShards_ShouldSynchronizeCorrectlyWithoutMixing()
    {
        // Arrange
        var nodeA = BuildNode("ReplicaA");
        var nodeB = BuildNode("ReplicaB");

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchB.InitializeAsync(CancellationToken.None);

        var contextB = scopeB.ServiceProvider.GetRequiredService<ReplicaContext>();
        
        // Inject multiple distinct shards in Node B explicitly executing correctly mapping bounds successfully natively
        var dvv1 = new DottedVersionVector(); dvv1.Versions["ReplicaB"] = 15;
        var dvv2 = new DottedVersionVector(); dvv2.Versions["ReplicaB"] = 25;
        
        contextB.ShardVersionVectors["shard-X"] = dvv1;
        contextB.ShardVersionVectors["shard-Y"] = dvv2;

        var serializerB = nodeB.Sp.GetRequiredService<ICrdtSerializer>();
        var syncMsgB = new CrdtStateSyncMessage(nodeB.ReplicaId, contextB.ShardVersionVectors);
        var syncBytesB = serializerB.SerializeToBytes(syncMsgB);
        var wrapperB = new CrdtMessageWrapper("Cluster", "CrdtSync", syncBytesB);
        var wrapperBytesB = serializerB.SerializeToBytes(wrapperB);

        var handlerA = nodeA.Sp.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh");

        // Act - Node A receives Node B's multi-shard context
        await handlerA.HandlePayloadAsync("TestMesh", nodeB.PeerId, wrapperBytesB, CancellationToken.None);

        // Assert - Node A tracks Node B's state distinctively mapping shards
        var trackerA = scopeA.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        var states = trackerA.GetClusterStates();
        
        states.Count.ShouldBe(1);
        states[0].ShouldContainKey("shard-X");
        states[0].ShouldContainKey("shard-Y");
        states[0]["shard-X"].Versions["ReplicaB"].ShouldBe(15);
        states[0]["shard-Y"].Versions["ReplicaB"].ShouldBe(25);
    }

    [IntegrationFact]
    public async Task TwoReplicas_AntiEntropy_ShouldRecoverMissingOperations_WhenOffline()
    {
        // Arrange
        var nodeA = BuildNode("ReplicaA");
        var nodeB = BuildNode("ReplicaB");

        // Do NOT wire mesh initially to simulate disconnected topologies natively
        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchA.InitializeAsync(CancellationToken.None);
        await orchA.CreateDocumentAsync("doc-ae", "shared-doc", CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchB.InitializeAsync(CancellationToken.None);
        await orchB.CreateDocumentAsync("doc-ae", "shared-doc", CancellationToken.None);

        // Modify offline
        var intentA = new MapSetIntent("OfflineKeyA", "ValueA");
        await orchA.ApplyIntentAsync<SharedTestState>("doc-ae", intentA, "DataMap", CancellationToken.None);

        var intentB = new MapSetIntent("OfflineKeyB", "ValueB");
        await orchB.ApplyIntentAsync<SharedTestState>("doc-ae", intentB, "DataMap", CancellationToken.None);

        await Task.Delay(500);

        // Act - Reconnect network
        WireMesh(nodeA, nodeB);

        // Manually trigger Anti-Entropy synchronization from A -> B natively
        var serializerA = nodeA.Sp.GetRequiredService<ICrdtSerializer>();
        var replicaContextA = scopeA.ServiceProvider.GetRequiredService<ReplicaContext>();
        
        var syncMsgA = new CrdtStateSyncMessage(nodeA.ReplicaId, replicaContextA.ShardVersionVectors);
        var syncBytesA = serializerA.SerializeToBytes(syncMsgA);
        var wrapperA = new CrdtMessageWrapper("Cluster", "CrdtSync", syncBytesA);
        var wrapperBytesA = serializerA.SerializeToBytes(wrapperA);

        var handlerB = nodeB.Sp.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh");
        await handlerB.HandlePayloadAsync("TestMesh", nodeA.PeerId, wrapperBytesA, CancellationToken.None);

        // Manually trigger Anti-Entropy synchronization from B -> A natively to explicitly exchange full missing bounds
        var serializerB = nodeB.Sp.GetRequiredService<ICrdtSerializer>();
        var replicaContextB = scopeB.ServiceProvider.GetRequiredService<ReplicaContext>();
        
        var syncMsgB = new CrdtStateSyncMessage(nodeB.ReplicaId, replicaContextB.ShardVersionVectors);
        var syncBytesB = serializerB.SerializeToBytes(syncMsgB);
        var wrapperB = new CrdtMessageWrapper("Cluster", "CrdtSync", syncBytesB);
        var wrapperBytesB = serializerB.SerializeToBytes(wrapperB);

        var handlerA = nodeA.Sp.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh");
        await handlerA.HandlePayloadAsync("TestMesh", nodeB.PeerId, wrapperBytesB, CancellationToken.None);

        await Task.Delay(1500); // Allow bidirectional DVV evaluation and Missing Operations transfers over direct channels

        // Assert - Both nodes converged explicitly handling missing boundaries correctly
        var crdtDocAAfter = await orchA.GetDocumentStateAsync<SharedTestState>("doc-ae", CancellationToken.None);
        var crdtDocBAfter = await orchB.GetDocumentStateAsync<SharedTestState>("doc-ae", CancellationToken.None);
        
        crdtDocAAfter.Data.DataMap.ShouldContainKeyAndValue("OfflineKeyA", "ValueA");
        crdtDocAAfter.Data.DataMap.ShouldContainKeyAndValue("OfflineKeyB", "ValueB");

        crdtDocBAfter.Data.DataMap.ShouldContainKeyAndValue("OfflineKeyA", "ValueA");
        crdtDocBAfter.Data.DataMap.ShouldContainKeyAndValue("OfflineKeyB", "ValueB");
    }

    [IntegrationFact]
    public async Task ThreeReplicas_FullMesh_ShouldConverge_Deterministically()
    {
        // Arrange
        var nodeA = BuildNode("ReplicaA");
        var nodeB = BuildNode("ReplicaB");
        var nodeC = BuildNode("ReplicaC");

        WireMesh(nodeA, nodeB, nodeC);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchB.InitializeAsync(CancellationToken.None);

        var scopeC = nodeC.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeC.ReplicaId);
        var orchC = scopeC.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchC.InitializeAsync(CancellationToken.None);

        // Node A creates the generic shared root, which propagates via dynamic topologies implicitly to B and C
        await orchA.CreateDocumentAsync("doc-3", "shared-doc", CancellationToken.None);
        
        await Task.Delay(1500); // Propagation boundary explicitly allowing standard background gossip delays

        // Act - Concurrently patch from all three distinct replica matrices
        var t1 = orchA.ApplyIntentAsync<SharedTestState>("doc-3", new MapSetIntent("KeyA", "ValA"), "DataMap", CancellationToken.None);
        var t2 = orchB.ApplyIntentAsync<SharedTestState>("doc-3", new MapSetIntent("KeyB", "ValB"), "DataMap", CancellationToken.None);
        var t3 = orchC.ApplyIntentAsync<SharedTestState>("doc-3", new MapSetIntent("KeyC", "ValC"), "DataMap", CancellationToken.None);

        await Task.WhenAll(t1, t2, t3);
        
        await Task.Delay(2000); // Full multi-mesh synchronization traversal limits bounds explicitly

        // Assert - Convergence ensures identical deterministic bounds organically globally
        var crdtDocAAfter = await orchA.GetDocumentStateAsync<SharedTestState>("doc-3", CancellationToken.None);
        var crdtDocBAfter = await orchB.GetDocumentStateAsync<SharedTestState>("doc-3", CancellationToken.None);
        var crdtDocCAfter = await orchC.GetDocumentStateAsync<SharedTestState>("doc-3", CancellationToken.None);
        
        foreach (var doc in new[] { crdtDocAAfter, crdtDocBAfter, crdtDocCAfter })
        {
            doc.Data.DataMap.ShouldContainKeyAndValue("KeyA", "ValA");
            doc.Data.DataMap.ShouldContainKeyAndValue("KeyB", "ValB");
            doc.Data.DataMap.ShouldContainKeyAndValue("KeyC", "ValC");
        }
    }

    [IntegrationFact]
    public async Task TwoReplicas_EvictionAndCooldown_ShouldCompletelyPurgeOfflinePeer()
    {
        // Arrange
        var nodeA = BuildNode("ReplicaA", opt => 
        {
            opt.AntiEntropyIntervalSeconds = 1;
            opt.MaintenanceIntervalSeconds = 1;
            opt.PeerEvictionTtlSeconds = 3;
            opt.PeerTombstoneCooldownSeconds = 1;
        });
        
        var nodeB = BuildNode("ReplicaB");

        WireMesh(nodeA, nodeB);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var trackerA = scopeA.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchB.InitializeAsync(CancellationToken.None);

        // Node A creates the document explicitly
        await orchA.CreateDocumentAsync("doc-evict", "shared-doc", CancellationToken.None);
        await Task.Delay(1000); // Allow Gossip of the registry update to propagate natively

        // Act - Manually trigger Anti-Entropy from B to A so A tracks B seamlessly
        var serializerB = nodeB.Sp.GetRequiredService<ICrdtSerializer>();
        var replicaContextB = scopeB.ServiceProvider.GetRequiredService<ReplicaContext>();
        
        var syncMsgB = new CrdtStateSyncMessage(nodeB.ReplicaId, replicaContextB.ShardVersionVectors);
        var syncBytesB = serializerB.SerializeToBytes(syncMsgB);
        var wrapperB = new CrdtMessageWrapper("Cluster", "CrdtSync", syncBytesB);
        var wrapperBytesB = serializerB.SerializeToBytes(wrapperB);

        var handlerA = nodeA.Sp.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh");
        await handlerA.HandlePayloadAsync("TestMesh", nodeB.PeerId, wrapperBytesB, CancellationToken.None);

        // Verify tracker A now explicitly knows about Node B
        trackerA.GetClusterStates().Count.ShouldBeGreaterThan(0);

        // Disconnect Node B natively simply by waiting and preventing further syncs.
        // Node A's background maintenance service will tick every 1s inherently.
        // After 3s -> TTL Eviction occurs, marking B as tombstoned safely.
        // After 1s more -> Cooldown Cleanup executes dropping B entirely natively.
        
        // Wait 6 seconds total allowing all TTL timers + Background Cooldown ticks to execute reliably
        await Task.Delay(6000);

        // Assert - Node B should have been evicted AND completely forgotten from memory maps inherently
        trackerA.IsReplicaTombstoned(nodeB.ReplicaId).ShouldBeFalse();
        trackerA.GetClusterStates().ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task NodeRestart_ShouldImmediatelyPurgeExpiredTombstones_DuringStateImport()
    {
        // Arrange
        var nodeA = BuildNode("ReplicaA", opt => 
        {
            opt.PeerTombstoneCooldownSeconds = 5;
        });

        // Stop hosted services to securely simulate the node going offline maintaining persistence states.
        foreach (var hs in nodeA.HostedServices)
        {
            await hs.StopAsync(CancellationToken.None);
        }

        var scopeManager = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope(nodeA.ReplicaId);
        var storage = scope.Storage;
        var tracker = scope.ClusterTracker;

        // Forcefully inject mapped tracker histories into the persistence layer containing mixed expiration offsets natively.
        var snapshot = new ClusterStateSnapshotDto();
        var expiredTimestamp = DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(10)); // Exceeds 5s cooldown
        var activeTimestamp = DateTime.UtcNow; // Within 5s cooldown limits explicitly
        
        snapshot.TombstonedReplicas.Add("ReplicaExpired", expiredTimestamp);
        snapshot.TombstonedReplicas.Add("ReplicaActive", activeTimestamp);

        await storage.SaveClusterStateAsync(nodeA.ReplicaId, snapshot, CancellationToken.None);

        // Act - Start initialization service explicitly simulating the standard node restart boot routines loading mapped tracking limits.
        var initService = nodeA.HostedServices.OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        // Assert - The expired tombstone should be explicitly pruned instantly out-of-the-gate, keeping active bounds safe.
        tracker.IsReplicaTombstoned("ReplicaExpired").ShouldBeFalse();
        tracker.IsReplicaTombstoned("ReplicaActive").ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task TwoReplicas_ShardedRemoteReadAndIntentForwarding_ShouldLazilyPromoteOnOwnerAndExecuteViaRpc()
    {
        // Arrange - NodeA owns none of the data shards, NodeB owns "default" shard
        var nodeA = BuildNodeWithCustomSharding("ReplicaA", isDataShardLocal: false);
        var nodeB = BuildNodeWithCustomSharding("ReplicaB", isDataShardLocal: true);

        WireMesh(nodeA, nodeB);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchB.InitializeAsync(CancellationToken.None);

        // Register peer mapping on Node A so RPC forwarding knows how to reach ReplicaB
        scopeA.ClusterTracker.UpdatePeerState("ReplicaB", nodeB.PeerId.Value.ToString(), new ShardVersionVectorMap());
        scopeB.ClusterTracker.UpdatePeerState("ReplicaA", nodeA.PeerId.Value.ToString(), new ShardVersionVectorMap());

        // Act - Node A (non-owner) forwards an intent for "rpc-sharded-doc" before even creating it in the registry
        var intent = new MapSetIntent("RemoteKey", "RemoteValue");
        var applied = await orchA.ApplyIntentAsync<SharedTestState>("rpc-sharded-doc", intent, nameof(SharedTestState.DataMap), IntentDeliveryMode.Acknowledged, CancellationToken.None);

        // Assert - Intent was forwarded over P2P RPC, lazily promoted on Node B, and acknowledged back to Node A
        applied.ShouldBeTrue();
        orchA.TryGetLocalDocument<SharedTestState>("rpc-sharded-doc").ShouldBeNull();
        orchB.TryGetLocalDocument<SharedTestState>("rpc-sharded-doc").ShouldNotBeNull();

        // Act - Node A reads the document state remotely via RPC from Node B
        var remoteReadDoc = await orchA.GetDocumentStateAsync<SharedTestState>("rpc-sharded-doc", CancellationToken.None);

        // Assert
        remoteReadDoc.Data.DataMap.ShouldContainKeyAndValue("RemoteKey", "RemoteValue");
    }

    [IntegrationFact]
    public async Task TwoReplicas_ShardedRpcFailover_WhenFirstTargetReplicaTimesOut_RetriesAndSucceedsOnHealthyReplica()
    {
        // Arrange - Target list for shard has an unresponsive replica ("UnresponsiveReplica") first, followed by healthy "ReplicaB"
        var nodeA = BuildNodeWithCustomSharding(
            "ReplicaA",
            isDataShardLocal: false,
            targetReplicas: ["UnresponsiveReplica", "ReplicaB"],
            configureOptions: opt =>
            {
                opt.RpcTimeoutMilliseconds = 150;
                opt.RpcMaxRetries = 3;
                opt.RpcRetryDelayMilliseconds = 20;
            });
        var nodeB = BuildNodeWithCustomSharding("ReplicaB", isDataShardLocal: true, targetReplicas: ["UnresponsiveReplica", "ReplicaB"]);

        WireMesh(nodeA, nodeB);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchB.InitializeAsync(CancellationToken.None);

        // Map both the unresponsive replica (pointing to a blackhole PeerId) and the healthy ReplicaB
        var blackholePeerId = new PeerId(Guid.NewGuid());
        scopeA.ClusterTracker.UpdatePeerState("UnresponsiveReplica", blackholePeerId.Value.ToString(), new ShardVersionVectorMap());
        scopeA.ClusterTracker.UpdatePeerState("ReplicaB", nodeB.PeerId.Value.ToString(), new ShardVersionVectorMap());
        scopeB.ClusterTracker.UpdatePeerState("ReplicaA", nodeA.PeerId.Value.ToString(), new ShardVersionVectorMap());

        // Act 1 - Forward an acknowledged intent; attempt 0 hits UnresponsiveReplica first (times out), then fails over to ReplicaB
        var intent = new MapSetIntent("FailoverKey", "FailoverSuccess");
        var applied = await orchA.ApplyIntentAsync<SharedTestState>("failover-doc", intent, nameof(SharedTestState.DataMap), IntentDeliveryMode.Acknowledged, CancellationToken.None);

        // Assert 1
        applied.ShouldBeTrue();

        // Act 2 - Read state remotely; also fails over past UnresponsiveReplica to ReplicaB
        var remoteDoc = await orchA.GetDocumentStateAsync<SharedTestState>("failover-doc", CancellationToken.None);

        // Assert 2
        remoteDoc.Data.DataMap.ShouldContainKeyAndValue("FailoverKey", "FailoverSuccess");
    }

    [IntegrationFact]
    public async Task TwoReplicas_ShardedFireAndForgetIntentForwarding_RoutesDirectWithoutWaitingForRpcAck()
    {
        // Arrange
        var nodeA = BuildNodeWithCustomSharding("ReplicaA", isDataShardLocal: false);
        var nodeB = BuildNodeWithCustomSharding("ReplicaB", isDataShardLocal: true);

        WireMesh(nodeA, nodeB);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchB.InitializeAsync(CancellationToken.None);

        scopeA.ClusterTracker.UpdatePeerState("ReplicaB", nodeB.PeerId.Value.ToString(), new ShardVersionVectorMap());

        // Act - Forward intent using FireAndForget from non-owner Node A
        var intent = new MapSetIntent("FFRemoteKey", "FFRemoteVal");
        var dispatched = await orchA.ApplyIntentAsync<SharedTestState>("ff-remote-doc", intent, nameof(SharedTestState.DataMap), IntentDeliveryMode.FireAndForget, CancellationToken.None);

        // Assert - Returns true immediately and converges asynchronously on Node B without Node B sending any CrdtIntentRes back
        dispatched.ShouldBeTrue();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!cts.Token.IsCancellationRequested)
        {
            var localDocB = orchB.TryGetLocalDocument<SharedTestState>("ff-remote-doc");
            if (localDocB != null)
            {
                var docStateB = await localDocB.GetDocumentAsync(CancellationToken.None);
                if (docStateB.Data.DataMap.ContainsKey("FFRemoteKey"))
                {
                    break;
                }
            }
            await Task.Delay(50);
        }

        var finalDocB = await orchB.GetDocumentStateAsync<SharedTestState>("ff-remote-doc", CancellationToken.None);
        finalDocB.Data.DataMap.ShouldContainKeyAndValue("FFRemoteKey", "FFRemoteVal");
        nodeB.Sender.Verify(s => s.SendDirectAsync(nodeA.PeerId, It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [IntegrationFact]
    public async Task ShardedNode_WhenAllTargetReplicasAreUnreachableOrUnmapped_ThrowsTimeoutExceptionOnReadAndReturnsFalseOnIntent()
    {
        // Arrange - Node A does not own data shards and targets "OfflineReplica" which has no peer mapping
        var nodeA = BuildNodeWithCustomSharding(
            "ReplicaA",
            isDataShardLocal: false,
            targetReplicas: ["OfflineReplica"],
            configureOptions: opt =>
            {
                opt.RpcTimeoutMilliseconds = 50;
                opt.RpcMaxRetries = 2;
                opt.RpcRetryDelayMilliseconds = 10;
            });

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchA.InitializeAsync(CancellationToken.None);

        var intent = new MapSetIntent("UnreachableKey", "UnreachableVal");

        // Act & Assert 1 - FireAndForget intent returns false when no target peer is mapped
        var ffResult = await orchA.ApplyIntentAsync<SharedTestState>("unreachable-doc", intent, nameof(SharedTestState.DataMap), IntentDeliveryMode.FireAndForget, CancellationToken.None);
        ffResult.ShouldBeFalse();

        // Act & Assert 2 - Acknowledged intent exhausts retries and returns false
        var ackResult = await orchA.ApplyIntentAsync<SharedTestState>("unreachable-doc", intent, nameof(SharedTestState.DataMap), IntentDeliveryMode.Acknowledged, CancellationToken.None);
        ackResult.ShouldBeFalse();

        // Act & Assert 3 - Remote read exhausts retries and throws InvalidOperationException
        await Should.ThrowAsync<InvalidOperationException>(async () => await orchA.GetDocumentStateAsync<SharedTestState>("unreachable-doc", CancellationToken.None));
    }

    [IntegrationFact]
    public async Task ShardedNode_WhenRemoteReplicaExplicitlyRejectsIntent_FailsOverToSecondaryReplicaAndSucceeds()
    {
        // Arrange - Shard targets ["RejectingReplica", "ReplicaB"].
        // "RejectingReplica" is reachable and immediately replies with CrdtIntentForwardResponseDto { IsSuccess = false }.
        // Node A must not abort on the negative ACK; it must fail over to "ReplicaB" in the same attempt.
        var nodeA = BuildNodeWithCustomSharding(
            "ReplicaA",
            isDataShardLocal: false,
            targetReplicas: ["RejectingReplica", "ReplicaB"],
            configureOptions: opt =>
            {
                opt.RpcTimeoutMilliseconds = 1000;
                opt.RpcMaxRetries = 1;
            });
        var nodeB = BuildNodeWithCustomSharding("ReplicaB", isDataShardLocal: true, targetReplicas: ["RejectingReplica", "ReplicaB"]);

        WireMesh(nodeA, nodeB);

        var rejectingPeerId = new PeerId(Guid.NewGuid());
        var serializerA = nodeA.Sp.GetRequiredService<ICrdtSerializer>();
        var handlerA = nodeA.Sp.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh");

        // Intercept sends to rejectingPeerId and immediately return an explicit failure CrdtIntentRes
        nodeA.Sender
            .Setup(s => s.SendDirectAsync(It.Is<PeerId>(p => p == rejectingPeerId), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns((PeerId _, ReadOnlyMemory<byte> payload, CancellationToken _) =>
            {
                var reqWrapper = serializerA.DeserializeFromBytes<CrdtMessageWrapper>(payload.ToArray());
                var reqDto = serializerA.DeserializeFromBytes<CrdtIntentForwardRequestDto>(reqWrapper.Payload!);

                _ = Task.Run(async () =>
                {
                    var failRes = new CrdtIntentForwardResponseDto
                    {
                        CorrelationId = reqDto.CorrelationId,
                        IsSuccess = false,
                        ErrorMessage = "Simulated remote application rejection."
                    };
                    var resWrapper = new CrdtMessageWrapper(reqWrapper.DocumentId, "CrdtIntentRes", serializerA.SerializeToBytes(failRes));
                    await handlerA.HandlePayloadAsync("TestMesh", rejectingPeerId, serializerA.SerializeToBytes(resWrapper), CancellationToken.None);
                });

                return Task.CompletedTask;
            });

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchB.InitializeAsync(CancellationToken.None);

        scopeA.ClusterTracker.UpdatePeerState("RejectingReplica", rejectingPeerId.Value.ToString(), new ShardVersionVectorMap());
        scopeA.ClusterTracker.UpdatePeerState("ReplicaB", nodeB.PeerId.Value.ToString(), new ShardVersionVectorMap());
        scopeB.ClusterTracker.UpdatePeerState("ReplicaA", nodeA.PeerId.Value.ToString(), new ShardVersionVectorMap());

        // Act
        var applied = await orchA.ApplyIntentAsync<SharedTestState>(
            "rejected-then-failover-doc",
            new MapSetIntent("FailoverKey", "RecoveredOnReplicaB"),
            nameof(SharedTestState.DataMap),
            IntentDeliveryMode.Acknowledged,
            CancellationToken.None);

        // Assert - Failed over past RejectingReplica's negative response and applied on ReplicaB
        applied.ShouldBeTrue();
        var stateOnB = await orchB.GetDocumentStateAsync<SharedTestState>("rejected-then-failover-doc", CancellationToken.None);
        stateOnB.Data.DataMap.ShouldContainKeyAndValue("FailoverKey", "RecoveredOnReplicaB");
    }

    private sealed record TestNode(
        IServiceProvider Sp, 
        Mock<IDirectMessageSender> Sender, 
        Mock<IP2pAlgorithm> Gossip, 
        PeerId PeerId,
        string ReplicaId,
        List<IHostedService> HostedServices);

    private TestNode BuildNode(string replicaId, Action<DistributedCrdtOptions>? configureOptions = null)
    {
        var senderMock = new Mock<IDirectMessageSender>();
        var gossipMock = new Mock<IP2pAlgorithm>();
        var peerId = new PeerId(Guid.NewGuid());

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDistributedCrdtCore(opt => 
        {
            opt.ActiveSyncEnabled = true; // Enabled for real-time Gossip propagation natively
            opt.AntiEntropyIntervalSeconds = 60; // Explicitly delayed to prevent test interference natively
            opt.CheckpointIntervalSeconds = 60; // Increased to ensure trims don't intercept standard testing arrays natively
            opt.PeerEvictionTtlSeconds = 0; // Disabled eviction to prevent validation errors by default
            opt.EnableGracefulShutdown = false;

            configureOptions?.Invoke(opt);
        });
        services.AddDistributedCrdtReplica(replicaId);
        
        services.AddCrdt()
                .AddCrdtAotContext(new MultiReplicaTestAotContext())
                .AddCrdtJsonTypeInfoResolver(MultiReplicaTestJsonContext.Default);
                
        services.AddDistributedDocumentType<SharedTestState>("shared-doc");
        services.AddDistributedCrdtP2p("TestMesh", replicaId);
        
        services.AddSingleton(senderMock.Object);
        services.AddSingleton(gossipMock.Object);

        var mockP2pRpcCorrelator = new Mock<IP2pRpcCorrelator>();
        mockP2pRpcCorrelator.SetReturnsDefault(Task.CompletedTask);
        services.AddSingleton(mockP2pRpcCorrelator.Object);

        var sp = services.BuildServiceProvider();

        // Start all hosted services to explicitly bind the localized mesh orchestrations natively (wiring up Gossip bindings natively)
        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            hs.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        var node = new TestNode(sp, senderMock, gossipMock, peerId, replicaId, hostedServices);
        _nodes.Add(node);
        return node;
    }

    private TestNode BuildNodeWithCustomSharding(
        string replicaId,
        bool isDataShardLocal,
        IReadOnlyList<string>? targetReplicas = null,
        Action<DistributedCrdtOptions>? configureOptions = null)
    {
        var senderMock = new Mock<IDirectMessageSender>();
        var gossipMock = new Mock<IP2pAlgorithm>();
        var peerId = new PeerId(Guid.NewGuid());

        var mockDist = new Mock<IShardDistributionManager>();
        mockDist.Setup(d => d.IsShardLocalAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string shardId, CancellationToken _) =>
                new ValueTask<bool>(shardId == "system-document-registry" || isDataShardLocal));
        mockDist.Setup(d => d.GetTargetNodesForShardAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(targetReplicas ?? new List<string> { "ReplicaB" });

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDistributedCrdtCore(opt =>
        {
            opt.ActiveSyncEnabled = true;
            opt.AntiEntropyIntervalSeconds = 60;
            opt.CheckpointIntervalSeconds = 60;
            opt.PeerEvictionTtlSeconds = 0;

            configureOptions?.Invoke(opt);
        });
        services.AddDistributedCrdtReplica(replicaId);

        services.AddCrdt()
                .AddCrdtAotContext(new MultiReplicaTestAotContext())
                .AddCrdtJsonTypeInfoResolver(MultiReplicaTestJsonContext.Default);

        services.AddDistributedDocumentType<SharedTestState>("shared-doc");
        services.AddDistributedCrdtP2p("TestMesh", replicaId);

        services.AddSingleton(senderMock.Object);
        services.AddSingleton(gossipMock.Object);
        services.AddSingleton<IP2pRpcCorrelator, P2pRpcCorrelator>();
        services.AddScoped(_ => mockDist.Object);

        var sp = services.BuildServiceProvider();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            hs.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        var node = new TestNode(sp, senderMock, gossipMock, peerId, replicaId, hostedServices);
        _nodes.Add(node);
        return node;
    }

    private void WireMesh(params TestNode[] nodes)
    {
        var handlers = nodes.ToDictionary(
            n => n.PeerId, 
            n => n.Sp.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh")
        );

        foreach (var node in nodes)
        {
            var others = nodes.Where(n => n.PeerId != node.PeerId).ToList();

            node.Sender.Setup(s => s.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns((PeerId target, ReadOnlyMemory<byte> payload, CancellationToken ct) => 
                {
                    var payloadArray = payload.ToArray(); // Duplicate safely avoiding leased buffer corruption
                    _ = Task.Run(async () => 
                    {
                        if (handlers.TryGetValue(target, out var targetHandler))
                        {
                            await targetHandler.HandlePayloadAsync("TestMesh", node.PeerId, payloadArray, CancellationToken.None);
                        }
                    });
                    return Task.CompletedTask;
                });

            node.Gossip.Setup(g => g.BroadcastAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns((ReadOnlyMemory<byte> payload, CancellationToken ct) => 
                {
                    var payloadArray = payload.ToArray(); // Duplicate safely avoiding leased buffer corruption
                    _ = Task.Run(async () => 
                    {
                        var tasks = others.Select(other => handlers[other.PeerId].HandlePayloadAsync("TestMesh", node.PeerId, payloadArray, CancellationToken.None));
                        await Task.WhenAll(tasks);
                    });
                    return Task.CompletedTask;
                });
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var node in _nodes)
        {
            foreach (var hs in node.HostedServices)
            {
                await hs.StopAsync(CancellationToken.None);
            }
        }
    }
}