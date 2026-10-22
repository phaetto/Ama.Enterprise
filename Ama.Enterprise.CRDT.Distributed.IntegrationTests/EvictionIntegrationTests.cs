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
using Ama.CRDT.Services.Versioning;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

[CrdtAotType(typeof(EvictionIntegrationTests.TestState))]
[CrdtAotType(typeof(Dictionary<string, string>))]
public sealed partial class EvictionIntegrationTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(EvictionIntegrationTests.TestState))]
[JsonSerializable(typeof(CrdtDocument<EvictionIntegrationTests.TestState>))]
public sealed partial class EvictionIntegrationTestJsonContext : JsonSerializerContext
{
}

public class EvictionIntegrationTests
{
    public class TestState
    {
        public string Id { get; set; } = "test-doc";
        public Dictionary<string, string> DataMap { get; set; } = new(StringComparer.Ordinal);
    }

    private IServiceProvider BuildNode(string replicaId, Action<IServiceCollection>? configureExtra = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        
        services.AddDistributedCrdtCore(opt =>
        {
            opt.CheckpointIntervalSeconds = 30;
            opt.AntiEntropyIntervalSeconds = 15;
        });

        services.AddDistributedCrdtReplica(replicaId);

        services.AddCrdt()
                .AddCrdtAotContext(new EvictionIntegrationTestAotContext())
                .AddCrdtJsonTypeInfoResolver(EvictionIntegrationTestJsonContext.Default);

        services.AddDistributedDocumentType<TestState>("test-doc");
        services.AddSingleton(Mock.Of<IP2pAlgorithm>());

        configureExtra?.Invoke(services);

        var sp = services.BuildServiceProvider();
        return sp;
    }

    private async Task StartNodeAsync(IServiceProvider sp)
    {
        var hostedServices = sp.GetServices<IHostedService>();
        foreach (var svc in hostedServices)
        {
            await svc.StartAsync(CancellationToken.None);
        }
    }

    [IntegrationFact]
    public async Task CrdtEvictionService_EvictsPeers_UpdatesGlobalVersionVector()
    {
        // Arrange
        var sp = BuildNode("NodeA");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("NodeA");
        
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);
        
        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        var evictionService = scope.ServiceProvider.GetRequiredService<ICrdtEvictionService>();

        context.ShardVersionVectors["test-doc"] = new DottedVersionVector();
        context.ShardVersionVectors["test-doc"].Versions["RemoteReplica1"] = 50;

        // Act
        await evictionService.EvictPeersAsync(new[] { "RemoteReplica1" }, CancellationToken.None);

        // Assert
        context.ShardVersionVectors["test-doc"].Versions.ContainsKey("RemoteReplica1").ShouldBeFalse();
    }

    [IntegrationFact]
    public async Task CrdtEvictionService_EvictsPeers_IsolatesShardClearing_WithoutCrossContamination()
    {
        // Arrange
        var sp = BuildNode("NodeA");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("NodeA");
        
        // Initialize orchestrator natively prior to interacting with documents to avoid null reference exceptions.
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);
        
        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        var evictionService = scope.ServiceProvider.GetRequiredService<ICrdtEvictionService>();

        context.ShardVersionVectors["shard-A"] = new DottedVersionVector();
        context.ShardVersionVectors["shard-A"].Versions["TargetEvict"] = 50;
        context.ShardVersionVectors["shard-A"].Versions["SafePeer"] = 100;

        context.ShardVersionVectors["shard-B"] = new DottedVersionVector();
        context.ShardVersionVectors["shard-B"].Versions["TargetEvict"] = 20;
        context.ShardVersionVectors["shard-B"].Versions["SafePeer"] = 80;

        // Act - Trigger generic structured bounds matching node constraints explicitly across bounds
        await evictionService.EvictPeersAsync(new[] { "TargetEvict" }, CancellationToken.None);

        // Assert - Evaluates correct isolated tracking constraints inherently
        context.ShardVersionVectors["shard-A"].Versions.ContainsKey("TargetEvict").ShouldBeFalse();
        context.ShardVersionVectors["shard-A"].Versions["SafePeer"].ShouldBe(100);

        context.ShardVersionVectors["shard-B"].Versions.ContainsKey("TargetEvict").ShouldBeFalse();
        context.ShardVersionVectors["shard-B"].Versions["SafePeer"].ShouldBe(80);
    }

    [IntegrationFact]
    public void TombstonesAreEphemeral_UnlessPersisted()
    {
        // Arrange - Initial Node Run
        var sp1 = BuildNode("NodeA");
        var scopeManager1 = sp1.GetRequiredService<DistributedCrdtScopeManager>();
        var scope1 = scopeManager1.GetOrCreateScope("NodeA");
        var tracker1 = scope1.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        
        tracker1.UpdatePeerState("NodeB", "NetworkB", new ShardVersionVectorMap());
        
        // Act
        tracker1.TombstoneReplica("NodeB");
        
        // Assert
        tracker1.IsReplicaTombstoned("NodeB").ShouldBeTrue();
        
        // Act - Simulate Node Restart
        var sp2 = BuildNode("NodeA");
        var scopeManager2 = sp2.GetRequiredService<DistributedCrdtScopeManager>();
        var scope2 = scopeManager2.GetOrCreateScope("NodeA");
        var tracker2 = scope2.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        
        // Assert
        tracker2.IsReplicaTombstoned("NodeB").ShouldBeFalse();
    }

    [IntegrationFact]
    public void NetworkDisconnects_PreserveState_ToPreventAmnesia()
    {
        // Arrange
        var sp = BuildNode("NodeA");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("NodeA");
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        var syncService = scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
        
        var nodeA_Dvv = new DottedVersionVector();
        nodeA_Dvv.Versions["NodeA"] = 5;
        var nodeA_Map = new ShardVersionVectorMap { ["test-doc"] = nodeA_Dvv };
        
        var nodeB_Dvv = new DottedVersionVector();
        nodeB_Dvv.Versions["NodeA"] = 2; // Node B is behind
        var nodeB_Map = new ShardVersionVectorMap { ["test-doc"] = nodeB_Dvv };
        
        tracker.UpdatePeerState("NodeA", "NetworkA", nodeA_Map);
        tracker.UpdatePeerState("NodeB", "NetworkB", nodeB_Map);
        
        // Act
        tracker.RemovePeerByNetworkId("NetworkB");
        
        var clusterStates = tracker.GetClusterStates();
        var vectors = clusterStates.Select(s => s.TryGetValue("test-doc", out var v) ? v : new DottedVersionVector()).ToList();
        var gmvv = syncService.CalculateGlobalMinimumVersionVector(vectors);
        
        // Assert
        gmvv["NodeA"].ShouldBe(2);
    }

    [IntegrationFact]
    public async Task RebootLocalIdentity_PreservesOfflineLocalData()
    {
        // Arrange
        var sp = BuildNode("NodeA");
        await StartNodeAsync(sp);

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("NodeA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.CreateDocumentAsync("test-doc", "test-doc", CancellationToken.None);
        
        var evictionService = scope.ServiceProvider.GetRequiredService<ICrdtEvictionService>();
        
        var crdtDoc = await orchestrator.GetDocumentStateAsync<TestState>("test-doc", CancellationToken.None);
        crdtDoc.Data.DataMap["Key"] = "Unsaved Offline Edit";
        
        // Act
        await evictionService.RebootLocalIdentityAsync(CancellationToken.None);
        
        // Assert
        var crdtDocAfter = await orchestrator.GetDocumentStateAsync<TestState>("test-doc", CancellationToken.None);
        crdtDocAfter.Data.DataMap["Key"].ShouldBe("Unsaved Offline Edit");
    }

    [IntegrationFact]
    public async Task SnapshotMerge_OverwritesPendingLocalEdits()
    {
        // Arrange
        var sp = BuildNode("NodeA");
        
        await StartNodeAsync(sp);

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("NodeA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.CreateDocumentAsync("test-doc", "test-doc", CancellationToken.None);

        var docManager = orchestrator.TryGetLocalDocument<TestState>("test-doc")!;
        var crdtDoc = await orchestrator.GetDocumentStateAsync<TestState>("test-doc", CancellationToken.None);
        var metadataManager = scope.ServiceProvider.GetRequiredService<ICrdtMetadataManager>();
        var serializer = scope.ServiceProvider.GetRequiredService<ICrdtSerializer>();
        var patcher = scope.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        var applicator = scope.ServiceProvider.GetRequiredService<IAsyncCrdtApplicator>();
        
        // Local pending edit (no metadata tracking)
        crdtDoc.Data.DataMap["Key"] = "Local Pending Edit";
        
        var snapshotState = new TestState { Id = "test-doc" };
        var metadata = metadataManager.Initialize(snapshotState);
        var snapshotDoc = new CrdtDocument<TestState>(snapshotState, metadata);
        
        // Advance the CRDT clock on the snapshot to ensure it dominates during the true state merge
        var intent = new MapSetIntent("Key", "Cluster Snapshot Data");
        var op = await patcher.GenerateOperationAsync(snapshotDoc, x => x.DataMap, intent, CancellationToken.None);
        var appliedDoc = await applicator.ApplyPatchAsync(snapshotDoc, new CrdtPatch(new[] { op }));
        
        var snapshotData = serializer.SerializeToBytes(appliedDoc.Document);
            
        // Act
        await docManager.MergeSnapshotAsync(snapshotData, new ShardVersionVectorMap(), CancellationToken.None);
        
        // Assert
        var crdtDocAfter = await orchestrator.GetDocumentStateAsync<TestState>("test-doc", CancellationToken.None);
        crdtDocAfter.Data.DataMap["Key"].ShouldBe("Cluster Snapshot Data");
    }

    [IntegrationFact]
    public async Task RebootLocalIdentity_WhenInvokedMultipleTimes_PreservesOriginalPrefixWithoutStackingGuids()
    {
        // Arrange
        var sp = BuildNode("NodeA");
        await StartNodeAsync(sp);

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("NodeA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        var evictionService = scope.ServiceProvider.GetRequiredService<ICrdtEvictionService>();
        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

        // Act - Reboot identity twice in a row
        await evictionService.RebootLocalIdentityAsync(CancellationToken.None);
        var firstRebootId = replicaContext.ReplicaId;

        await evictionService.RebootLocalIdentityAsync(CancellationToken.None);
        var secondRebootId = replicaContext.ReplicaId;

        // Assert - Prefix stays "NodeA_" with a single 32-char GUID suffix instead of stacking "NodeA_<guid>_<guid>"
        firstRebootId.ShouldStartWith("NodeA_");
        firstRebootId.Length.ShouldBe("NodeA_".Length + 32);

        secondRebootId.ShouldStartWith("NodeA_");
        secondRebootId.Length.ShouldBe("NodeA_".Length + 32);
        secondRebootId.ShouldNotBe(firstRebootId);
    }

    [IntegrationFact]
    public async Task CrdtEvictionService_EvictsPeers_ClearsBothVersionsAndOutOfOrderDotsAcrossShards()
    {
        // Arrange
        var sp = BuildNode("NodeA");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("NodeA");

        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        var evictionService = scope.ServiceProvider.GetRequiredService<ICrdtEvictionService>();

        var dvv = new DottedVersionVector();
        dvv.Versions["DeadPeer"] = 5;
        dvv.Add("DeadPeer", 8); // Out-of-order dot at 8
        dvv.Versions["HealthyPeer"] = 12;
        dvv.Add("HealthyPeer", 15);

        context.ShardVersionVectors["shard-dots"] = dvv;

        // Act
        await evictionService.EvictPeersAsync(new[] { "DeadPeer" }, CancellationToken.None);

        // Assert - Both Versions and Dots for DeadPeer are removed while HealthyPeer remains intact
        var updatedDvv = context.ShardVersionVectors["shard-dots"];
        updatedDvv.Versions.ContainsKey("DeadPeer").ShouldBeFalse();
        updatedDvv.Dots.ContainsKey("DeadPeer").ShouldBeFalse();

        updatedDvv.Versions["HealthyPeer"].ShouldBe(12);
        updatedDvv.Dots["HealthyPeer"].ShouldContain(15L);
    }

    [IntegrationFact]
    public async Task CrdtP2pPayloadHandler_WhenTombstonedReplicaSendsPatchOrSnapshot_RejectsPayloadAndBroadcastsEvictionRejection()
    {
        // Arrange
        var capturedBroadcasts = new List<ReadOnlyMemory<byte>>();
        var mockP2p = new Mock<IP2pAlgorithm>();
        mockP2p.Setup(p => p.BroadcastAsync(Capture.In(capturedBroadcasts), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var sp = BuildNode("NodeA", services =>
        {
            services.AddDistributedCrdtP2p("TestMesh", "NodeA");
            services.AddSingleton(mockP2p.Object);
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("NodeA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        await orchestrator.CreateDocumentAsync("test-doc", "test-doc", CancellationToken.None);

        // Tombstone ZombieReplica on NodeA
        scope.ClusterTracker.TombstoneReplica("ZombieReplica");
        capturedBroadcasts.Clear();

        var serializer = sp.GetRequiredService<ICrdtSerializer>();
        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh");

        // Construct a CrdtPatch from ZombieReplica
        var dummyOp = default(CrdtOperation) with { Id = Guid.NewGuid(), ReplicaId = "ZombieReplica", JsonPath = "$.dataMap.ZombieKey", ShardId = "default", ShardClock = 1 };
        var patchMsg = new Models.CrdtPatchMessage("ZombieReplica", new CrdtPatch([dummyOp]));
        var patchWrapper = new Models.CrdtMessageWrapper("test-doc", "CrdtPatch", serializer.SerializeToBytes(patchMsg), "test-doc");

        // Construct a CrdtSnapshot from ZombieReplica
        var metadataManager = scope.ServiceProvider.GetRequiredService<ICrdtMetadataManager>();
        var zombieState = new TestState { Id = "test-doc" };
        zombieState.DataMap["ZombieSnapKey"] = "ShouldBeRejected";
        var zombieDoc = new CrdtDocument<TestState>(zombieState, metadataManager.Initialize(zombieState));
        var snapMsg = new Models.CrdtSnapshotMessage("ZombieReplica", serializer.SerializeToBytes(zombieDoc), new ShardVersionVectorMap());
        var snapWrapper = new Models.CrdtMessageWrapper("test-doc", "CrdtSnapshot", serializer.SerializeToBytes(snapMsg), "test-doc");

        // Act
        await handler.HandlePayloadAsync("TestMesh", new Ama.Enterprise.P2p.Models.Core.PeerId(Guid.NewGuid()), serializer.SerializeToBytes(patchWrapper), CancellationToken.None);
        await handler.HandlePayloadAsync("TestMesh", new Ama.Enterprise.P2p.Models.Core.PeerId(Guid.NewGuid()), serializer.SerializeToBytes(snapWrapper), CancellationToken.None);

        // Assert - Neither payload was applied, and two CrdtEviction rejection messages were broadcast
        var docAfter = await orchestrator.GetDocumentStateAsync<TestState>("test-doc", CancellationToken.None);
        docAfter.Data.DataMap.ShouldBeEmpty();

        capturedBroadcasts.Count.ShouldBe(2);
        foreach (var broadcast in capturedBroadcasts)
        {
            var wrapper = serializer.DeserializeFromBytes<Models.CrdtMessageWrapper>(broadcast.ToArray());
            wrapper.MessageType.ShouldBe("CrdtEviction");
            var evictionMsg = serializer.DeserializeFromBytes<Models.CrdtEvictionRejectionMessage>(wrapper.Payload!);
            evictionMsg.EvictedReplicaId.ShouldBe("ZombieReplica");
        }
    }

    [IntegrationFact]
    public async Task CrdtP2pPayloadHandler_WhenReceivingEvictionRejection_IgnoresOtherReplicaId_AndRebootsOnlyWhenMatchingLocalReplicaId()
    {
        // Arrange
        var sp = BuildNode("NodeA", services =>
        {
            services.AddDistributedCrdtP2p("TestMesh", "NodeA");
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("NodeA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        var serializer = sp.GetRequiredService<ICrdtSerializer>();
        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh");

        // Act 1 - Receive CrdtEviction targeting a different replica ("NodeB")
        var otherEviction = new Models.CrdtEvictionRejectionMessage("NodeB");
        var otherWrapper = new Models.CrdtMessageWrapper("test-doc", "CrdtEviction", serializer.SerializeToBytes(otherEviction));
        await handler.HandlePayloadAsync("TestMesh", new Ama.Enterprise.P2p.Models.Core.PeerId(Guid.NewGuid()), serializer.SerializeToBytes(otherWrapper), CancellationToken.None);

        // Assert 1 - Local identity is untouched
        replicaContext.ReplicaId.ShouldBe("NodeA");

        // Act 2 - Receive CrdtEviction targeting local replica ("NodeA")
        var selfEviction = new Models.CrdtEvictionRejectionMessage("NodeA");
        var selfWrapper = new Models.CrdtMessageWrapper("test-doc", "CrdtEviction", serializer.SerializeToBytes(selfEviction));
        await handler.HandlePayloadAsync("TestMesh", new Ama.Enterprise.P2p.Models.Core.PeerId(Guid.NewGuid()), serializer.SerializeToBytes(selfWrapper), CancellationToken.None);

        // Assert 2 - Local identity was rebooted with a new GUID suffix
        replicaContext.ReplicaId.ShouldStartWith("NodeA_");
        replicaContext.ReplicaId.ShouldNotBe("NodeA");
    }
}