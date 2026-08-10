namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Generic;
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

        context.GlobalVersionVector.Versions["RemoteReplica1"] = 50;

        // Act
        await evictionService.EvictPeersAsync(new[] { "RemoteReplica1" }, CancellationToken.None);

        // Assert
        context.GlobalVersionVector.Versions.ContainsKey("RemoteReplica1").ShouldBeFalse();
    }

    [IntegrationFact]
    public void TombstonesAreEphemeral_UnlessPersisted()
    {
        // Arrange - Initial Node Run
        var sp1 = BuildNode("NodeA");
        var scopeManager1 = sp1.GetRequiredService<DistributedCrdtScopeManager>();
        var scope1 = scopeManager1.GetOrCreateScope("NodeA");
        var tracker1 = scope1.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        
        tracker1.UpdatePeerState("NodeB", "NetworkB", new DottedVersionVector());
        
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
        
        var nodeB_Dvv = new DottedVersionVector();
        nodeB_Dvv.Versions["NodeA"] = 2; // Node B is behind
        
        tracker.UpdatePeerState("NodeA", "NetworkA", nodeA_Dvv);
        tracker.UpdatePeerState("NodeB", "NetworkB", nodeB_Dvv);
        
        // Act
        tracker.RemovePeerByNetworkId("NetworkB");
        
        var clusterStates = tracker.GetClusterStates();
        var gmvv = syncService.CalculateGlobalMinimumVersionVector(clusterStates);
        
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
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);
        
        var docManager = orchestrator.GetDocument<TestState>("test-doc")!;
        var evictionService = scope.ServiceProvider.GetRequiredService<ICrdtEvictionService>();
        
        var originalDoc = docManager.Document;
        docManager.Document.Data.DataMap["Key"] = "Unsaved Offline Edit";
        
        // Act
        await evictionService.RebootLocalIdentityAsync(CancellationToken.None);
        
        // Assert
        docManager.Document.Data.DataMap["Key"].ShouldBe("Unsaved Offline Edit");
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
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docManager = orchestrator.GetDocument<TestState>("test-doc")!;
        var metadataManager = scope.ServiceProvider.GetRequiredService<ICrdtMetadataManager>();
        var serializer = scope.ServiceProvider.GetRequiredService<ICrdtSerializer>();
        var patcher = scope.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        var applicator = scope.ServiceProvider.GetRequiredService<IAsyncCrdtApplicator>();
        
        // Local pending edit (no metadata tracking)
        docManager.Document.Data.DataMap["Key"] = "Local Pending Edit";
        
        var snapshotState = new TestState { Id = "test-doc" };
        var metadata = metadataManager.Initialize(snapshotState);
        var snapshotDoc = new CrdtDocument<TestState>(snapshotState, metadata);
        
        // Advance the CRDT clock on the snapshot to ensure it dominates during the true state merge
        var intent = new MapSetIntent("Key", "Cluster Snapshot Data");
        var op = await patcher.GenerateOperationAsync(snapshotDoc, x => x.DataMap, intent, CancellationToken.None);
        var appliedDoc = await applicator.ApplyPatchAsync(snapshotDoc, new CrdtPatch(new[] { op }));
        
        var snapshotData = serializer.SerializeToBytes(appliedDoc.Document);
            
        // Act
        await docManager.MergeSnapshotAsync(snapshotData, new DottedVersionVector(), CancellationToken.None);
        
        // Assert
        docManager.Document.Data.DataMap["Key"].ShouldBe("Cluster Snapshot Data");
    }
}