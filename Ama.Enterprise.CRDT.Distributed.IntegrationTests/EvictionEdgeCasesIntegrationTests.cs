namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Attributes;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.CRDT.Services.Versioning;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

[CrdtAotType(typeof(EvictionEdgeCasesIntegrationTests.TestState))]
public sealed partial class EvictionEdgeCasesTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(EvictionEdgeCasesIntegrationTests.TestState))]
[JsonSerializable(typeof(CrdtDocument<EvictionEdgeCasesIntegrationTests.TestState>))]
public sealed partial class EvictionEdgeCasesTestJsonContext : JsonSerializerContext
{
}

public class EvictionEdgeCasesIntegrationTests
{
    public class TestState
    {
        public string Id { get; set; } = "test-doc";
        public string Data { get; set; } = "initial";
    }

    private IServiceProvider BuildNode(string replicaId, Action<IServiceCollection>? configureExtra = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        
        services.AddDistributedCrdtCore(opt =>
        {
            opt.ReplicaId = replicaId;
            opt.CheckpointIntervalSeconds = 30;
            opt.AntiEntropyIntervalSeconds = 15;
        });

        // Register the AOT contexts for our custom test models to satisfy the AOT pipeline requirements
        services.AddCrdt()
                .AddCrdtAotContext(new EvictionEdgeCasesTestAotContext())
                .AddCrdtJsonTypeInfoResolver(EvictionEdgeCasesTestJsonContext.Default);

        services.AddDistributedDocumentType<TestState>("test-doc");
        services.AddSingleton(Mock.Of<IP2pProtocol>());

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
    public void EdgeCase1_TombstonesAreEphemeral_UnlessPersisted()
    {
        // Arrange - Initial Node Run
        var sp1 = BuildNode("NodeA");
        var tracker1 = sp1.GetRequiredService<IClusterStateTracker>();
        
        tracker1.UpdatePeerState("NodeB", "NetworkB", new DottedVersionVector());
        
        // Act - Tombstone the peer
        tracker1.TombstoneReplica("NodeB");
        
        // Assert - Correctly tombstoned in memory
        tracker1.IsReplicaTombstoned("NodeB").ShouldBeTrue();
        
        // Act - Simulate Node Restart (New DI container, representing process restart)
        // Storage is usually injected/persisted, but tombstones are strictly in-memory (HashSet).
        var sp2 = BuildNode("NodeA");
        var tracker2 = sp2.GetRequiredService<IClusterStateTracker>();
        
        // Assert - Tombstones are correctly cleared on restart unless persisted natively.
        tracker2.IsReplicaTombstoned("NodeB").ShouldBeFalse();
    }

    [IntegrationFact]
    public void EdgeCase2_NetworkDisconnects_PreserveState_ToPreventAmnesia()
    {
        // Arrange
        var sp = BuildNode("NodeA");
        var tracker = sp.GetRequiredService<IClusterStateTracker>();
        var syncService = sp.GetRequiredService<IVersionVectorSyncService>();
        
        var nodeA_Dvv = new DottedVersionVector();
        nodeA_Dvv.Versions["NodeA"] = 5;
        
        var nodeB_Dvv = new DottedVersionVector();
        nodeB_Dvv.Versions["NodeA"] = 2; // Node B is behind
        
        tracker.UpdatePeerState("NodeA", "NetworkA", nodeA_Dvv);
        tracker.UpdatePeerState("NodeB", "NetworkB", nodeB_Dvv);
        
        // Act - Node B disconnects gracefully. TTL is 0 (disabled by default).
        // RemovePeerByNetworkId ONLY removes network routing, keeping the state map for offline recovery.
        tracker.RemovePeerByNetworkId("NetworkB");
        
        var clusterStates = tracker.GetClusterStates();
        var gmvv = syncService.CalculateGlobalMinimumVersionVector(clusterStates);
        
        // Assert - The disconnected peer state is intentionally preserved to mathematically avoid amnesia.
        gmvv["NodeA"].ShouldBe(2);
    }

    [IntegrationFact]
    public async Task EdgeCase3_RebootLocalIdentity_PreservesOfflineLocalData_Safely()
    {
        // Arrange
        var sp = BuildNode("NodeA");
        await StartNodeAsync(sp);

        var scopeProvider = sp.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.CreateDocumentAsync("test-doc", "test-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);
        
        var docManager = orchestrator.GetDocument<TestState>("test-doc")!;
        var evictionService = sp.GetRequiredService<ICrdtEvictionService>();
        
        // Simulate local offline edit by mutating the Document state directly
        var originalDoc = docManager.Document;
        docManager.Document.Data.Data = "Unsaved Offline Edit";
        
        // Act - Receive Eviction Rejection (Cluster tombstoned us, forcing identity reboot)
        await evictionService.RebootLocalIdentityAsync(CancellationToken.None);
        
        // Assert - The document state intentionally preserves offline local data safely
        docManager.Document.Data.Data.ShouldBe("Unsaved Offline Edit");
    }

    [IntegrationFact]
    public async Task EdgeCase4_SnapshotMerge_PreservesPendingLocalEdits_Safely()
    {
        // Arrange
        var mockSerializer = new Mock<ICrdtSerializer>();
        var sp = BuildNode("NodeA", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockSerializer.Object));
        });
        
        await StartNodeAsync(sp);

        var scopeProvider = sp.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.CreateDocumentAsync("test-doc", "test-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docManager = orchestrator.GetDocument<TestState>("test-doc")!;
        var metadataManager = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtMetadataManager>();
        
        // Local node has pending unsynced edits
        docManager.Document.Data.Data = "Local Pending Edit";
        
        var metadata = metadataManager.Initialize(new TestState());
        var snapshotDoc = new CrdtDocument<TestState>(new TestState { Id = "test-doc", Data = "Cluster Snapshot Data" }, metadata);
        
        mockSerializer.Setup(s => s.DeserializeFromBytes<CrdtDocument<TestState>>(It.IsAny<byte[]>()))
            .Returns(snapshotDoc);
            
        // Act - Receive a snapshot message because we fell behind the journal bounds
        await docManager.MergeSnapshotAsync(new byte[] { 1, 2, 3 }, new DottedVersionVector(), CancellationToken.None);
        
        // Assert - The snapshot merge intentionally preserves pending local edits safely
        docManager.Document.Data.Data.ShouldBe("Local Pending Edit");
    }
}