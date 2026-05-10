namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Attributes;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

[CrdtAotType(typeof(MainServicesHappyPathIntegrationTests.HappyPathTestState))]
public sealed partial class HappyPathTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(MainServicesHappyPathIntegrationTests.HappyPathTestState))]
[JsonSerializable(typeof(CrdtDocument<MainServicesHappyPathIntegrationTests.HappyPathTestState>))]
public sealed partial class HappyPathTestJsonContext : JsonSerializerContext
{
}

public sealed class MainServicesHappyPathIntegrationTests
{
    public sealed class HappyPathTestState
    {
        public string Id { get; set; } = "happy-doc";
        public string StateValue { get; set; } = "initial";
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
                .AddCrdtAotContext(new HappyPathTestAotContext())
                .AddCrdtJsonTypeInfoResolver(HappyPathTestJsonContext.Default);

        services.AddDistributedDocumentType<HappyPathTestState>("happy-doc");
        services.AddDistributedCrdtP2p("TestMesh", replicaId);
        
        // Mock P2P Outbound
        services.AddSingleton(Mock.Of<IP2pProtocol>());
        services.AddSingleton(Mock.Of<IDirectMessageSender>());

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
    }

    [IntegrationFact]
    public void ClusterStateTracker_UpdateAndTombstone_HappyPath()
    {
        // Arrange
        var sp = BuildNode("Replica1");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        var remoteDvv = new DottedVersionVector();
        remoteDvv.Versions["RemoteReplica1"] = 10;

        // Act - Track a new peer
        tracker.UpdatePeerState("RemoteReplica1", "NetworkId1", remoteDvv);

        // Assert - Peer is tracked
        var states = tracker.GetClusterStates();
        states.Count.ShouldBe(1);
        states[0].Versions["RemoteReplica1"].ShouldBe(10);
        tracker.IsReplicaTombstoned("RemoteReplica1").ShouldBeFalse();

        // Act - Unmap network (simulate offline)
        tracker.RemovePeerByNetworkId("NetworkId1");

        // Assert - CRDT state is preserved for offline recovery
        tracker.GetClusterStates().Count.ShouldBe(1);

        // Act - Force tombstone
        tracker.TombstoneReplica("RemoteReplica1");

        // Assert - Peer is explicitly tombstoned and state drops
        tracker.GetClusterStates().Count.ShouldBe(0);
        tracker.IsReplicaTombstoned("RemoteReplica1").ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task CrdtEvictionService_EvictsPeers_UpdatesGlobalVersionVector_HappyPath()
    {
        // Arrange
        var sp = BuildNode("Replica1");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);
        
        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        var evictionService = scope.ServiceProvider.GetRequiredService<ICrdtEvictionService>();

        // Set up the local global version vector tracking a remote peer
        context.GlobalVersionVector.Versions["RemoteReplica1"] = 50;

        // Act
        await evictionService.EvictPeersAsync(new[] { "RemoteReplica1" }, CancellationToken.None);

        // Assert - The remote peer's tracked state should be explicitly removed
        context.GlobalVersionVector.Versions.ContainsKey("RemoteReplica1").ShouldBeFalse();
    }

    [IntegrationFact]
    public async Task DistributedCrdtDocument_InitializeAndSnapshot_HappyPath()
    {
        // Arrange
        var mockSender = new Mock<IDirectMessageSender>();
        var sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("happy-doc", "happy-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docManager = orchestrator.GetDocument<HappyPathTestState>("happy-doc")!;

        // Act - Initialize resolves initial state
        await docManager.InitializeAsync(CancellationToken.None);

        docManager.DocumentId.ShouldBe("happy-doc");
        docManager.Document.Data.StateValue.ShouldBe("initial");

        // Act - Ask for snapshot
        await docManager.ProvideSnapshotAsync("RemoteReplica2", new PeerId(Guid.NewGuid()), CancellationToken.None);

        // Assert - Ensure the component sent the payload over direct sender
        mockSender.Verify(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once());
    }

    [IntegrationFact]
    public async Task CrdtInitializationService_Startup_RestoresPersistedState_HappyPath()
    {
        // Arrange
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        var sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockStorage.Object));
        });

        var savedDvv = new DottedVersionVector();
        savedDvv.Versions["Replica1"] = 100;
        
        mockStorage.Setup(s => s.LoadGlobalVersionVectorAsync("Replica1", It.IsAny<CancellationToken>()))
                   .ReturnsAsync(savedDvv);
        
        mockStorage.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(AsyncEnumerable.Empty<JournaledOperation>());

        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();

        // Act
        await initService.StartAsync(CancellationToken.None);

        // Assert - Context replaced safely in-place
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

        context.GlobalVersionVector.Versions["Replica1"].ShouldBe(100);
    }

    [IntegrationFact]
    public async Task CrdtGossipHandler_ProcessesStateSync_HappyPath()
    {
        // Arrange
        var mockSender = new Mock<IDirectMessageSender>();
        var sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("happy-doc", "happy-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh");
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        var remoteDvv = new DottedVersionVector();
        remoteDvv.Versions["RemoteReplica2"] = 15;
        // We pretend the remote already has our local operations mapped to prevent any missing operations sync payload back
        remoteDvv.Versions["Replica1"] = 10;

        var syncMsg = new CrdtStateSyncMessage("RemoteReplica2", remoteDvv);
        var syncPayload = serializer.SerializeToBytes(syncMsg);
        var wrapper = new CrdtMessageWrapper("happy-doc", "CrdtSync", syncPayload);
        var wrapperPayload = serializer.SerializeToBytes(wrapper);

        var senderId = new PeerId(Guid.NewGuid());

        // Act
        await handler.HandlePayloadAsync("TestMesh", senderId, wrapperPayload, CancellationToken.None);

        // Assert
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        var states = tracker.GetClusterStates();
        
        states.Count.ShouldBe(1);
        states[0].Versions["RemoteReplica2"].ShouldBe(15);
        
        // Since we explicitly configured the remote to already possess our changes, it shouldn't have sent any return operations
        mockSender.Verify(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Never());
    }
}