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
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

[CrdtAotType(typeof(AntiEntropyStateSyncIntegrationTests.SyncTestState))]
[CrdtAotType(typeof(Dictionary<string, string>))]
public sealed partial class AntiEntropyStateSyncTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(AntiEntropyStateSyncIntegrationTests.SyncTestState))]
[JsonSerializable(typeof(CrdtDocument<AntiEntropyStateSyncIntegrationTests.SyncTestState>))]
public sealed partial class AntiEntropyStateSyncTestJsonContext : JsonSerializerContext
{
}

public sealed class AntiEntropyStateSyncIntegrationTests
{
    private const string TestMeshId = "SyncMesh";

    public sealed class SyncTestState
    {
        public string Id { get; set; } = "sync-doc";
        public Dictionary<string, string> DataMap { get; set; } = new(StringComparer.Ordinal);
    }

    private IServiceProvider BuildNode(string replicaId, Action<IServiceCollection>? configureExtra = null, bool activeSync = false)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            opt.CheckpointIntervalSeconds = 1;
            opt.AntiEntropyIntervalSeconds = 1;
            opt.AntiEntropyInitialDelaySeconds = 0;
            opt.ActiveSyncEnabled = activeSync;
        });

        services.AddDistributedCrdtReplica(replicaId);

        services.AddCrdt()
                .AddCrdtAotContext(new AntiEntropyStateSyncTestAotContext())
                .AddCrdtJsonTypeInfoResolver(AntiEntropyStateSyncTestJsonContext.Default);

        services.AddDistributedDocumentType<SyncTestState>("sync-doc-1");
        services.AddDistributedDocumentType<SyncTestState>("sync-doc-2");
        services.AddDistributedCrdtP2p(TestMeshId, replicaId);

        services.AddSingleton(Mock.Of<IP2pAlgorithm>());
        services.AddSingleton(Mock.Of<IDirectMessageSender>());
        
        // Anti-Entropy peer resolution explicitly maps internal registries natively
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        services.AddSingleton(new P2pMeshMetadata(TestMeshId));

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
    }

    [IntegrationFact]
    public async Task ProcessStateSyncAsync_ShouldRetrieveMissingOpsOnce_AndBroadcastPerDocument()
    {
        // Arrange
        var mockSender = new Mock<IDirectMessageSender>();
        var capturedDirectSends = new List<ReadOnlyMemory<byte>>();
        mockSender.Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), Capture.In(capturedDirectSends), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher = scope.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        
        await orchestrator.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await orchestrator.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var doc1 = orchestrator.GetDocument<SyncTestState>("sync-doc-1")!;
        var doc2 = orchestrator.GetDocument<SyncTestState>("sync-doc-2")!;

        var intent1 = new MapSetIntent("testKey", "Val1");
        var op1 = await patcher.GenerateOperationAsync(doc1.Document, x => x.DataMap, intent1, CancellationToken.None);

        var intent2 = new MapSetIntent("testKey", "Val2");
        var op2 = await patcher.GenerateOperationAsync(doc2.Document, x => x.DataMap, intent2, CancellationToken.None);

        await doc1.ApplyPatchAsync(new CrdtPatch(new[] { op1 }), CancellationToken.None);
        await doc2.ApplyPatchAsync(new CrdtPatch(new[] { op2 }), CancellationToken.None);

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        var remoteState = new DottedVersionVector();
        var syncMsg = new CrdtStateSyncMessage("RemoteReplica", remoteState);
        var syncBytes = serializer.SerializeToBytes(syncMsg);
        var wrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", syncBytes);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        // Act
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), wrapperBytes, CancellationToken.None);

        // Assert
        capturedDirectSends.ShouldNotBeEmpty();

        var broadcastedDocIds = new HashSet<string>();
        foreach (var payload in capturedDirectSends)
        {
            var msgWrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(payload.ToArray());
            msgWrapper.MessageType.ShouldBe("CrdtOps");
            broadcastedDocIds.Add(msgWrapper.DocumentId);
        }

        broadcastedDocIds.ShouldContain("sync-doc-1");
        broadcastedDocIds.ShouldContain("sync-doc-2");
        broadcastedDocIds.ShouldContain(orchestrator.Registry.DocumentId);
    }

    [IntegrationFact]
    public async Task CrdtTopologyObserver_HandlesPeerJoined_WithoutBroadcastStorms()
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
        await orchestrator.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var observer = sp.GetRequiredKeyedService<IPeerTopologyObserver>(TestMeshId);
        var peerNode = new PeerNode(new PeerId(Guid.NewGuid()), new TcpPeerEndpoint("http://localhost", 5000));
        
        // Map explicitly into registry natively 
        var registry = sp.GetRequiredService<IPeerRegistry>();
        await registry.AddOrUpdatePeerAsync(TestMeshId, peerNode, PeerStatus.Active, CancellationToken.None);

        // Act
        await observer.OnPeerJoinedAsync(TestMeshId, peerNode, CancellationToken.None);
        
        // Assert - Should trigger point-to-point direct message for anti-entropy right away
        mockSender.Verify(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once());
        
        // Act - Trigger again to test thread-safe connection check constraints
        var peerNode2 = new PeerNode(new PeerId(Guid.NewGuid()), new TcpPeerEndpoint("http://localhost2", 5001));
        await registry.AddOrUpdatePeerAsync(TestMeshId, peerNode2, PeerStatus.Active, CancellationToken.None);
        await observer.OnPeerJoinedAsync(TestMeshId, peerNode2, CancellationToken.None);

        // Assert - Still averting overlapping broadcasts
        mockSender.Verify(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once());
    }

    [IntegrationFact]
    public async Task DistributedCrdtDocument_ApplyPatch_ShouldTriggerActiveSync_WhenEnabled()
    {
        // Arrange
        var mockP2p = new Mock<IP2pAlgorithm>();
        var sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockP2p.Object));
        }, activeSync: true);

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher = scope.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var svc in hostedServices)
        {
            await svc.StartAsync(CancellationToken.None);
        }

        var docManager = orchestrator.GetDocument<SyncTestState>("sync-doc-1")!;
        
        // Prepare empty patch to verify it skips
        var patch = new CrdtPatch(Array.Empty<CrdtOperation>());

        // Act
        await docManager.ApplyPatchAsync(patch, CancellationToken.None);

        var intent = new MapSetIntent("Field", "test");
        var op1 = await patcher.GenerateOperationAsync(docManager.Document, x => x.DataMap, intent, CancellationToken.None);
        var populatedPatch = new CrdtPatch(new[] { op1 });

        await docManager.ApplyPatchAsync(populatedPatch, CancellationToken.None);

        await Task.Delay(1000);

        foreach (var svc in hostedServices)
        {
            await svc.StopAsync(CancellationToken.None);
        }

        // Assert - Broadcast triggered correctly
        mockP2p.Verify(p => p.BroadcastAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [IntegrationFact]
    public async Task CrdtGossipHandler_ProcessesStateSync_CalculatesMissingOperations()
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
        await orchestrator.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        var remoteDvv = new DottedVersionVector();
        remoteDvv.Versions["RemoteReplica2"] = 15;
        // Pretend remote already has local ops preventing payload sync back
        remoteDvv.Versions["Replica1"] = 10;

        var syncMsg = new CrdtStateSyncMessage("RemoteReplica2", remoteDvv);
        var syncPayload = serializer.SerializeToBytes(syncMsg);
        var wrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtSync", syncPayload);
        var wrapperPayload = serializer.SerializeToBytes(wrapper);

        // Act
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), wrapperPayload, CancellationToken.None);

        // Assert
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        var states = tracker.GetClusterStates();
        
        states.Count.ShouldBe(1);
        states[0].Versions["RemoteReplica2"].ShouldBe(15);
        
        // Should not send direct payload because remote vector covered our local operations
        mockSender.Verify(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Never());
    }
}