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
using Ama.CRDT.Services.Providers;
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
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.CRDT.Distributed.Services.Storage;
using Ama.Enterprise.CRDT.Distributed.Services.Sharding;
using Ama.Enterprise.CRDT.Distributed.Services.AntiEntropy;

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

        services.AddCrdt()
                .AddCrdtAotContext(new AntiEntropyStateSyncTestAotContext())
                .AddCrdtJsonTypeInfoResolver(AntiEntropyStateSyncTestJsonContext.Default);

        services.AddDistributedDocumentType<SyncTestState>("sync-doc-1");
        services.AddDistributedDocumentType<SyncTestState>("sync-doc-2");
        services.AddDistributedCrdtReplica(replicaId, TestMeshId);

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

        await orchestrator.InitializeAsync(CancellationToken.None);
        
        await orchestrator.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await orchestrator.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);

        var intent1 = new MapSetIntent("testKey", "Val1");
        await orchestrator.ApplyIntentAsync<SyncTestState>("sync-doc-1", intent1, "DataMap", CancellationToken.None);

        var intent2 = new MapSetIntent("testKey", "Val2");
        await orchestrator.ApplyIntentAsync<SyncTestState>("sync-doc-2", intent2, "DataMap", CancellationToken.None);

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        var remoteState = new ShardVersionVectorMap();
        remoteState["default"] = new DottedVersionVector();
        remoteState[orchestrator.Registry.DocumentId] = new DottedVersionVector();

        var syncMsg = new CrdtStateSyncMessage("RemoteReplica", remoteState);
        var syncBytes = serializer.SerializeToBytes(syncMsg);
        var wrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", syncBytes, "RemoteReplica");
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        // Act
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), wrapperBytes, CancellationToken.None);

        // Assert
        capturedDirectSends.ShouldNotBeEmpty();

        var broadcastedDocIds = new HashSet<string>();
        foreach (var payload in capturedDirectSends)
        {
            var msgWrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(payload.ToArray());
            msgWrapper.MessageType.ShouldBe("CrdtPatch");
            broadcastedDocIds.Add(msgWrapper.DocumentId);
        }

        broadcastedDocIds.ShouldContain("sync-doc-1");
        broadcastedDocIds.ShouldContain("sync-doc-2");
        broadcastedDocIds.ShouldContain(orchestrator.Registry.DocumentId);
    }

    [IntegrationFact]
    public async Task ProcessStateSyncAsync_ShouldIsolateDifferentShards_WhenSyncing()
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

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        var dvv1 = new DottedVersionVector(); dvv1.Versions["RemoteReplica"] = 10;
        var dvv2 = new DottedVersionVector(); dvv2.Versions["RemoteReplica"] = 20;

        var remoteState = new ShardVersionVectorMap();
        remoteState["shard-1"] = dvv1;
        remoteState["shard-2"] = dvv2;

        var syncMsg = new CrdtStateSyncMessage("RemoteReplica", remoteState);
        var syncBytes = serializer.SerializeToBytes(syncMsg);
        var wrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", syncBytes, "RemoteReplica");
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        // Act
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), wrapperBytes, CancellationToken.None);

        // Assert - Validates shards do not mix their vectors across tracking bounds
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        var states = tracker.GetClusterStates();
        states.Count.ShouldBe(1);
        
        var nodeState = states[0];
        nodeState.ShouldContainKey("shard-1");
        nodeState.ShouldContainKey("shard-2");
        nodeState["shard-1"].Versions["RemoteReplica"].ShouldBe(10);
        nodeState["shard-2"].Versions["RemoteReplica"].ShouldBe(20);
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

        var peerNode = new PeerNode(new PeerId(Guid.NewGuid()), new TcpPeerEndpoint("http://localhost", 5000));
        
        // Act - Adding peer to InMemoryPeerRegistry automatically triggers CrdtTopologyObserver.OnPeerJoinedAsync
        var registry = sp.GetRequiredService<IPeerRegistry>();
        await registry.AddOrUpdatePeerAsync(TestMeshId, peerNode, PeerStatus.Active, CancellationToken.None);
        
        // Assert - Should trigger point-to-point direct message for anti-entropy right away
        mockSender.Verify(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once());
        
        // Act - Add second peer to test thread-safe connection check constraints
        var peerNode2 = new PeerNode(new PeerId(Guid.NewGuid()), new TcpPeerEndpoint("http://localhost2", 5001));
        await registry.AddOrUpdatePeerAsync(TestMeshId, peerNode2, PeerStatus.Active, CancellationToken.None);

        // Assert - Still averting overlapping broadcasts
        mockSender.Verify(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once());
    }

    [IntegrationFact]
    public async Task CrdtTopologyObserver_WhenShardingEnabled_SendsImmediateTopologyAnnouncementsAndDebouncesFullShardSync()
    {
        // Arrange
        var capturedSends = new List<(PeerId Peer, ReadOnlyMemory<byte> Payload)>();
        var mockSender = new Mock<IDirectMessageSender>();
        mockSender
            .Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<PeerId, ReadOnlyMemory<byte>, CancellationToken>((peer, payload, _) =>
            {
                lock (capturedSends)
                {
                    capturedSends.Add((peer, payload));
                }
            })
            .Returns(Task.CompletedTask);

        var sp = BuildNode("Replica1", services =>
        {
            services.Configure<DistributedCrdtOptions>(opt =>
            {
                opt.EnableSharding = true;
                opt.TopologyStabilizationDelayMilliseconds = 250;
            });
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var serializer = sp.GetRequiredService<ICrdtSerializer>();
        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);

        var registry = sp.GetRequiredService<IPeerRegistry>();
        var peer1 = new PeerNode(new PeerId(Guid.NewGuid()), new TcpPeerEndpoint("127.0.0.1", 5001));
        var peer2 = new PeerNode(new PeerId(Guid.NewGuid()), new TcpPeerEndpoint("127.0.0.1", 5002));

        // Act 1 - Two peers join in rapid succession within the 250ms stabilization window
        await registry.AddOrUpdatePeerAsync(TestMeshId, peer1, PeerStatus.Active, CancellationToken.None);
        await registry.AddOrUpdatePeerAsync(TestMeshId, peer2, PeerStatus.Active, CancellationToken.None);

        // Assert 1 - Immediately after joining, only lightweight topology announcements (IsTopologyAnnouncementOnly = true) are sent
        List<(PeerId Peer, CrdtStateSyncMessage Msg)> phase1Messages;
        lock (capturedSends)
        {
            phase1Messages = capturedSends
                .Select(x => (x.Peer, serializer.DeserializeFromBytes<CrdtStateSyncMessage>(
                    serializer.DeserializeFromBytes<CrdtMessageWrapper>(x.Payload.ToArray()).Payload!)))
                .ToList();
        }

        phase1Messages.Count.ShouldBe(2);
        phase1Messages.ShouldAllBe(x => x.Msg.IsTopologyAnnouncementOnly);
        phase1Messages.ShouldAllBe(x => x.Msg.State != null && !x.Msg.State.ContainsKey("default"));

        // Act 2 - Wait for the 250ms stabilization window to elapse
        await Task.Delay(450);

        // Assert 2 - Full data shard state syncs (IsTopologyAnnouncementOnly = false) are now dispatched to both peers
        List<(PeerId Peer, CrdtStateSyncMessage Msg)> phase2Messages;
        lock (capturedSends)
        {
            phase2Messages = capturedSends
                .Skip(2)
                .Select(x => (x.Peer, serializer.DeserializeFromBytes<CrdtStateSyncMessage>(
                    serializer.DeserializeFromBytes<CrdtMessageWrapper>(x.Payload.ToArray()).Payload!)))
                .ToList();
        }

        phase2Messages.Count.ShouldBe(2);
        phase2Messages.ShouldAllBe(x => !x.Msg.IsTopologyAnnouncementOnly);
        phase2Messages.Select(x => x.Peer).ShouldContain(peer1.Id);
        phase2Messages.Select(x => x.Peer).ShouldContain(peer2.Id);
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

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var svc in hostedServices)
        {
            await svc.StartAsync(CancellationToken.None);
        }

        // Prepare empty patch to verify it skips
        var patch = new CrdtPatch(Array.Empty<CrdtOperation>());

        // Act
        await orchestrator.ApplyPatchAsync<SyncTestState>("sync-doc-1", patch, CancellationToken.None);

        var intent = new MapSetIntent("Field", "test");
        await orchestrator.ApplyIntentAsync<SyncTestState>("sync-doc-1", intent, "DataMap", CancellationToken.None);

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

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        var remoteDvv = new DottedVersionVector();
        remoteDvv.Versions["RemoteReplica2"] = 15;
        // Pretend remote already has local ops preventing payload sync back
        remoteDvv.Versions["Replica1"] = 10;

        var remoteState = new ShardVersionVectorMap();
        remoteState[orchestrator.Registry.DocumentId] = remoteDvv;
        remoteState["sync-doc-1"] = remoteDvv;

        var syncMsg = new CrdtStateSyncMessage("RemoteReplica2", remoteState);
        var syncPayload = serializer.SerializeToBytes(syncMsg);
        var wrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtSync", syncPayload, "RemoteReplica2");
        var wrapperPayload = serializer.SerializeToBytes(wrapper);

        // Act
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), wrapperPayload, CancellationToken.None);

        // Assert
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        var states = tracker.GetClusterStates();
        
        states.Count.ShouldBe(1);
        states[0][orchestrator.Registry.DocumentId].Versions["RemoteReplica2"].ShouldBe(15);
        
        // Should not send direct payload because remote vector covered our local operations
        mockSender.Verify(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Never());
    }

    [IntegrationFact]
    public async Task ProcessStateSyncAsync_ShouldIgnoreUninstantiatedRemoteShards_WithoutSendingFalseSnapshots()
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
        await orchestrator.InitializeAsync(CancellationToken.None);

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        // Remote advertises a shard "uninstantiated-shard" that local node does not have active documents for
        var remoteState = new ShardVersionVectorMap
        {
            ["uninstantiated-shard"] = new DottedVersionVector(),
            [orchestrator.Registry.DocumentId] = new DottedVersionVector()
        };

        var syncMsg = new CrdtStateSyncMessage("RemoteReplica", remoteState);
        var syncBytes = serializer.SerializeToBytes(syncMsg);
        var wrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", syncBytes, "RemoteReplica");
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        // Act
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), wrapperBytes, CancellationToken.None);

        // Assert - No snapshots or patches should be sent for the uninstantiated shard
        capturedDirectSends.ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task ProcessProxyRpcPayloadsAsync_ShouldLazilyPromoteLocalDocument_WhenDirectRpcOutracesRegistryGossip()
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
        await orchestrator.InitializeAsync(CancellationToken.None);

        // Document "lazy-rpc-doc" is NOT created in the registry yet
        orchestrator.TryGetLocalDocument<SyncTestState>("lazy-rpc-doc").ShouldBeNull();

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        var intent = new MapSetIntent("RpcKey", "RpcVal");
        var correlationId = Guid.NewGuid();
        var req = new CrdtIntentForwardRequestDto
        {
            CorrelationId = correlationId,
            DocumentId = "lazy-rpc-doc",
            SerializedIntent = serializer.SerializeToBytes<IOperationIntent>(intent),
            IntentTypeAlias = nameof(MapSetIntent),
            TargetProperty = nameof(SyncTestState.DataMap),
            DocumentTypeAlias = "sync-doc-1"
        };

        var reqBytes = serializer.SerializeToBytes(req);
        var wrapper = new CrdtMessageWrapper("lazy-rpc-doc", "CrdtIntentReq", reqBytes, "RemoteReplica", "sync-doc-1");
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        // Act - Send RPC intent before registry gossip arrives
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), wrapperBytes, CancellationToken.None);

        // Assert - Document was lazily promoted and the intent succeeded
        var promotedDoc = orchestrator.TryGetLocalDocument<SyncTestState>("lazy-rpc-doc");
        promotedDoc.ShouldNotBeNull();

        var docState = await promotedDoc!.GetDocumentAsync(CancellationToken.None);
        docState.Data.DataMap.ShouldContainKeyAndValue("RpcKey", "RpcVal");

        capturedDirectSends.Count.ShouldBe(1);
        var resWrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(capturedDirectSends[0].ToArray());
        resWrapper.MessageType.ShouldBe("CrdtIntentRes");
        var resDto = serializer.DeserializeFromBytes<CrdtIntentForwardResponseDto>(resWrapper.Payload!);
        resDto.CorrelationId.ShouldBe(correlationId);
        resDto.IsSuccess.ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task ProcessPatchAsync_ShouldLazilyRegisterUnknownSenderInClusterTracker_AndPromoteDocument_WhenPatchArrivesBeforeStateSync()
    {
        // Arrange
        var sp = BuildNode("Replica1");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        var remoteNodeSp = BuildNode("RemoteGossipReplica");
        var remoteScope = remoteNodeSp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("RemoteGossipReplica");
        var remoteOrch = remoteScope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var remoteStorage = remoteScope.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        await remoteOrch.InitializeAsync(CancellationToken.None);

        await remoteOrch.CreateDocumentAsync("gossip-first-doc", "sync-doc-1", CancellationToken.None);
        await remoteOrch.ApplyIntentAsync<SyncTestState>("gossip-first-doc", new MapSetIntent("FastKey", "FastValue"), nameof(SyncTestState.DataMap), CancellationToken.None);

        var remoteOps = await remoteStorage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        var docOps = remoteOps.Where(o => o.DocumentId == "gossip-first-doc").Select(o => o.Operation).ToArray();
        docOps.ShouldNotBeEmpty();

        var serializer = sp.GetRequiredService<ICrdtSerializer>();
        var patchMsg = new CrdtPatchMessage("RemoteGossipReplica", new CrdtPatch(docOps));
        var patchBytes = serializer.SerializeToBytes(patchMsg);
        var wrapper = new CrdtMessageWrapper("gossip-first-doc", "CrdtPatch", patchBytes, "RemoteGossipReplica", "sync-doc-1");
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var senderPeerId = new PeerId(Guid.NewGuid());

        // Verify Replica1 knows neither the peer nor the document before the patch arrives
        scope.ClusterTracker.GetPeerIdByReplicaId("RemoteGossipReplica").ShouldBeNull();
        orchestrator.TryGetLocalDocument<SyncTestState>("gossip-first-doc").ShouldBeNull();

        // Act - Deliver gossip patch before any CrdtSync or registry update
        await handler.HandlePayloadAsync(TestMeshId, senderPeerId, wrapperBytes, CancellationToken.None);

        // Assert - Sender is registered in ClusterTracker and document is lazily promoted with the patch applied
        scope.ClusterTracker.GetPeerIdByReplicaId("RemoteGossipReplica").ShouldBe(senderPeerId.Value.ToString());
        scope.ClusterTracker.GetReplicaIdByPeerId(senderPeerId.Value.ToString()).ShouldBe("RemoteGossipReplica");

        var promotedDoc = orchestrator.TryGetLocalDocument<SyncTestState>("gossip-first-doc");
        promotedDoc.ShouldNotBeNull();
        var docState = await promotedDoc!.GetDocumentAsync(CancellationToken.None);
        docState.Data.DataMap.ShouldContainKeyAndValue("FastKey", "FastValue");
    }

    [IntegrationFact]
    public async Task DistributedCrdtDocument_ActiveSyncDebounce_ShouldCoalesceRapidMutationsIntoSingleBatchedPatchBroadcast()
    {
        // Arrange
        var mockP2p = new Mock<IP2pAlgorithm>();
        var capturedBroadcasts = new List<ReadOnlyMemory<byte>>();
        mockP2p.Setup(p => p.BroadcastAsync(Capture.In(capturedBroadcasts), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var sp = BuildNode("Replica1", services =>
        {
            services.Configure<DistributedCrdtOptions>(opt =>
            {
                opt.ActiveSyncEnabled = true;
                opt.ActiveSyncDebounceMilliseconds = 150;
            });
            services.Replace(ServiceDescriptor.Singleton(mockP2p.Object));
        }, activeSync: true);

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);

        // Wait for the registry creation debounce timer to flush and clear captured broadcasts
        await Task.Delay(300);
        capturedBroadcasts.Clear();

        // Act - Apply 5 rapid mutations well within the 150ms debounce window
        for (int i = 0; i < 5; i++)
        {
            var intent = new MapSetIntent($"DebounceKey_{i}", $"Val_{i}");
            await orchestrator.ApplyIntentAsync<SyncTestState>("sync-doc-1", intent, nameof(SyncTestState.DataMap), CancellationToken.None);
        }

        // Wait for the single debounced timer callback to fire
        await Task.Delay(350);

        // Assert - All 5 operations were coalesced into a single CrdtPatch broadcast for sync-doc-1
        var docBroadcasts = capturedBroadcasts
            .Select(b => serializer.DeserializeFromBytes<CrdtMessageWrapper>(b.ToArray()))
            .Where(w => w.DocumentId == "sync-doc-1" && w.MessageType == "CrdtPatch")
            .ToList();

        docBroadcasts.Count.ShouldBe(1);
        var batchedPatch = serializer.DeserializeFromBytes<CrdtPatchMessage>(docBroadcasts[0].Payload!);
        batchedPatch.Patch.Operations.Count.ShouldBe(5);
    }

    [IntegrationFact]
    public async Task CrdtAntiEntropyService_AdaptiveStartupWindow_ShouldExecuteRapidCyclesBeforeTransitioningToNormalInterval()
    {
        // Arrange
        var dispatchCount = 0;
        var tcsTwoCycles = new TaskCompletionSource();
        var mockSender = new Mock<IDirectMessageSender>();
        mockSender.Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback(() =>
            {
                if (Interlocked.Increment(ref dispatchCount) >= 2)
                {
                    tcsTwoCycles.TrySetResult();
                }
            })
            .Returns(Task.CompletedTask);

        var sp = BuildNode("Replica1", services =>
        {
            services.Configure<DistributedCrdtOptions>(opt =>
            {
                opt.AntiEntropyInitialDelaySeconds = 0;
                opt.AntiEntropyInitialMaxJitterMilliseconds = 0;
                opt.AntiEntropyStartupDurationSeconds = 10;
                opt.AntiEntropyStartupIntervalSeconds = 1;
                opt.AntiEntropyIntervalSeconds = 60; // Normal interval is very long
            });
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        await scope.Orchestrator.InitializeAsync(CancellationToken.None);

        var peerRegistry = sp.GetRequiredService<IPeerRegistry>();
        var peerNode = new PeerNode(new PeerId(Guid.NewGuid()), new TcpPeerEndpoint("127.0.0.1", 5000));
        await peerRegistry.AddOrUpdatePeerAsync(TestMeshId, peerNode, PeerStatus.Active, CancellationToken.None);

        var antiEntropyService = sp.GetServices<IHostedService>().OfType<CrdtAntiEntropyService>().Single();

        // Act
        await antiEntropyService.StartAsync(CancellationToken.None);
        var completed = await Task.WhenAny(tcsTwoCycles.Task, Task.Delay(3500));
        await antiEntropyService.StopAsync(CancellationToken.None);

        // Assert - At least 2 cycles ran within 3.5s using the 1s startup interval instead of blocking on the 60s normal interval
        completed.ShouldBe(tcsTwoCycles.Task);
        dispatchCount.ShouldBeGreaterThanOrEqualTo(2);
    }

    [IntegrationFact]
    public async Task ProcessProxyRpcPayloadsAsync_ShouldReturnNullPayloadResponse_WhenRemoteReadTargetsNonExistentAndUnregisteredDocument()
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
        await orchestrator.InitializeAsync(CancellationToken.None);

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        var correlationId = Guid.NewGuid();
        var readReq = new CrdtReadRequestDto
        {
            CorrelationId = correlationId,
            DocumentId = "unknown-missing-doc",
            DocumentTypeAlias = "unregistered-type-alias"
        };

        var reqBytes = serializer.SerializeToBytes(readReq);
        var wrapper = new CrdtMessageWrapper("unknown-missing-doc", "CrdtReadReq", reqBytes, "RemoteReplica", "unregistered-type-alias");
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        // Act
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), wrapperBytes, CancellationToken.None);

        // Assert - Responds with CrdtReadRes containing a null SerializedDocumentPayload without throwing
        capturedDirectSends.Count.ShouldBe(1);
        var resWrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(capturedDirectSends[0].ToArray());
        resWrapper.MessageType.ShouldBe("CrdtReadRes");

        var resDto = serializer.DeserializeFromBytes<CrdtReadResponseDto>(resWrapper.Payload!);
        resDto.CorrelationId.ShouldBe(correlationId);
        resDto.StatePayload.ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task ProcessProxyRpcPayloadsAsync_ShouldReturnFailureResponse_WhenIntentForwardRequestHasUnknownIntentTypeAlias()
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
        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        var correlationId = Guid.NewGuid();
        var intentReq = new CrdtIntentForwardRequestDto
        {
            CorrelationId = correlationId,
            DocumentId = "sync-doc-1",
            SerializedIntent = [1, 2, 3],
            IntentTypeAlias = "NonExistentIntentType",
            TargetProperty = nameof(SyncTestState.DataMap),
            DocumentTypeAlias = "sync-doc-1"
        };

        var reqBytes = serializer.SerializeToBytes(intentReq);
        var wrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtIntentReq", reqBytes, "RemoteReplica", "sync-doc-1");
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        // Act
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), wrapperBytes, CancellationToken.None);

        // Assert - Responds with IsSuccess = false instead of crashing the payload handler
        capturedDirectSends.Count.ShouldBe(1);
        var resWrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(capturedDirectSends[0].ToArray());
        resWrapper.MessageType.ShouldBe("CrdtIntentRes");

        var resDto = serializer.DeserializeFromBytes<CrdtIntentForwardResponseDto>(resWrapper.Payload!);
        resDto.CorrelationId.ShouldBe(correlationId);
        resDto.IsSuccess.ShouldBeFalse();
    }

    [IntegrationFact]
    public async Task DuplicateReplicaIdProtection_ShouldRejectCollidingSyncPatchesAndSnapshots_AndProtectLocalDvv()
    {
        // Arrange - Two distinct nodes accidentally configured with the exact same ReplicaId ("Replica1")
        var node1Sp = BuildNode("Replica1");
        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node1Orch.InitializeAsync(CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("Key1", "Node1Val"), nameof(SyncTestState.DataMap), CancellationToken.None);

        var node2Sp = BuildNode("Replica1");
        var node2Scope = node2Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node2Orch = node2Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var node2Storage = node2Scope.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        await node2Orch.InitializeAsync(CancellationToken.None);
        await node2Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);

        // Advance Node 2's clock for "Replica1" far ahead of Node 1 (e.g., 3 mutations -> clock 3 on sync-doc-1)
        await node2Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("CollisionKey1", "Bad1"), nameof(SyncTestState.DataMap), CancellationToken.None);
        await node2Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("CollisionKey2", "Bad2"), nameof(SyncTestState.DataMap), CancellationToken.None);
        await node2Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("CollisionKey3", "Bad3"), nameof(SyncTestState.DataMap), CancellationToken.None);

        var node1Doc = node1Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1")!;
        var initialLocalDvv = node1Doc.GetLocalState()[node1Doc.ShardId];
        var initialLocalVersion = initialLocalDvv.Versions["Replica1"];
        initialLocalVersion.ShouldBe(1);

        var handler = node1Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = node1Sp.GetRequiredService<ICrdtSerializer>();
        var collidingPeerId = new PeerId(Guid.NewGuid());

        // 1. Colliding CrdtSync message from Node 2 claiming ReplicaId = "Replica1"
        var node2GlobalState = await node2Scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>().GetFullSnapshotAsync(CancellationToken.None);
        var syncMsg = new CrdtStateSyncMessage("Replica1", node2GlobalState);
        var syncWrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", serializer.SerializeToBytes(syncMsg), "Replica1");
        await handler.HandlePayloadAsync(TestMeshId, collidingPeerId, serializer.SerializeToBytes(syncWrapper), CancellationToken.None);

        // 2. Colliding CrdtPatch message from Node 2 claiming ReplicaId = "Replica1"
        var node2Ops = await node2Storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        var collidingOps = node2Ops.Where(o => o.DocumentId.ToString().Contains("sync-doc-1", StringComparison.Ordinal)).Select(o => o.Operation).ToArray();
        collidingOps.ShouldNotBeEmpty();

        var patchMsg = new CrdtPatchMessage("Replica1", new CrdtPatch(collidingOps));
        var patchWrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtPatch", serializer.SerializeToBytes(patchMsg), "Replica1", "sync-doc-1");
        await handler.HandlePayloadAsync(TestMeshId, collidingPeerId, serializer.SerializeToBytes(patchWrapper), CancellationToken.None);

        // 3. Colliding CrdtSnapshot message from Node 2 claiming ReplicaId = "Replica1"
        var node2Doc = node2Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1")!;
        var node2Snapshot = await node2Doc.GetSnapshotDataAsync(CancellationToken.None);
        var snapshotMsg = new CrdtSnapshotMessage("Replica1", node2Snapshot.SnapshotData, node2Snapshot.GlobalState);
        var snapshotWrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtSnapshot", serializer.SerializeToBytes(snapshotMsg), "Replica1", "sync-doc-1");
        await handler.HandlePayloadAsync(TestMeshId, collidingPeerId, serializer.SerializeToBytes(snapshotWrapper), CancellationToken.None);

        // Assert - Node 1's ClusterTracker rejected "Replica1" as a remote peer, and Node 1's own DVV for "Replica1" was never corrupted
        node1Scope.ClusterTracker.GetPeerIdByReplicaId("Replica1").ShouldBeNull();
        var afterLocalDvv = node1Doc.GetLocalState()[node1Doc.ShardId];
        afterLocalDvv.Versions["Replica1"].ShouldBe(initialLocalVersion);
        afterLocalDvv.Dots.ContainsKey("Replica1").ShouldBeFalse();

        // Verify Node 1 can still generate and apply its own next sequential operation (Clock 2) without failing
        var applied = await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("Key2", "Node1Val2"), nameof(SyncTestState.DataMap), CancellationToken.None);
        applied.ShouldBeTrue();

        var finalDoc = await node1Doc.GetDocumentAsync(CancellationToken.None);
        finalDoc.Data.DataMap.ShouldContainKeyAndValue("Key1", "Node1Val");
        finalDoc.Data.DataMap.ShouldContainKeyAndValue("Key2", "Node1Val2");
        finalDoc.Data.DataMap.ShouldNotContainKey("CollisionKey1");
        node1Doc.GetLocalState()[node1Doc.ShardId].Versions["Replica1"].ShouldBe(2);
    }

    [IntegrationFact]
    public async Task LocalGossipLoopbackAndRelayedBounceBack_ShouldBeSilentlyIgnored_WithoutFalseCollisionCriticalLogs()
    {
        // Arrange - Configure local P2pNodeOptions with a concrete LocalPeerId so we can test both:
        // 1. GossipAlgorithm.BroadcastAsync local loopback (where senderId == LocalPeerId and patchMsg.ReplicaId == "Replica1")
        // 2. Relayed bounce-back patch (where a remote peer "Replica2" relays an operation originally created by "Replica1")
        var localPeerGuid = Guid.NewGuid();
        var remotePeerGuid = Guid.NewGuid();

        var node1Sp = BuildNode("Replica1", services =>
        {
            services.Configure<P2pNodeOptions>(TestMeshId, opt => opt.LocalPeerId = localPeerGuid);
        });

        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var node1Storage = node1Scope.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        await node1Orch.InitializeAsync(CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("LocalKey", "LocalVal"), nameof(SyncTestState.DataMap), CancellationToken.None);

        var node1Ops = await node1Storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        var ownDocOps = node1Ops.Where(o => o.DocumentId.ToString().Contains("sync-doc-1", StringComparison.Ordinal)).Select(o => o.Operation).ToArray();
        ownDocOps.ShouldNotBeEmpty();

        var handler = node1Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = node1Sp.GetRequiredService<ICrdtSerializer>();

        // Act 1: GossipAlgorithm local loopback (BroadcastAsync dispatches to local state.Dispatcher with localPeerGuid)
        var loopbackPatchMsg = new CrdtPatchMessage("Replica1", new CrdtPatch(ownDocOps));
        var loopbackWrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtPatch", serializer.SerializeToBytes(loopbackPatchMsg), "Replica1", "sync-doc-1");
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(localPeerGuid), serializer.SerializeToBytes(loopbackWrapper), CancellationToken.None);

        // Act 2: Remote peer ("Replica2") relays back a patch containing an operation authored by "Replica1"
        // (e.g. during anti-entropy or multi-hop forwarding)
        var bouncedPatchMsg = new CrdtPatchMessage("Replica2", new CrdtPatch(ownDocOps));
        var bouncedWrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtPatch", serializer.SerializeToBytes(bouncedPatchMsg), "Replica2", "sync-doc-1");
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(remotePeerGuid), serializer.SerializeToBytes(bouncedWrapper), CancellationToken.None);

        // Assert - Local DVV remains at version 1 with no out-of-order dots or corruption, and "Replica1" is never registered as a remote peer
        node1Scope.ClusterTracker.GetPeerIdByReplicaId("Replica1").ShouldBeNull();
        var node1Doc = node1Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1")!;
        var dvv = node1Doc.GetLocalState()[node1Doc.ShardId];
        dvv.Versions["Replica1"].ShouldBe(1);
        dvv.Dots.ContainsKey("Replica1").ShouldBeFalse();
    }

    [IntegrationFact]
    public async Task AmnesiaRecovery_WhenNodeRestartsAndReceivesSnapshot_ShouldRestoreOwnDvvAndStopInfiniteSnapshotLoop()
    {
        // Arrange: Node 2 has a snapshot where Node 1 ("Replica1") was at version 9000 and Node 2 ("Replica2") was at version 10000.
        // Node 1 restarts with amnesia (or loads from an older state where Replica1 is 0) and receives the snapshot from Node 2.
        var capturedDirectSends = new List<ReadOnlyMemory<byte>>();
        var mockSender = new Mock<IDirectMessageSender>();
        mockSender.Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), Capture.In(capturedDirectSends), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var node1Sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
        });
        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node1Orch.InitializeAsync(CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);

        var node2Sp = BuildNode("Replica2", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
        });
        var node2Scope = node2Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica2");
        var node2Orch = node2Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node2Orch.InitializeAsync(CancellationToken.None);
        await node2Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node2Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("RecoveredKey", "RecoveredVal"), nameof(SyncTestState.DataMap), CancellationToken.None);

        var serializer = node2Sp.GetRequiredService<ICrdtSerializer>();
        var node2Doc = node2Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1")!;
        var node2Snapshot = await node2Doc.GetSnapshotDataAsync(CancellationToken.None);

        // Simulate that before restart, Replica1 had reached version 9000 and Replica2 had reached version 10000
        var snapshotDoc = serializer.DeserializeFromBytes<CrdtDocument<SyncTestState>>(node2Snapshot.SnapshotData);
        snapshotDoc.Metadata!.VersionVector["Replica1"] = 9000;
        snapshotDoc.Metadata.VersionVector["Replica2"] = 10000;

        var shardId = node2Doc.ShardId;
        var globalState = new ShardVersionVectorMap(node2Snapshot.GlobalState);
        globalState[shardId].Versions["Replica1"] = 9000;
        globalState[shardId].Versions["Replica2"] = 10000;

        // Also update Node 2's local shard DVV to match (Replica1: 9000, Replica2: 10000)
        var node2Vvm = node2Scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        node2Vvm.MergeShardState(shardId, globalState);

        var snapshotMsg = new CrdtSnapshotMessage("Replica2", serializer.SerializeToBytes(snapshotDoc), globalState);
        var snapshotWrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtSnapshot", serializer.SerializeToBytes(snapshotMsg), "Replica2", "sync-doc-1");

        var node1Handler = node1Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var node2Handler = node2Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);

        // Act 1: Node 1 receives the snapshot from Node 2
        await node1Handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), serializer.SerializeToBytes(snapshotWrapper), CancellationToken.None);

        // Assert 1: Node 1's local DVV and document metadata must have restored "Replica1" to 9000 (NOT stripped it to 0!)
        var node1Doc = node1Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1")!;
        var node1LocalDvv = node1Doc.GetLocalState()[shardId];
        node1LocalDvv.Versions.ShouldContainKeyAndValue("Replica1", 9000);
        node1LocalDvv.Versions.ShouldContainKeyAndValue("Replica2", 10000);

        var node1Materialized = await node1Doc.GetDocumentAsync(CancellationToken.None);
        node1Materialized.Metadata!.VersionVector.ShouldContainKeyAndValue("Replica1", 9000);
        node1Materialized.Metadata.VersionVector.ShouldContainKeyAndValue("Replica2", 10000);

        // Act 2: Node 1 now sends a CrdtSync message to Node 2 with its current state
        capturedDirectSends.Clear();
        var node1FullState = await node1Scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>().GetFullSnapshotAsync(CancellationToken.None);
        // Ensure registry shard is also aligned so we only test the document shard
        var node2FullState = await node2Vvm.GetFullSnapshotAsync(CancellationToken.None);
        node1FullState[node2Orch.Registry.DocumentId] = node2FullState[node2Orch.Registry.DocumentId];

        var syncFromNode1 = new CrdtStateSyncMessage("Replica1", node1FullState);
        var syncWrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", serializer.SerializeToBytes(syncFromNode1), "Replica1");
        await node2Handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), serializer.SerializeToBytes(syncWrapper), CancellationToken.None);

        // Assert 2: Node 2 sees that Node 1 is at (Replica1: 9000, Replica2: 10000) and does NOT resend any snapshot or patch!
        capturedDirectSends.ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task ProcessPatchAsync_WhenCompoundPatchContainsMixedLocalAndRemoteOperations_FiltersOnlyLocalOperationsAndAppliesRemoteOnes()
    {
        // Arrange
        var node1Sp = BuildNode("Replica1");
        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var node1Storage = node1Scope.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        await node1Orch.InitializeAsync(CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("OwnKey", "OwnVal"), nameof(SyncTestState.DataMap), CancellationToken.None);

        var node2Sp = BuildNode("Replica2");
        var node2Scope = node2Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica2");
        var node2Orch = node2Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var node2Storage = node2Scope.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        await node2Orch.InitializeAsync(CancellationToken.None);
        await node2Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node2Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("RemoteKey1", "RemoteVal1"), nameof(SyncTestState.DataMap), CancellationToken.None);
        await node2Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("RemoteKey2", "RemoteVal2"), nameof(SyncTestState.DataMap), CancellationToken.None);

        var ownOp = (await node1Storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync())
            .First(o => o.DocumentId.ToString().Contains("sync-doc-1", StringComparison.Ordinal)).Operation;

        var remoteOps = (await node2Storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync())
            .Where(o => o.DocumentId.ToString().Contains("sync-doc-1", StringComparison.Ordinal))
            .OrderBy(o => o.Operation.ShardClock)
            .Select(o => o.Operation)
            .ToArray();

        // Build a mixed patch where a bounced-back Replica1 op is interleaved between two valid Replica2 ops
        var mixedPatch = new CrdtPatch([remoteOps[0], ownOp with { Id = Guid.NewGuid(), ShardClock = 999 }, remoteOps[1]]);

        var serializer = node1Sp.GetRequiredService<ICrdtSerializer>();
        var patchMsg = new CrdtPatchMessage("Replica2", mixedPatch);
        var wrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtPatch", serializer.SerializeToBytes(patchMsg), "Replica2", "sync-doc-1");
        var handler = node1Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);

        // Act
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), serializer.SerializeToBytes(wrapper), CancellationToken.None);

        // Assert - Both Replica2 operations were applied, while the bounced Replica1 operation was stripped without corrupting Replica1's DVV
        var node1Doc = node1Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1")!;
        var state = await node1Doc.GetDocumentAsync(CancellationToken.None);
        state.Data.DataMap.ShouldContainKeyAndValue("OwnKey", "OwnVal");
        state.Data.DataMap.ShouldContainKeyAndValue("RemoteKey1", "RemoteVal1");
        state.Data.DataMap.ShouldContainKeyAndValue("RemoteKey2", "RemoteVal2");

        var dvv = node1Doc.GetLocalState()[node1Doc.ShardId];
        dvv.Versions["Replica1"].ShouldBe(1);
        dvv.Dots.ContainsKey("Replica1").ShouldBeFalse();
        dvv.Versions["Replica2"].ShouldBe(2);
    }

    [IntegrationFact]
    public async Task ProcessProxyRpcPayloadsAsync_WhenReadRequestTargetsLocalShardWithUncreatedDocument_LazilyPromotesAndReturnsMaterializedState()
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
        await orchestrator.InitializeAsync(CancellationToken.None);

        // Pre-populate storage with a document checkpoint before any registry entry or in-memory instance exists
        var metadataManager = scope.ServiceProvider.GetRequiredService<Ama.CRDT.Services.ICrdtMetadataManager>();
        var preStoredState = new SyncTestState { Id = "lazy-read-doc" };
        preStoredState.DataMap["PreStoredKey"] = "PreStoredVal";
        var preStoredDoc = new CrdtDocument<SyncTestState>(preStoredState, metadataManager.Initialize(preStoredState));
        await scope.Storage.SaveDocumentAsync(
            "default",
            CrdtRegistryEntry.GetRegistryKey("sync-doc-1", "lazy-read-doc"),
            preStoredDoc,
            CancellationToken.None);

        orchestrator.TryGetLocalDocument<SyncTestState>("lazy-read-doc").ShouldBeNull();

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        var correlationId = Guid.NewGuid();
        var readReq = new CrdtReadRequestDto
        {
            CorrelationId = correlationId,
            DocumentId = "lazy-read-doc",
            DocumentTypeAlias = "sync-doc-1"
        };

        var wrapper = new CrdtMessageWrapper("lazy-read-doc", "CrdtReadReq", serializer.SerializeToBytes(readReq), "RemoteReplica", "sync-doc-1");

        // Act - Remote node sends a CrdtReadReq for a document that is registered by type alias and owned by a local shard, but not yet promoted
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), serializer.SerializeToBytes(wrapper), CancellationToken.None);

        // Assert - Document was lazily promoted, loaded from storage, and returned in CrdtReadRes
        orchestrator.TryGetLocalDocument<SyncTestState>("lazy-read-doc").ShouldNotBeNull();

        capturedDirectSends.Count.ShouldBe(1);
        var resWrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(capturedDirectSends[0].ToArray());
        resWrapper.MessageType.ShouldBe("CrdtReadRes");

        var resDto = serializer.DeserializeFromBytes<CrdtReadResponseDto>(resWrapper.Payload!);
        resDto.CorrelationId.ShouldBe(correlationId);
        resDto.StatePayload.ShouldNotBeEmpty();

        var returnedDoc = serializer.DeserializeFromBytes<CrdtDocument<SyncTestState>>(resDto.StatePayload!);
        returnedDoc.Data.DataMap.ShouldContainKeyAndValue("PreStoredKey", "PreStoredVal");
    }

    [IntegrationFact]
    public async Task LargeBurstWithTrimmedJournal_WhenMultiDocumentShardAndDotsExist_ShouldSendSnapshotOnceAndNotLoop()
    {
        // Arrange: Node 1 has multiple documents in the same shard ("default"), performs a large burst of changes,
        // has non-contiguous dots in its shard DVV, and its journal is completely trimmed.
        var node1ToNode2Payloads = new List<ReadOnlyMemory<byte>>();
        var mockNode1Sender = new Mock<IDirectMessageSender>();
        mockNode1Sender.Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), Capture.In(node1ToNode2Payloads), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var node1Sp = BuildNode("Replica1", services =>
        {
            services.Configure<DistributedCrdtOptions>(o => {
                o.AntiEntropyStartupIntervalSeconds = 1;
                o.AntiEntropyMaxAdaptiveIntervalSeconds = 1;
            });
            services.Replace(ServiceDescriptor.Singleton(mockNode1Sender.Object));
        });
        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node1Orch.InitializeAsync(CancellationToken.None);

        await node1Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);

        for (int i = 0; i < 25; i++)
        {
            await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent($"K1_{i}", $"V1_{i}"), nameof(SyncTestState.DataMap), CancellationToken.None);
        }

        // Inject a non-contiguous dot into Node 1's shard DVV (simulating an out-of-order gossip op before journal trim)
        var node1Vvm = node1Scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        var dotMap = new ShardVersionVectorMap
        {
            ["default"] = new DottedVersionVector(
                new Dictionary<string, long> { ["Replica1"] = 25 },
                new Dictionary<string, ISet<long>> { ["Replica3"] = new HashSet<long> { 5 } })
        };
        node1Vvm.MergeShardState("default", dotMap);

        // Trim Node 1's journal completely up to version 25 so a lagging peer requires a snapshot
        await node1Scope.Storage.TrimAsync("default", new Dictionary<string, long> { ["Replica1"] = 25, ["Replica3"] = 5 }, CancellationToken.None);
        await node1Scope.Storage.TrimAsync(node1Orch.Registry.ShardId, new Dictionary<string, long> { ["Replica1"] = 10 }, CancellationToken.None);

        var node2Sp = BuildNode("Replica2");
        var node2Scope = node2Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica2");
        var node2Orch = node2Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node2Orch.InitializeAsync(CancellationToken.None);
        // Node 2 already knows both documents exist in the shard (e.g. via registry gossip)
        await node2Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node2Orch.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);

        var node1Handler = node1Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var node2Handler = node2Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = node1Sp.GetRequiredService<ICrdtSerializer>();
        var node2PeerId = new PeerId(Guid.NewGuid());
        var node1PeerId = new PeerId(Guid.NewGuid());

        // Act 1: Node 2 sends initial CrdtSync (behind on "default" shard)
        var node2InitialState = await node2Scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>().GetFullSnapshotAsync(CancellationToken.None);
        var syncMsg1 = new CrdtStateSyncMessage("Replica2", node2InitialState);
        var syncWrapper1 = new CrdtMessageWrapper("Cluster", "CrdtSync", serializer.SerializeToBytes(syncMsg1), "Replica2");
        await node1Handler.HandlePayloadAsync(TestMeshId, node2PeerId, serializer.SerializeToBytes(syncWrapper1), CancellationToken.None);

        var firstBatchPayloads = node1ToNode2Payloads.ToList();
        firstBatchPayloads.ShouldNotBeEmpty();

        // Act 2: Before Node 2 even finishes applying the snapshots, another CrdtSync arrives at Node 1 within the cooldown window
        node1ToNode2Payloads.Clear();
        await node1Handler.HandlePayloadAsync(TestMeshId, node2PeerId, serializer.SerializeToBytes(syncWrapper1), CancellationToken.None);

        // Assert 2: Node 1 suppresses duplicate snapshot transmission during the in-flight cooldown window
        node1ToNode2Payloads.ShouldBeEmpty();

        // Act 3: Deliver the first batch of snapshots to Node 2
        foreach (var payload in firstBatchPayloads)
        {
            await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, payload, CancellationToken.None);
        }

        // Assert 3: Node 2's shard DVV has converged (including the non-contiguous dot for Replica3)
        var node2UpdatedState = await node2Scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>().GetFullSnapshotAsync(CancellationToken.None);
        node2UpdatedState["default"].Versions.ShouldContainKeyAndValue("Replica1", 25);
        node2UpdatedState["default"].Dots.ShouldContainKey("Replica3");
        node2UpdatedState["default"].Dots["Replica3"].ShouldContain(5);

        // Act 4: Node 2 now sends a new CrdtSync with its updated DVV to Node 1
        node1ToNode2Payloads.Clear();
        var syncMsg2 = new CrdtStateSyncMessage("Replica2", node2UpdatedState);
        var syncWrapper2 = new CrdtMessageWrapper("Cluster", "CrdtSync", serializer.SerializeToBytes(syncMsg2), "Replica2");
        await node1Handler.HandlePayloadAsync(TestMeshId, node2PeerId, serializer.SerializeToBytes(syncWrapper2), CancellationToken.None);

        // Assert 4: Node 1 sees Node 2 is completely caught up and sends zero snapshots or patches
        node1ToNode2Payloads.ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task EvaluateAndDispatchAntiEntropyStateAsync_WhenPipelineQueueExceedsBusyThreshold_SkipsSyncAndReportsWorkload()
    {
        // Arrange
        var mockSender = new Mock<IDirectMessageSender>();
        var sp = BuildNode("Replica1", services =>
        {
            services.Configure<DistributedCrdtOptions>(opt =>
            {
                opt.AntiEntropyBusyQueueThreshold = 1;
                opt.AntiEntropyAdaptiveDelayPerThousandItemsMilliseconds = 1000;
            });
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);

        var peerRegistry = sp.GetRequiredService<IPeerRegistry>();
        var peerId = new PeerId(Guid.NewGuid());
        var peerNode = new PeerNode(peerId, new TcpPeerEndpoint("127.0.0.1", 5000));
        await peerRegistry.AddOrUpdatePeerAsync(TestMeshId, peerNode, PeerStatus.Active, CancellationToken.None);

        // Simulate a lagging peer that is 2500 operations behind on "default" shard
        var localVvm = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        localVvm.MergeShardState("default", new ShardVersionVectorMap
        {
            ["default"] = new DottedVersionVector(new Dictionary<string, long> { ["Replica1"] = 2500 }, new Dictionary<string, ISet<long>>())
        });
        scope.ClusterTracker.UpdatePeerState("Replica2", peerId.Value.ToString(), new ShardVersionVectorMap
        {
            ["default"] = new DottedVersionVector(new Dictionary<string, long> { ["Replica1"] = 0 }, new Dictionary<string, ISet<long>>())
        });

        // Wait for any fire-and-forget topology re-evaluation (and any demotion snapshot push) triggered by UpdatePeerState to drain
        await orchestrator.FlushAsync(CancellationToken.None);
        mockSender.Invocations.Clear();

        // Acquire anti-entropy work scope to simulate an in-flight sync operation
        var concreteOrch = (CrdtDocumentOrchestrator)orchestrator;
        using var heldScope = concreteOrch.TryAcquireAntiEntropyWorkScope();
        heldScope.ShouldNotBeNull();

        // Act - Attempt to evaluate and dispatch while already working
        var busyWorkload = await orchestrator.EvaluateAndDispatchAntiEntropyStateAsync(CancellationToken.None);

        // Assert - Skipped network dispatch while busy and accurately reported the 2500 estimated items
        busyWorkload.IsBusy.ShouldBeTrue();
        busyWorkload.EstimatedPendingSyncItems.ShouldBeGreaterThanOrEqualTo(2500);
        mockSender.Verify(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Never());

        // Release work scope
        heldScope!.Dispose();
        await orchestrator.FlushAsync(CancellationToken.None);

        // Act 2 - Evaluate again now that the node is idle
        var idleWorkload = await orchestrator.EvaluateAndDispatchAntiEntropyStateAsync(CancellationToken.None);

        // Assert 2 - Dispatched anti-entropy sync now that the node is no longer busy
        idleWorkload.IsBusy.ShouldBeFalse();
        idleWorkload.EstimatedPendingSyncItems.ShouldBeGreaterThanOrEqualTo(2500);
        mockSender.Verify(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once());
    }

    [IntegrationFact]
    public async Task SnapshotBatchWithDroppedMessage_ShouldHoldShardDvvAdvanceUntilTimeout_AndRecoverMissingDocumentOnNextSyncCycle()
    {
        // Arrange: Node 1 has two documents in the "default" shard ("sync-doc-1" and "sync-doc-2") and a trimmed journal.
        // Node 2 is behind and requests sync. Node 1 sends snapshots for both documents, but "sync-doc-2"'s snapshot is dropped in transit.
        var node1ToNode2Payloads = new List<ReadOnlyMemory<byte>>();
        var mockNode1Sender = new Mock<IDirectMessageSender>();
        mockNode1Sender.Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), Capture.In(node1ToNode2Payloads), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var node1Sp = BuildNode("Replica1", services =>
        {
            services.Configure<DistributedCrdtOptions>(o =>
            {
                o.AntiEntropyStartupIntervalSeconds = 1;
                o.AntiEntropyMaxAdaptiveIntervalSeconds = 1;
            });
            services.Replace(ServiceDescriptor.Singleton(mockNode1Sender.Object));
        });
        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node1Orch.InitializeAsync(CancellationToken.None);

        await node1Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);

        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("Doc1Key", "Doc1Val"), nameof(SyncTestState.DataMap), CancellationToken.None);
        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-2", new MapSetIntent("Doc2Key", "Doc2Val"), nameof(SyncTestState.DataMap), CancellationToken.None);

        // Trim Node 1's journal completely up to version 2 so Node 2 must recover via snapshots
        await node1Scope.Storage.TrimAsync("default", new Dictionary<string, long> { ["Replica1"] = 2 }, CancellationToken.None);
        await node1Scope.Storage.TrimAsync(node1Orch.Registry.ShardId, new Dictionary<string, long> { ["Replica1"] = 10 }, CancellationToken.None);

        var node2Sp = BuildNode("Replica2");
        var node2Scope = node2Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica2");
        var node2Orch = node2Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var node2Vvm = node2Scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await node2Orch.InitializeAsync(CancellationToken.None);
        await node2Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node2Orch.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);

        var node1Handler = node1Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var node2Handler = node2Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = node1Sp.GetRequiredService<ICrdtSerializer>();
        var node1PeerId = new PeerId(Guid.NewGuid());
        var node2PeerId = new PeerId(Guid.NewGuid());

        // Act 1: Node 2 sends CrdtSync to Node 1. Node 1 dispatches snapshots for the registry, sync-doc-1, and sync-doc-2.
        var node2InitialState = await node2Vvm.GetFullSnapshotAsync(CancellationToken.None);
        var syncMsg1 = new CrdtStateSyncMessage("Replica2", node2InitialState);
        var syncWrapper1 = new CrdtMessageWrapper("Cluster", "CrdtSync", serializer.SerializeToBytes(syncMsg1), "Replica2");
        await node1Handler.HandlePayloadAsync(TestMeshId, node2PeerId, serializer.SerializeToBytes(syncWrapper1), CancellationToken.None);

        var initialBatch = node1ToNode2Payloads.ToList();
        initialBatch.ShouldNotBeEmpty();

        // Simulate packet loss: Drop the CrdtSnapshot message for "sync-doc-2" and deliver everything else
        var droppedDoc2Count = 0;
        foreach (var payload in initialBatch)
        {
            var wrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(payload.ToArray());
            if (wrapper.DocumentId == "sync-doc-2" && wrapper.MessageType == "CrdtSnapshot")
            {
                droppedDoc2Count++;
                continue;
            }

            await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, payload, CancellationToken.None);
        }

        droppedDoc2Count.ShouldBe(1);

        // Assert 1: Node 2 applied sync-doc-1's state, but because sync-doc-2's snapshot was missed:
        // - The multi-document shard barrier did NOT prematurely advance Node 2's "default" shard DVV to Replica1 = 2.
        // - TryGetActiveSnapshotReceptionState reports that 1 expected document is still missing.
        var doc1OnNode2 = await node2Orch.GetDocumentStateAsync<SyncTestState>("sync-doc-1", CancellationToken.None);
        doc1OnNode2.Data.DataMap.ShouldContainKeyAndValue("Doc1Key", "Doc1Val");

        var doc2OnNode2BeforeRecovery = await node2Orch.GetDocumentStateAsync<SyncTestState>("sync-doc-2", CancellationToken.None);
        doc2OnNode2BeforeRecovery.Data.DataMap.ShouldNotContainKey("Doc2Key");

        var isReceiving = node2Vvm.TryGetActiveSnapshotReceptionState(5000, out var remainingDocs);
        isReceiving.ShouldBeTrue();
        remainingDocs.ShouldBe(1);

        var node2MidState = await node2Vvm.GetFullSnapshotAsync(CancellationToken.None);
        node2MidState["default"].Versions.ContainsKey("Replica1").ShouldBeFalse();

        // Act 2: Wait for the stale snapshot reception window and sender cooldown to expire, then run the next anti-entropy cycle
        await Task.Delay(750);
        node2Vvm.TryGetActiveSnapshotReceptionState(500, out var remainingAfterTimeout).ShouldBeFalse();
        remainingAfterTimeout.ShouldBe(0);

        // Expire sender's per-peer snapshot cooldown by using a reconnected PeerId (or after cooldown expiry)
        var node2ReconnectedPeerId = new PeerId(Guid.NewGuid());
        node1ToNode2Payloads.Clear();

        var node2RetryState = await node2Vvm.GetFullSnapshotAsync(CancellationToken.None);
        var syncMsg2 = new CrdtStateSyncMessage("Replica2", node2RetryState);
        var syncWrapper2 = new CrdtMessageWrapper("Cluster", "CrdtSync", serializer.SerializeToBytes(syncMsg2), "Replica2");
        await node1Handler.HandlePayloadAsync(TestMeshId, node2ReconnectedPeerId, serializer.SerializeToBytes(syncWrapper2), CancellationToken.None);

        // Assert 2: Because Node 2 never falsely advanced its "default" shard DVV, Node 1 detects the gap and resends the shard snapshots
        var recoveryBatch = node1ToNode2Payloads.ToList();
        recoveryBatch.ShouldNotBeEmpty();

        foreach (var payload in recoveryBatch)
        {
            await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, payload, CancellationToken.None);
        }

        // Assert 3: Node 2 now has the missed document ("sync-doc-2") AND its shard DVV has advanced to Replica1 = 2
        var doc2OnNode2AfterRecovery = await node2Orch.GetDocumentStateAsync<SyncTestState>("sync-doc-2", CancellationToken.None);
        doc2OnNode2AfterRecovery.Data.DataMap.ShouldContainKeyAndValue("Doc2Key", "Doc2Val");

        var node2FinalState = await node2Vvm.GetFullSnapshotAsync(CancellationToken.None);
        node2FinalState["default"].Versions.ShouldContainKeyAndValue("Replica1", 2);

        // A subsequent sync results in zero messages because convergence is complete
        node1ToNode2Payloads.Clear();
        var syncMsg3 = new CrdtStateSyncMessage("Replica2", node2FinalState);
        var syncWrapper3 = new CrdtMessageWrapper("Cluster", "CrdtSync", serializer.SerializeToBytes(syncMsg3), "Replica2");
        await node1Handler.HandlePayloadAsync(TestMeshId, node2ReconnectedPeerId, serializer.SerializeToBytes(syncWrapper3), CancellationToken.None);
        node1ToNode2Payloads.ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task SnapshotMidFlightMutation_WhenSenderMutatesShardDuringSnapshotTransfer_RecipientRecoversMissedDeltaOperationsOnNextSync()
    {
        // Arrange: Node 1 has two documents in "default" shard at clock 1..2 (journal trimmed up to 2).
        // Node 1 takes a snapshot of sync-doc-1 (at clock 2). While the snapshot transfer is happening,
        // a new write (clock 3) is applied to sync-doc-1 on Node 1 (which remains in the live journal!).
        // Then Node 1 takes a snapshot of sync-doc-2 (whose GlobalState now carries Replica1 = 3, even though sync-doc-1's snapshot only had up to clock 2).
        var node1ToNode2Payloads = new List<ReadOnlyMemory<byte>>();
        var mockNode1Sender = new Mock<IDirectMessageSender>();
        mockNode1Sender.Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), Capture.In(node1ToNode2Payloads), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var node1Sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockNode1Sender.Object));
        });
        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node1Orch.InitializeAsync(CancellationToken.None);

        await node1Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);

        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("InitialKey1", "InitialVal1"), nameof(SyncTestState.DataMap), CancellationToken.None);
        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-2", new MapSetIntent("InitialKey2", "InitialVal2"), nameof(SyncTestState.DataMap), CancellationToken.None);

        // Trim operations 1 and 2 so snapshots are required for the baseline state
        await node1Scope.Storage.TrimAsync("default", new Dictionary<string, long> { ["Replica1"] = 2 }, CancellationToken.None);

        var node2Sp = BuildNode("Replica2");
        var node2Scope = node2Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica2");
        var node2Orch = node2Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var node2Vvm = node2Scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await node2Orch.InitializeAsync(CancellationToken.None);
        await node2Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node2Orch.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);

        var node1Doc1 = node1Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1")!;
        var node1Doc2 = node1Orch.TryGetLocalDocument<SyncTestState>("sync-doc-2")!;
        var serializer = node1Sp.GetRequiredService<ICrdtSerializer>();
        var node1Handler = node1Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var node2Handler = node2Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var node1PeerId = new PeerId(Guid.NewGuid());
        var node2PeerId = new PeerId(Guid.NewGuid());

        // Step 1: Capture snapshot of sync-doc-1 BEFORE the concurrent mutation (GlobalState has Replica1 = 2)
        var snap1 = await node1Doc1.GetSnapshotDataAsync(CancellationToken.None);
        snap1.GlobalState["default"].Versions["Replica1"].ShouldBe(2);

        // Step 2: Concurrent mutation occurs on sync-doc-1 while snapshots are being generated (advances shard clock to 3, journaled at clock 3)
        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("MidFlightKey", "MidFlightVal"), nameof(SyncTestState.DataMap), CancellationToken.None);

        // Step 3: Capture snapshot of sync-doc-2 AFTER the concurrent mutation on sync-doc-1 (GlobalState now claims Replica1 = 3!)
        var snap2 = await node1Doc2.GetSnapshotDataAsync(CancellationToken.None);
        snap2.GlobalState["default"].Versions["Replica1"].ShouldBe(3);

        var expectedShardDocs = new List<string>
        {
            CrdtRegistryEntry.GetRegistryKey("sync-doc-1", "sync-doc-1"),
            CrdtRegistryEntry.GetRegistryKey("sync-doc-2", "sync-doc-2")
        };
        var snapMsg1 = new CrdtSnapshotMessage("Replica1", snap1.SnapshotData, snap1.GlobalState, expectedShardDocs);
        var snapWrapper1 = new CrdtMessageWrapper("sync-doc-1", "CrdtSnapshot", serializer.SerializeToBytes(snapMsg1), "Replica1", "sync-doc-1");

        var snapMsg2 = new CrdtSnapshotMessage("Replica1", snap2.SnapshotData, snap2.GlobalState, expectedShardDocs);
        var snapWrapper2 = new CrdtMessageWrapper("sync-doc-2", "CrdtSnapshot", serializer.SerializeToBytes(snapMsg2), "Replica1", "sync-doc-2");

        // Act 1: Deliver both skewed snapshots to Node 2
        await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, serializer.SerializeToBytes(snapWrapper1), CancellationToken.None);
        await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, serializer.SerializeToBytes(snapWrapper2), CancellationToken.None);

        // Assert 1: ShardVersionVectorManager computed the safe floor (GMVV = min(2, 3) = 2) across the batch
        // rather than adopting the optimistic 3 from snap2, which would have caused Node 2 to permanently miss clock 3 on sync-doc-1!
        var node2StateAfterSnapshots = await node2Vvm.GetFullSnapshotAsync(CancellationToken.None);
        node2StateAfterSnapshots["default"].Versions["Replica1"].ShouldBe(2);

        var doc1BeforeDelta = await node2Orch.GetDocumentStateAsync<SyncTestState>("sync-doc-1", CancellationToken.None);
        doc1BeforeDelta.Data.DataMap.ShouldContainKeyAndValue("InitialKey1", "InitialVal1");
        doc1BeforeDelta.Data.DataMap.ShouldNotContainKey("MidFlightKey");

        // Act 2: Node 2 sends a CrdtSync to Node 1 advertising its safe floor DVV (Replica1 = 2)
        node1ToNode2Payloads.Clear();
        // Align registry DVV in the sync payload so we only inspect the "default" shard delta
        var node1FullState = await node1Scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>().GetFullSnapshotAsync(CancellationToken.None);
        node2StateAfterSnapshots[node1Orch.Registry.ShardId] = node1FullState[node1Orch.Registry.ShardId];

        var syncMsg = new CrdtStateSyncMessage("Replica2", node2StateAfterSnapshots);
        var syncWrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", serializer.SerializeToBytes(syncMsg), "Replica2");
        await node1Handler.HandlePayloadAsync(TestMeshId, node2PeerId, serializer.SerializeToBytes(syncWrapper), CancellationToken.None);

        // Assert 2: Node 1 sees Node 2 is at clock 2 and sends the missing clock 3 operation as a lightweight CrdtPatch (not a full snapshot!)
        node1ToNode2Payloads.Count.ShouldBe(1);
        var deltaWrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(node1ToNode2Payloads[0].ToArray());
        deltaWrapper.MessageType.ShouldBe("CrdtPatch");
        deltaWrapper.DocumentId.ShouldBe("sync-doc-1");

        // Deliver the delta patch to Node 2
        await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, node1ToNode2Payloads[0], CancellationToken.None);

        // Assert 3: Node 2 now has the mid-flight item ("MidFlightKey") and its shard DVV has converged to 3
        var doc1AfterDelta = await node2Orch.GetDocumentStateAsync<SyncTestState>("sync-doc-1", CancellationToken.None);
        doc1AfterDelta.Data.DataMap.ShouldContainKeyAndValue("InitialKey1", "InitialVal1");
        doc1AfterDelta.Data.DataMap.ShouldContainKeyAndValue("MidFlightKey", "MidFlightVal");

        var node2FinalState = await node2Vvm.GetFullSnapshotAsync(CancellationToken.None);
        node2FinalState["default"].Versions["Replica1"].ShouldBe(3);
    }

    [IntegrationFact]
    public async Task SnapshotBatchHappyPath_ShouldSuppressRecipientAntiEntropyWhileInFlight_AndConvergeOnceAllDocumentsArrive()
    {
        // Arrange: Node 1 has two documents in the "default" shard and a trimmed journal.
        var node1ToNode2Payloads = new List<ReadOnlyMemory<byte>>();
        var mockNode1Sender = new Mock<IDirectMessageSender>();
        mockNode1Sender.Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), Capture.In(node1ToNode2Payloads), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var node2ToNode1Payloads = new List<ReadOnlyMemory<byte>>();
        var mockNode2Sender = new Mock<IDirectMessageSender>();
        mockNode2Sender.Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), Capture.In(node2ToNode1Payloads), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var node1Sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockNode1Sender.Object));
        });
        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node1Orch.InitializeAsync(CancellationToken.None);

        await node1Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);

        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("HappyKey1", "HappyVal1"), nameof(SyncTestState.DataMap), CancellationToken.None);
        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-2", new MapSetIntent("HappyKey2", "HappyVal2"), nameof(SyncTestState.DataMap), CancellationToken.None);

        await node1Scope.Storage.TrimAsync("default", new Dictionary<string, long> { ["Replica1"] = 2 }, CancellationToken.None);
        await node1Scope.Storage.TrimAsync(node1Orch.Registry.ShardId, new Dictionary<string, long> { ["Replica1"] = 10 }, CancellationToken.None);

        var node2Sp = BuildNode("Replica2", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockNode2Sender.Object));
        });
        var node2Scope = node2Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica2");
        var node2Orch = node2Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var node2Vvm = node2Scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await node2Orch.InitializeAsync(CancellationToken.None);
        await node2Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node2Orch.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);

        var node2PeerRegistry = node2Sp.GetRequiredService<IPeerRegistry>();
        var node1PeerId = new PeerId(Guid.NewGuid());
        var node2PeerId = new PeerId(Guid.NewGuid());
        await node2PeerRegistry.AddOrUpdatePeerAsync(TestMeshId, new PeerNode(node1PeerId, new TcpPeerEndpoint("127.0.0.1", 5001)), PeerStatus.Active, CancellationToken.None);

        var node1Handler = node1Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var node2Handler = node2Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = node1Sp.GetRequiredService<ICrdtSerializer>();

        // Act 1: Request sync from Node 1
        var node2InitialState = await node2Vvm.GetFullSnapshotAsync(CancellationToken.None);
        var syncMsg = new CrdtStateSyncMessage("Replica2", node2InitialState);
        var syncWrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", serializer.SerializeToBytes(syncMsg), "Replica2");
        await node1Handler.HandlePayloadAsync(TestMeshId, node2PeerId, serializer.SerializeToBytes(syncWrapper), CancellationToken.None);

        var docSnapshots = node1ToNode2Payloads
            .Select(p => (Raw: p, Wrapper: serializer.DeserializeFromBytes<CrdtMessageWrapper>(p.ToArray())))
            .Where(x => x.Wrapper.MessageType == "CrdtSnapshot" && x.Wrapper.DocumentId != node1Orch.Registry.DocumentId)
            .ToList();

        var regSnapshot = node1ToNode2Payloads
            .Select(p => (Raw: p, Wrapper: serializer.DeserializeFromBytes<CrdtMessageWrapper>(p.ToArray())))
            .First(x => x.Wrapper.MessageType == "CrdtSnapshot" && x.Wrapper.DocumentId == node1Orch.Registry.DocumentId);

        docSnapshots.Count.ShouldBe(2);

        // Act 2: Deliver the registry snapshot and first document snapshot, then verify Node 2 suppresses periodic anti-entropy while all shards are receiving snapshots
        await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, regSnapshot.Raw, CancellationToken.None);
        await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, docSnapshots[0].Raw, CancellationToken.None);

        node2ToNode1Payloads.Clear();
        var midBatchWorkload = await node2Orch.EvaluateAndDispatchAntiEntropyStateAsync(CancellationToken.None);
        midBatchWorkload.IsBusy.ShouldBeTrue();
        midBatchWorkload.PendingQueueDepth.ShouldBeGreaterThanOrEqualTo(1);
        node2ToNode1Payloads.ShouldBeEmpty();

        // Act 3: Deliver the second document snapshot to complete the shard batch
        await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, docSnapshots[1].Raw, CancellationToken.None);

        // Assert: Both documents are materialized and the shard DVV advanced to Replica1 = 2
        var doc1 = await node2Orch.GetDocumentStateAsync<SyncTestState>("sync-doc-1", CancellationToken.None);
        var doc2 = await node2Orch.GetDocumentStateAsync<SyncTestState>("sync-doc-2", CancellationToken.None);
        doc1.Data.DataMap.ShouldContainKeyAndValue("HappyKey1", "HappyVal1");
        doc2.Data.DataMap.ShouldContainKeyAndValue("HappyKey2", "HappyVal2");

        var convergedState = await node2Vvm.GetFullSnapshotAsync(CancellationToken.None);
        convergedState["default"].Versions.ShouldContainKeyAndValue("Replica1", 2);
    }

    [IntegrationFact]
    public async Task SnapshotBatchEdgeCase_WhenSnapshotsArriveOutOfOrderWithDuplicates_AndContainDisjointDots_AdvancesOnlyOnFullBatchWithIntersectedDots()
    {
        // Arrange: Node 2 expects 2 documents in "default" shard ("sync-doc-1" and "sync-doc-2").
        // Snapshot 1 has versions { Replica1: 5 } and dots { Replica3: [7, 9] }.
        // Snapshot 2 has versions { Replica1: 5 } and dots { Replica3: [7, 11] }.
        // Only dot 7 is common to both snapshots; dot 9 and dot 11 must not be prematurely marked as covered for the whole shard.
        var node1Sp = BuildNode("Replica1");
        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node1Orch.InitializeAsync(CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);
        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("K1", "V1"), nameof(SyncTestState.DataMap), CancellationToken.None);
        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-2", new MapSetIntent("K2", "V2"), nameof(SyncTestState.DataMap), CancellationToken.None);

        var node2Sp = BuildNode("Replica2");
        var node2Scope = node2Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica2");
        var node2Orch = node2Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var node2Vvm = node2Scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await node2Orch.InitializeAsync(CancellationToken.None);
        await node2Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node2Orch.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);

        var serializer = node1Sp.GetRequiredService<ICrdtSerializer>();
        var node2Handler = node2Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var node1PeerId = new PeerId(Guid.NewGuid());

        var snap1Data = await node1Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1")!.GetSnapshotDataAsync(CancellationToken.None);
        var snap2Data = await node1Orch.TryGetLocalDocument<SyncTestState>("sync-doc-2")!.GetSnapshotDataAsync(CancellationToken.None);

        var state1 = new ShardVersionVectorMap
        {
            ["default"] = new DottedVersionVector(
                new Dictionary<string, long> { ["Replica1"] = 5 },
                new Dictionary<string, ISet<long>> { ["Replica3"] = new HashSet<long> { 7, 9 } })
        };
        var state2 = new ShardVersionVectorMap
        {
            ["default"] = new DottedVersionVector(
                new Dictionary<string, long> { ["Replica1"] = 5 },
                new Dictionary<string, ISet<long>> { ["Replica3"] = new HashSet<long> { 7, 11 } })
        };

        var expectedBatchDocs = new List<string>
        {
            CrdtRegistryEntry.GetRegistryKey("sync-doc-1", "sync-doc-1"),
            CrdtRegistryEntry.GetRegistryKey("sync-doc-2", "sync-doc-2")
        };

        var msg2 = new CrdtSnapshotMessage("Replica1", snap2Data.SnapshotData, state2, expectedBatchDocs);
        var wrap2 = new CrdtMessageWrapper("sync-doc-2", "CrdtSnapshot", serializer.SerializeToBytes(msg2), "Replica1", "sync-doc-2");

        var msg1 = new CrdtSnapshotMessage("Replica1", snap1Data.SnapshotData, state1, expectedBatchDocs);
        var wrap1 = new CrdtMessageWrapper("sync-doc-1", "CrdtSnapshot", serializer.SerializeToBytes(msg1), "Replica1", "sync-doc-1");

        // Act 1: Deliver sync-doc-2 first, then a duplicate of sync-doc-2
        await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, serializer.SerializeToBytes(wrap2), CancellationToken.None);
        await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, serializer.SerializeToBytes(wrap2), CancellationToken.None);

        // Assert 1: Duplicate delivery of sync-doc-2 must NOT satisfy the 2-document barrier
        var midDvv = (await node2Vvm.GetFullSnapshotAsync(CancellationToken.None))["default"];
        midDvv.Versions.ContainsKey("Replica1").ShouldBeFalse();
        node2Vvm.TryGetActiveSnapshotReceptionState(5000, out var remaining).ShouldBeTrue();
        remaining.ShouldBe(1);

        // Act 2: Deliver sync-doc-1 to complete the batch
        await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, serializer.SerializeToBytes(wrap1), CancellationToken.None);

        // Assert 2: Barrier completes, Replica1 advances to 5, and only the intersected dot (7) for Replica3 is committed to shard DVV
        var finalDvv = (await node2Vvm.GetFullSnapshotAsync(CancellationToken.None))["default"];
        finalDvv.Versions.ShouldContainKeyAndValue("Replica1", 5);
        finalDvv.Dots.ShouldContainKey("Replica3");
        finalDvv.Dots["Replica3"].ShouldContain(7);
        finalDvv.Dots["Replica3"].ShouldNotContain(9);
        finalDvv.Dots["Replica3"].ShouldNotContain(11);
    }

    [IntegrationFact]
    public async Task SnapshotEdgeCase_WhenRecipientHasNotYetCreatedDocumentsLocally_LazilyPromotesDocumentsAndConvergesShardDvv()
    {
        // Arrange: Node 1 creates and mutates sync-doc-1 and sync-doc-2.
        // Node 2 has just initialized and has NOT created sync-doc-1 or sync-doc-2 in its local registry or memory yet.
        var node1Sp = BuildNode("Replica1");
        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node1Orch.InitializeAsync(CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);
        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("LazySnap1", "Val1"), nameof(SyncTestState.DataMap), CancellationToken.None);
        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-2", new MapSetIntent("LazySnap2", "Val2"), nameof(SyncTestState.DataMap), CancellationToken.None);

        var node2Sp = BuildNode("Replica2");
        var node2Scope = node2Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica2");
        var node2Orch = node2Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var node2Vvm = node2Scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await node2Orch.InitializeAsync(CancellationToken.None);

        // Verify Node 2 does not have either document promoted before snapshots arrive
        node2Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1").ShouldBeNull();
        node2Orch.TryGetLocalDocument<SyncTestState>("sync-doc-2").ShouldBeNull();

        var serializer = node1Sp.GetRequiredService<ICrdtSerializer>();
        var node2Handler = node2Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var node1PeerId = new PeerId(Guid.NewGuid());

        var snap1 = await node1Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1")!.GetSnapshotDataAsync(CancellationToken.None);
        var snap2 = await node1Orch.TryGetLocalDocument<SyncTestState>("sync-doc-2")!.GetSnapshotDataAsync(CancellationToken.None);

        var expectedBatchDocs = new List<string>
        {
            CrdtRegistryEntry.GetRegistryKey("sync-doc-1", "sync-doc-1"),
            CrdtRegistryEntry.GetRegistryKey("sync-doc-2", "sync-doc-2")
        };

        var msg1 = new CrdtSnapshotMessage("Replica1", snap1.SnapshotData, snap1.GlobalState, expectedBatchDocs);
        var wrap1 = new CrdtMessageWrapper("sync-doc-1", "CrdtSnapshot", serializer.SerializeToBytes(msg1), "Replica1", "sync-doc-1");

        var msg2 = new CrdtSnapshotMessage("Replica1", snap2.SnapshotData, snap2.GlobalState, expectedBatchDocs);
        var wrap2 = new CrdtMessageWrapper("sync-doc-2", "CrdtSnapshot", serializer.SerializeToBytes(msg2), "Replica1", "sync-doc-2");

        // Act: Deliver snapshots to Node 2 before registry gossip
        await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, serializer.SerializeToBytes(wrap1), CancellationToken.None);
        await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, serializer.SerializeToBytes(wrap2), CancellationToken.None);

        // Assert: Both documents were lazily promoted, merged, and the shard DVV advanced to 2
        var doc1 = node2Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1");
        var doc2 = node2Orch.TryGetLocalDocument<SyncTestState>("sync-doc-2");
        doc1.ShouldNotBeNull();
        doc2.ShouldNotBeNull();

        (await doc1!.GetDocumentAsync(CancellationToken.None)).Data.DataMap.ShouldContainKeyAndValue("LazySnap1", "Val1");
        (await doc2!.GetDocumentAsync(CancellationToken.None)).Data.DataMap.ShouldContainKeyAndValue("LazySnap2", "Val2");

        var node2State = await node2Vvm.GetFullSnapshotAsync(CancellationToken.None);
        node2State["default"].Versions.ShouldContainKeyAndValue("Replica1", 2);
    }

    [IntegrationFact]
    public async Task SnapshotEdgeCase_WhenStaleSnapshotArrivesAfterNewerPatches_DoesNotRegressStateOrMarkDocumentDirty()
    {
        // Arrange: Node 1 creates sync-doc-1, applies V1, takes a snapshot at clock 1, and then applies V2 (removing or overwriting V1) at clock 2.
        // Node 2 already received both V1 and V2 via live patches (so Node 2 is at clock 2), and then the delayed clock-1 snapshot arrives.
        var node1Sp = BuildNode("Replica1");
        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node1Orch.InitializeAsync(CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);

        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("Key", "OldValueFromSnapshot"), nameof(SyncTestState.DataMap), CancellationToken.None);
        var node1Doc = node1Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1")!;
        var staleSnapshot = await node1Doc.GetSnapshotDataAsync(CancellationToken.None);

        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("Key", "NewerValueFromPatch"), nameof(SyncTestState.DataMap), CancellationToken.None);
        var allOps = await node1Scope.Storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        var docOps = allOps.Where(o => (o.DocumentId?.ToString() ?? string.Empty).Contains("sync-doc-1", StringComparison.Ordinal)).Select(o => o.Operation).ToArray();

        var stateChangedNotifications = 0;
        var mockStateObserver = new Mock<IDocumentStateObserver<SyncTestState>>();
        mockStateObserver
            .Setup(o => o.OnStateChangedAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback(() => Interlocked.Increment(ref stateChangedNotifications))
            .Returns(Task.CompletedTask);

        var node2Sp = BuildNode("Replica2", services =>
        {
            services.AddSingleton(mockStateObserver.Object);
        });
        var node2Scope = node2Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica2");
        var node2Orch = node2Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node2Orch.InitializeAsync(CancellationToken.None);
        await node2Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);

        await node2Orch.ApplyPatchAsync<SyncTestState>("sync-doc-1", new CrdtPatch(docOps), CancellationToken.None);
        var node2Doc = node2Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1")!;

        // Checkpoint Node 2 so its internal dirty state becomes false, and reset observer notification counter
        await node2Doc.CheckpointAsync(CancellationToken.None);
        Volatile.Write(ref stateChangedNotifications, 0);

        var serializer = node1Sp.GetRequiredService<ICrdtSerializer>();
        var node2Handler = node2Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var staleMsg = new CrdtSnapshotMessage("Replica1", staleSnapshot.SnapshotData, staleSnapshot.GlobalState);
        var staleWrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtSnapshot", serializer.SerializeToBytes(staleMsg), "Replica1", "sync-doc-1");

        // Act: Deliver the stale clock-1 snapshot to Node 2 (which is already at clock 2)
        await node2Handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), serializer.SerializeToBytes(staleWrapper), CancellationToken.None);

        // Assert: Document value did not regress, DVV remained at clock 2, and redundant snapshot did not trigger state-change notifications
        var currentDoc = await node2Doc.GetDocumentAsync(CancellationToken.None);
        currentDoc.Data.DataMap.ShouldContainKeyAndValue("Key", "NewerValueFromPatch");
        node2Doc.GetLocalState()["default"].Versions["Replica1"].ShouldBe(2);
        Volatile.Read(ref stateChangedNotifications).ShouldBe(0);
    }

    [IntegrationFact]
    public async Task ProvideSnapshotAsync_WhenDocumentIsStoredOnDiskButNotPromotedInActiveMemory_LoadsAndDispatchesSnapshot()
    {
        // Arrange: Node 1 has a stored checkpoint for "stored-only-doc" in storage, but it is not currently in activeDocuments.
        var capturedSends = new List<ReadOnlyMemory<byte>>();
        var mockSender = new Mock<IDirectMessageSender>();
        mockSender.Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), Capture.In(capturedSends), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var node1Sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
        });
        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node1Orch.InitializeAsync(CancellationToken.None);

        var metadataManager = node1Scope.ServiceProvider.GetRequiredService<Ama.CRDT.Services.ICrdtMetadataManager>();
        var storedState = new SyncTestState { Id = "stored-only-doc" };
        storedState.DataMap["DiskKey"] = "DiskVal";
        var storedCrdtDoc = new CrdtDocument<SyncTestState>(storedState, metadataManager.Initialize(storedState));
        var compositeKey = CrdtRegistryEntry.GetRegistryKey("sync-doc-1", "stored-only-doc");
        await node1Scope.Storage.SaveDocumentAsync("default", compositeKey, storedCrdtDoc, CancellationToken.None);

        var targetPeerId = new PeerId(Guid.NewGuid());
        var serializer = node1Sp.GetRequiredService<ICrdtSerializer>();

        // Act: Request ProvideSnapshotAsync for the stored document
        await node1Orch.ProvideSnapshotAsync("stored-only-doc", "sync-doc-1", "Replica2", targetPeerId, CancellationToken.None);

        // Assert: Snapshot was loaded from storage and sent to the target peer
        capturedSends.Count.ShouldBe(1);
        var wrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(capturedSends[0].ToArray());
        wrapper.MessageType.ShouldBe("CrdtSnapshot");
        wrapper.DocumentId.ShouldBe("stored-only-doc");
        wrapper.DocumentTypeAlias.ShouldBe("sync-doc-1");

        var snapMsg = serializer.DeserializeFromBytes<CrdtSnapshotMessage>(wrapper.Payload!);
        var deserializedDoc = serializer.DeserializeFromBytes<CrdtDocument<SyncTestState>>(snapMsg.SnapshotData);
        deserializedDoc.Data.DataMap.ShouldContainKeyAndValue("DiskKey", "DiskVal");
    }

    private sealed class DocumentIdAsShardIdProvider : IShardIdProvider
    {
        public string GetShardId<T>(CrdtDocument<T> document) where T : class => document.Data is SyncTestState s ? s.Id : "default";
        public string GetShardId<T>(IComparable documentId) where T : class => documentId?.ToString() ?? "default";
        public string GetShardId(IComparable documentId, Type documentType) => documentId?.ToString() ?? "default";
    }

    [IntegrationFact]
    public async Task EvaluateAndDispatchAntiEntropyStateAsync_WhenOneShardIsBusy_StillAllowsIndependentShardsToSyncAndStreamSnapshots()
    {
        // Arrange: Configure a shard-per-document provider so sync-doc-1 and sync-doc-2 live on distinct shards ("sync-doc-1" and "sync-doc-2").
        var capturedSends = new List<ReadOnlyMemory<byte>>();
        var mockSender = new Mock<IDirectMessageSender>();
        mockSender.Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), Capture.In(capturedSends), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
            services.AddSingleton<IShardIdProvider, DocumentIdAsShardIdProvider>();
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var concreteOrch = (CrdtDocumentOrchestrator)orchestrator;
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await orchestrator.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);

        await orchestrator.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("K1", "V1"), nameof(SyncTestState.DataMap), CancellationToken.None);
        await orchestrator.ApplyIntentAsync<SyncTestState>("sync-doc-2", new MapSetIntent("K2", "V2"), nameof(SyncTestState.DataMap), CancellationToken.None);

        var peerRegistry = sp.GetRequiredService<IPeerRegistry>();
        var peerId = new PeerId(Guid.NewGuid());
        await peerRegistry.AddOrUpdatePeerAsync(TestMeshId, new PeerNode(peerId, new TcpPeerEndpoint("127.0.0.1", 5000)), PeerStatus.Active, CancellationToken.None);
        await orchestrator.FlushAsync(CancellationToken.None);
        capturedSends.Clear();

        // Hold an active anti-entropy work scope ONLY on shard "sync-doc-1"
        using var heldShard1Scope = concreteOrch.TryAcquireAntiEntropyWorkScope("sync-doc-1");
        heldShard1Scope.ShouldNotBeNull();

        // Attempting to acquire the same shard "sync-doc-1" again should return null
        concreteOrch.TryAcquireAntiEntropyWorkScope("sync-doc-1").ShouldBeNull();

        // Act 1: Trigger EvaluateAndDispatchAntiEntropyStateAsync while shard "sync-doc-1" is busy
        var workload = await orchestrator.EvaluateAndDispatchAntiEntropyStateAsync(CancellationToken.None);

        // Assert 1: Anti-entropy was NOT globally blocked; it dispatched CrdtSync for the remaining available shards ("sync-doc-2" and registry)
        workload.IsBusy.ShouldBeFalse();
        capturedSends.Count.ShouldBe(1);
        var syncWrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(capturedSends[0].ToArray());
        syncWrapper.MessageType.ShouldBe("CrdtSync");
        var syncMsg = serializer.DeserializeFromBytes<CrdtStateSyncMessage>(syncWrapper.Payload!);
        syncMsg.IsPartialShardSelection.ShouldBeTrue();
        syncMsg.State.ShouldContainKey("sync-doc-2");
        syncMsg.State.ShouldNotContainKey("sync-doc-1");

        // Verify that when a remote peer receives this partial CrdtSync message (which intentionally omits "sync-doc-1"),
        // it does NOT mistake the omitted "sync-doc-1" shard for an empty DVV and send back a false snapshot or patch for "sync-doc-1".
        var peerCapturedSends = new List<ReadOnlyMemory<byte>>();
        var mockPeerSender = new Mock<IDirectMessageSender>();
        mockPeerSender.Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), Capture.In(peerCapturedSends), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var peerSp = BuildNode("Replica2", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockPeerSender.Object));
            services.AddSingleton<IShardIdProvider, DocumentIdAsShardIdProvider>();
        });
        var peerScope = peerSp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica2");
        var peerOrch = peerScope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await peerOrch.InitializeAsync(CancellationToken.None);
        await peerOrch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await peerOrch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("PeerK1", "PeerV1"), nameof(SyncTestState.DataMap), CancellationToken.None);

        var peerHandler = peerSp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        await peerHandler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), capturedSends[0], CancellationToken.None);

        var peerSentDocIds = peerCapturedSends
            .Select(p => serializer.DeserializeFromBytes<CrdtMessageWrapper>(p.ToArray()))
            .Select(w => w.DocumentId)
            .ToList();
        peerSentDocIds.ShouldNotContain("sync-doc-1");

        // Act 2: Trim journals for both shards and send an incoming CrdtSync from a remote peer requesting both shards while "sync-doc-1" is still locked
        await scope.Storage.TrimAsync("sync-doc-1", new Dictionary<string, long> { ["Replica1"] = 1 }, CancellationToken.None);
        await scope.Storage.TrimAsync("sync-doc-2", new Dictionary<string, long> { ["Replica1"] = 1 }, CancellationToken.None);
        capturedSends.Clear();

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var remoteState = new ShardVersionVectorMap
        {
            ["sync-doc-1"] = new DottedVersionVector(),
            ["sync-doc-2"] = new DottedVersionVector(),
            [orchestrator.Registry.ShardId] = new DottedVersionVector()
        };
        var incomingSync = new CrdtStateSyncMessage("Replica2", remoteState);
        var incomingWrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", serializer.SerializeToBytes(incomingSync), "Replica2");
        await handler.HandlePayloadAsync(TestMeshId, peerId, serializer.SerializeToBytes(incomingWrapper), CancellationToken.None);

        // Assert 2: Snapshot for "sync-doc-2" was immediately dispatched even though "sync-doc-1" was busy
        var sentDocIds = capturedSends
            .Select(p => serializer.DeserializeFromBytes<CrdtMessageWrapper>(p.ToArray()))
            .Where(w => w.MessageType == "CrdtSnapshot")
            .Select(w => w.DocumentId)
            .ToList();

        sentDocIds.ShouldContain("sync-doc-2");
        sentDocIds.ShouldNotContain("sync-doc-1");
    }

    [IntegrationFact]
    public async Task EvaluateAndDispatchAntiEntropyStateAsync_WhenInboundNetworkQueueHasUnprocessedMessages_SkipsSyncAndPreventsRedundantResends()
    {
        // Arrange: Node 1 has operations in its journal. Node 2 has thousands of unprocessed messages sitting in IInboundMessageQueue<IMeshMessage>.
        var node1ToNode2Payloads = new List<ReadOnlyMemory<byte>>();
        var mockNode1Sender = new Mock<IDirectMessageSender>();
        mockNode1Sender.Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), Capture.In(node1ToNode2Payloads), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var node1Sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockNode1Sender.Object));
        });
        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node1Orch.InitializeAsync(CancellationToken.None);
        await node1Orch.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await node1Orch.ApplyIntentAsync<SyncTestState>("sync-doc-1", new MapSetIntent("BurstKey", "BurstVal"), nameof(SyncTestState.DataMap), CancellationToken.None);

        var inboundQueue = new InboundMessageQueue<IMeshMessage>();
        var mockNode2Sender = new Mock<IDirectMessageSender>();
        var node2Sp = BuildNode("Replica2", services =>
        {
            services.AddKeyedSingleton<IInboundMessageQueue<IMeshMessage>>(TestMeshId, inboundQueue);
            services.Replace(ServiceDescriptor.Singleton(mockNode2Sender.Object));
        });
        var node2Scope = node2Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica2");
        var node2Orch = node2Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node2Orch.InitializeAsync(CancellationToken.None);

        var peerRegistry = node2Sp.GetRequiredService<IPeerRegistry>();
        var node1PeerId = new PeerId(Guid.NewGuid());
        await peerRegistry.AddOrUpdatePeerAsync(TestMeshId, new PeerNode(node1PeerId, new TcpPeerEndpoint("127.0.0.1", 5001)), PeerStatus.Active, CancellationToken.None);
        await node2Orch.FlushAsync(CancellationToken.None);
        mockNode2Sender.Invocations.Clear();

        // Enqueue an unprocessed message in the P2P inbound queue (simulating 10k items still being unpacked from TCP)
        var dummyMessage = new Ama.Enterprise.P2p.Models.Algorithms.GossipMessage(
            TestMeshId,
            "1.0.0",
            Guid.NewGuid(),
            node1PeerId,
            1,
            new byte[] { 1, 2, 3 });
        await inboundQueue.WriteAsync(dummyMessage, CancellationToken.None);
        inboundQueue.PendingCount.ShouldBe(1);

        // Act 1: Node 2 runs periodic anti-entropy while its inbound network queue still has unprocessed messages
        var workload = await node2Orch.EvaluateAndDispatchAntiEntropyStateAsync(CancellationToken.None);

        // Assert 1: Node 2 detects the inbound network queue backlog, reports IsBusy = true, and does NOT send a stale empty CrdtSync!
        workload.IsBusy.ShouldBeTrue();
        workload.PendingQueueDepth.ShouldBeGreaterThanOrEqualTo(1);
        mockNode2Sender.Verify(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Never());

        // Act 2: Even if two duplicate CrdtSync requests arrive at Node 1 back-to-back while Node 2 is still processing journaled patches,
        // Node 1's per-peer/per-shard dispatch cooldown suppresses resending the exact same journaled patch batch a second time.
        var node1Handler = node1Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = node1Sp.GetRequiredService<ICrdtSerializer>();
        var node2EmptyState = new ShardVersionVectorMap
        {
            ["default"] = new DottedVersionVector(),
            [node1Orch.Registry.ShardId] = new DottedVersionVector()
        };
        var staleSyncMsg = new CrdtStateSyncMessage("Replica2", node2EmptyState);
        var staleSyncWrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", serializer.SerializeToBytes(staleSyncMsg), "Replica2");
        var node2PeerId = new PeerId(Guid.NewGuid());

        await node1Handler.HandlePayloadAsync(TestMeshId, node2PeerId, serializer.SerializeToBytes(staleSyncWrapper), CancellationToken.None);
        node1ToNode2Payloads.ShouldNotBeEmpty();

        node1ToNode2Payloads.Clear();
        await node1Handler.HandlePayloadAsync(TestMeshId, node2PeerId, serializer.SerializeToBytes(staleSyncWrapper), CancellationToken.None);

        // Assert 2: Second duplicate CrdtSync within the cooldown window did not re-push the same operations again
        node1ToNode2Payloads.ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task ProcessPatchAndSnapshotBatch_WithThousandsOfPerDocumentShards_ShouldNotIncurQuadraticScalingBottleneck()
    {
        // Arrange: Simulate ShowCaseShardIdProvider where every document has its own shard ID (e.g. 500 documents -> 500 shards).
        // Verify that receiving 500 distinct shard patches/snapshots on a newly spawned node completes in well under a few seconds
        // without O(N^2) ClusterTracker DVV cloning or O(N^2) TryFindRegistryEntry scans.
        const int documentCount = 500;

        var node1Sp = BuildNode("Replica1", services =>
        {
            services.AddSingleton<IShardIdProvider, DocumentIdAsShardIdProvider>();
        });
        var node1Scope = node1Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica1");
        var node1Orch = node1Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node1Orch.InitializeAsync(CancellationToken.None);

        var node2Sp = BuildNode("Replica2", services =>
        {
            services.AddSingleton<IShardIdProvider, DocumentIdAsShardIdProvider>();
        });
        var node2Scope = node2Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("Replica2");
        var node2Orch = node2Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await node2Orch.InitializeAsync(CancellationToken.None);

        var serializer = node1Sp.GetRequiredService<ICrdtSerializer>();
        var node2Handler = node2Sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var node1PeerId = new PeerId(Guid.NewGuid());

        var payloads = new List<byte[]>(documentCount);
        for (int i = 1; i <= documentCount; i++)
        {
            var docId = $"doc-shard-{i}";
            await node1Orch.CreateDocumentAsync(docId, "sync-doc-1", IntentDeliveryMode.FireAndForget, CancellationToken.None);
            await node1Orch.ApplyIntentAsync<SyncTestState>(docId, new MapSetIntent("K", "V"), nameof(SyncTestState.DataMap), IntentDeliveryMode.FireAndForget, CancellationToken.None);
        }

        await node1Orch.FlushAsync(CancellationToken.None);

        for (int i = 1; i <= documentCount; i++)
        {
            var docId = $"doc-shard-{i}";
            var doc = node1Orch.TryGetLocalDocument<SyncTestState>(docId)!;
            var snap = await doc.GetSnapshotDataAsync(CancellationToken.None);
            var msg = new CrdtSnapshotMessage("Replica1", snap.SnapshotData, snap.GlobalState);
            var wrapper = new CrdtMessageWrapper(docId, "CrdtSnapshot", serializer.SerializeToBytes(msg), "Replica1", "sync-doc-1");
            payloads.Add(serializer.SerializeToBytes(wrapper));
        }

        var sw = System.Diagnostics.Stopwatch.StartNew();

        // Act: Deliver all 500 per-shard snapshots to Node 2 (which has not even received the registry yet)
        for (int i = 0; i < payloads.Count; i++)
        {
            await node2Handler.HandlePayloadAsync(TestMeshId, node1PeerId, payloads[i], CancellationToken.None);
        }

        await node2Orch.FlushAsync(CancellationToken.None);

        sw.Stop();

        // Assert: All 500 documents promoted, merged, and tracked in ClusterTracker in a fraction of a second
        node2Orch.GetActiveDocuments().Count.ShouldBe(documentCount + 1); // +1 for Registry
        var trackedPeerState = node2Scope.ClusterTracker.TryGetPeerState("Replica1");
        trackedPeerState.ShouldNotBeNull();
        trackedPeerState!.Count.ShouldBe(documentCount);
        sw.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(15));
    }
}