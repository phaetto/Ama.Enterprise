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
using Ama.Enterprise.CRDT.Distributed.Services.P2p;

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
        var wrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", syncBytes);
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

        var observers = sp.GetServices<IPeerTopologyObserver>();
        var observer = observers.OfType<CrdtTopologyObserver>().Single();
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
        var wrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtSync", syncPayload);
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
        var wrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", syncBytes);
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
        var wrapper = new CrdtMessageWrapper("lazy-rpc-doc", "CrdtIntentReq", reqBytes, "sync-doc-1");
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
        var wrapper = new CrdtMessageWrapper("gossip-first-doc", "CrdtPatch", patchBytes, "sync-doc-1");
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
        var wrapper = new CrdtMessageWrapper("unknown-missing-doc", "CrdtReadReq", reqBytes, "unregistered-type-alias");
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
        var wrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtIntentReq", reqBytes, "sync-doc-1");
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
}