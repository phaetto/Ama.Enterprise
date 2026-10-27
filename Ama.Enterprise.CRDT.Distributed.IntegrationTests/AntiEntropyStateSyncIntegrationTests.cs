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
        var syncWrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", serializer.SerializeToBytes(syncMsg));
        await handler.HandlePayloadAsync(TestMeshId, collidingPeerId, serializer.SerializeToBytes(syncWrapper), CancellationToken.None);

        // 2. Colliding CrdtPatch message from Node 2 claiming ReplicaId = "Replica1"
        var node2Ops = await node2Storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        var collidingOps = node2Ops.Where(o => o.DocumentId.ToString().Contains("sync-doc-1", StringComparison.Ordinal)).Select(o => o.Operation).ToArray();
        collidingOps.ShouldNotBeEmpty();

        var patchMsg = new CrdtPatchMessage("Replica1", new CrdtPatch(collidingOps));
        var patchWrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtPatch", serializer.SerializeToBytes(patchMsg), "sync-doc-1");
        await handler.HandlePayloadAsync(TestMeshId, collidingPeerId, serializer.SerializeToBytes(patchWrapper), CancellationToken.None);

        // 3. Colliding CrdtSnapshot message from Node 2 claiming ReplicaId = "Replica1"
        var node2Doc = node2Orch.TryGetLocalDocument<SyncTestState>("sync-doc-1")!;
        var node2Snapshot = await node2Doc.GetSnapshotDataAsync(CancellationToken.None);
        var snapshotMsg = new CrdtSnapshotMessage("Replica1", node2Snapshot.SnapshotData, node2Snapshot.GlobalState);
        var snapshotWrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtSnapshot", serializer.SerializeToBytes(snapshotMsg), "sync-doc-1");
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
        var loopbackWrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtPatch", serializer.SerializeToBytes(loopbackPatchMsg), "sync-doc-1");
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(localPeerGuid), serializer.SerializeToBytes(loopbackWrapper), CancellationToken.None);

        // Act 2: Remote peer ("Replica2") relays back a patch containing an operation authored by "Replica1"
        // (e.g. during anti-entropy or multi-hop forwarding)
        var bouncedPatchMsg = new CrdtPatchMessage("Replica2", new CrdtPatch(ownDocOps));
        var bouncedWrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtPatch", serializer.SerializeToBytes(bouncedPatchMsg), "sync-doc-1");
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
        var snapshotWrapper = new CrdtMessageWrapper("sync-doc-1", "CrdtSnapshot", serializer.SerializeToBytes(snapshotMsg), "sync-doc-1");

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
        var syncWrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", serializer.SerializeToBytes(syncFromNode1));
        await node2Handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), serializer.SerializeToBytes(syncWrapper), CancellationToken.None);

        // Assert 2: Node 2 sees that Node 1 is at (Replica1: 9000, Replica2: 10000) and does NOT resend any snapshot or patch!
        capturedDirectSends.ShouldBeEmpty();
    }
}