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
        var patcherA = scopeA.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcherB = scopeB.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        await orchB.InitializeAsync(CancellationToken.None);

        // Act - Node A dynamically creates a document
        await orchA.CreateDocumentAsync("doc-1", "shared-doc", CancellationToken.None);
        
        await Task.Delay(1000); // Allow Gossip of the registry update to propagate natively

        // Node B dynamically processes the registry update to instantiate the document
        await orchB.SyncDocumentsAsync(CancellationToken.None);

        var docA = orchA.GetDocument<SharedTestState>("doc-1")!;
        var docB = orchB.GetDocument<SharedTestState>("doc-1")!;
        docB.ShouldNotBeNull();

        // Concurrently modify both documents
        var intentA = new MapSetIntent("KeyA", "ValueA");
        var opA = await patcherA.GenerateOperationAsync(docA.Document, x => x.DataMap, intentA, CancellationToken.None);
        var taskA = docA.ApplyPatchAsync(new CrdtPatch(new[] { opA }), CancellationToken.None);

        var intentB = new MapSetIntent("KeyB", "ValueB");
        var opB = await patcherB.GenerateOperationAsync(docB.Document, x => x.DataMap, intentB, CancellationToken.None);
        var taskB = docB.ApplyPatchAsync(new CrdtPatch(new[] { opB }), CancellationToken.None);

        await Task.WhenAll(taskA, taskB);
        
        await Task.Delay(1500); // Allow CRDT lock-free channel queues to naturally process the inbound payloads

        // Assert
        docA.Document.Data.DataMap.ShouldContainKeyAndValue("KeyA", "ValueA");
        docA.Document.Data.DataMap.ShouldContainKeyAndValue("KeyB", "ValueB");

        docB.Document.Data.DataMap.ShouldContainKeyAndValue("KeyA", "ValueA");
        docB.Document.Data.DataMap.ShouldContainKeyAndValue("KeyB", "ValueB");
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
        var patcherA = scopeA.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        await orchA.InitializeAsync(CancellationToken.None);
        await orchA.CreateDocumentAsync("doc-ae", "shared-doc", CancellationToken.None);
        await orchA.SyncDocumentsAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcherB = scopeB.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        await orchB.InitializeAsync(CancellationToken.None);
        await orchB.CreateDocumentAsync("doc-ae", "shared-doc", CancellationToken.None);
        await orchB.SyncDocumentsAsync(CancellationToken.None);

        var docA = orchA.GetDocument<SharedTestState>("doc-ae")!;
        var docB = orchB.GetDocument<SharedTestState>("doc-ae")!;

        // Modify offline
        var intentA = new MapSetIntent("OfflineKeyA", "ValueA");
        var opA = await patcherA.GenerateOperationAsync(docA.Document, x => x.DataMap, intentA, CancellationToken.None);
        await docA.ApplyPatchAsync(new CrdtPatch(new[] { opA }), CancellationToken.None);

        var intentB = new MapSetIntent("OfflineKeyB", "ValueB");
        var opB = await patcherB.GenerateOperationAsync(docB.Document, x => x.DataMap, intentB, CancellationToken.None);
        await docB.ApplyPatchAsync(new CrdtPatch(new[] { opB }), CancellationToken.None);

        await Task.Delay(500);

        // Act - Reconnect network
        WireMesh(nodeA, nodeB);

        // Manually trigger Anti-Entropy synchronization from A -> B natively
        var serializerA = nodeA.Sp.GetRequiredService<ICrdtSerializer>();
        var replicaContextA = scopeA.ServiceProvider.GetRequiredService<ReplicaContext>();
        
        var syncMsgA = new CrdtStateSyncMessage(nodeA.ReplicaId, replicaContextA.GlobalVersionVector);
        var syncBytesA = serializerA.SerializeToBytes(syncMsgA);
        var wrapperA = new CrdtMessageWrapper("Cluster", "CrdtSync", syncBytesA);
        var wrapperBytesA = serializerA.SerializeToBytes(wrapperA);

        var handlerB = nodeB.Sp.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh");
        await handlerB.HandlePayloadAsync("TestMesh", nodeA.PeerId, wrapperBytesA, CancellationToken.None);

        // Manually trigger Anti-Entropy synchronization from B -> A natively to explicitly exchange full missing bounds
        var serializerB = nodeB.Sp.GetRequiredService<ICrdtSerializer>();
        var replicaContextB = scopeB.ServiceProvider.GetRequiredService<ReplicaContext>();
        
        var syncMsgB = new CrdtStateSyncMessage(nodeB.ReplicaId, replicaContextB.GlobalVersionVector);
        var syncBytesB = serializerB.SerializeToBytes(syncMsgB);
        var wrapperB = new CrdtMessageWrapper("Cluster", "CrdtSync", syncBytesB);
        var wrapperBytesB = serializerB.SerializeToBytes(wrapperB);

        var handlerA = nodeA.Sp.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh");
        await handlerA.HandlePayloadAsync("TestMesh", nodeB.PeerId, wrapperBytesB, CancellationToken.None);

        await Task.Delay(1500); // Allow bidirectional DVV evaluation and Missing Operations transfers over direct channels

        // Assert - Both nodes converged explicitly handling missing boundaries correctly
        docA.Document.Data.DataMap.ShouldContainKeyAndValue("OfflineKeyA", "ValueA");
        docA.Document.Data.DataMap.ShouldContainKeyAndValue("OfflineKeyB", "ValueB");

        docB.Document.Data.DataMap.ShouldContainKeyAndValue("OfflineKeyA", "ValueA");
        docB.Document.Data.DataMap.ShouldContainKeyAndValue("OfflineKeyB", "ValueB");
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
        var patcherA = scopeA.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcherB = scopeB.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        await orchB.InitializeAsync(CancellationToken.None);

        var scopeC = nodeC.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeC.ReplicaId);
        var orchC = scopeC.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcherC = scopeC.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        await orchC.InitializeAsync(CancellationToken.None);

        // Node A creates the generic shared root, which propagates via dynamic topologies implicitly to B and C
        await orchA.CreateDocumentAsync("doc-3", "shared-doc", CancellationToken.None);
        
        await Task.Delay(1500); // Propagation boundary explicitly allowing standard background gossip delays

        await orchB.SyncDocumentsAsync(CancellationToken.None);
        await orchC.SyncDocumentsAsync(CancellationToken.None);

        var docA = orchA.GetDocument<SharedTestState>("doc-3")!;
        var docB = orchB.GetDocument<SharedTestState>("doc-3")!;
        var docC = orchC.GetDocument<SharedTestState>("doc-3")!;
        
        docB.ShouldNotBeNull();
        docC.ShouldNotBeNull();

        // Act - Concurrently patch from all three distinct replica matrices
        var opA = await patcherA.GenerateOperationAsync(docA.Document, x => x.DataMap, new MapSetIntent("KeyA", "ValA"), CancellationToken.None);
        var opB = await patcherB.GenerateOperationAsync(docB.Document, x => x.DataMap, new MapSetIntent("KeyB", "ValB"), CancellationToken.None);
        var opC = await patcherC.GenerateOperationAsync(docC.Document, x => x.DataMap, new MapSetIntent("KeyC", "ValC"), CancellationToken.None);

        var t1 = docA.ApplyPatchAsync(new CrdtPatch(new[] { opA }), CancellationToken.None);
        var t2 = docB.ApplyPatchAsync(new CrdtPatch(new[] { opB }), CancellationToken.None);
        var t3 = docC.ApplyPatchAsync(new CrdtPatch(new[] { opC }), CancellationToken.None);

        await Task.WhenAll(t1, t2, t3);
        
        await Task.Delay(2000); // Full multi-mesh synchronization traversal limits bounds explicitly

        // Assert - Convergence ensures identical deterministic bounds organically globally
        foreach (var doc in new[] { docA, docB, docC })
        {
            doc.Document.Data.DataMap.ShouldContainKeyAndValue("KeyA", "ValA");
            doc.Document.Data.DataMap.ShouldContainKeyAndValue("KeyB", "ValB");
            doc.Document.Data.DataMap.ShouldContainKeyAndValue("KeyC", "ValC");
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

        await orchB.SyncDocumentsAsync(CancellationToken.None);

        // Act - Manually trigger Anti-Entropy from B to A so A tracks B seamlessly
        var serializerB = nodeB.Sp.GetRequiredService<ICrdtSerializer>();
        var replicaContextB = scopeB.ServiceProvider.GetRequiredService<ReplicaContext>();
        
        var syncMsgB = new CrdtStateSyncMessage(nodeB.ReplicaId, replicaContextB.GlobalVersionVector);
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