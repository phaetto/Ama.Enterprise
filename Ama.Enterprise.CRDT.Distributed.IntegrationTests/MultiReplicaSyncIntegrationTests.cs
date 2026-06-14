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

    private sealed record TestNode(
        IServiceProvider Sp, 
        Mock<IDirectMessageSender> Sender, 
        Mock<IP2pAlgorithm> Gossip, 
        PeerId PeerId,
        string ReplicaId,
        List<IHostedService> HostedServices);

    private TestNode BuildNode(string replicaId)
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
            opt.PeerEvictionTtlSeconds = 0; // Disabled eviction to prevent validation errors
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