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
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

[CrdtAotType(typeof(BackgroundAndStorageIntegrationTests.StorageTestState))]
public sealed partial class BackgroundAndStorageTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(BackgroundAndStorageIntegrationTests.StorageTestState))]
[JsonSerializable(typeof(CrdtDocument<BackgroundAndStorageIntegrationTests.StorageTestState>))]
public sealed partial class BackgroundAndStorageTestJsonContext : JsonSerializerContext
{
}

public sealed class BackgroundAndStorageIntegrationTests
{
    private const string TestMeshId = "StorageMesh";

    public sealed class StorageTestState
    {
        public string Id { get; set; } = "storage-doc";
        public string Field { get; set; } = "initial";
    }

    private IServiceProvider BuildNode(string replicaId, Action<IServiceCollection>? configureExtra = null, bool activeSync = false)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            opt.CheckpointIntervalSeconds = 1; // Short interval for background testing
            opt.AntiEntropyIntervalSeconds = 1;
            opt.AntiEntropyInitialDelaySeconds = 0;
            opt.ActiveSyncEnabled = activeSync;
        });

        services.AddDistributedCrdtReplica(replicaId);

        services.AddCrdt()
                .AddCrdtAotContext(new BackgroundAndStorageTestAotContext())
                .AddCrdtJsonTypeInfoResolver(BackgroundAndStorageTestJsonContext.Default);

        services.AddDistributedDocumentType<StorageTestState>("storage-doc");
        services.AddDistributedCrdtP2p(TestMeshId, replicaId);

        services.AddSingleton(Mock.Of<IP2pAlgorithm>());
        services.AddSingleton(Mock.Of<IDirectMessageSender>());

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
    }

    [IntegrationFact]
    public async Task MemoryCrdtStorage_ShouldAppendRetrieveAndTrim()
    {
        // Arrange
        var storage = new MemoryCrdtStorage();
        var op1Id = Guid.NewGuid();
        var op2Id = Guid.NewGuid();
        var op3Id = Guid.NewGuid();

        var ops = new List<CrdtOperation>
        {
            // Use default to initialize the readonly struct
            default(CrdtOperation) with { Id = op1Id, ReplicaId = "ReplicaA", GlobalClock = 1 },
            default(CrdtOperation) with { Id = op2Id, ReplicaId = "ReplicaA", GlobalClock = 2 },
            default(CrdtOperation) with { Id = op3Id, ReplicaId = "ReplicaB", GlobalClock = 1 }
        };

        // Act - Append
        await storage.AppendAsync("storage-doc", ops, CancellationToken.None);

        // Assert - Retrieve All
        var allOps = await storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        allOps.Count.ShouldBe(3);

        // Act - Trim based on Global Minimum Version Vector (GMVV) bounds
        var gmvv = new Dictionary<string, long>
        {
            { "ReplicaA", 1 }, // ReplicaA up to 1 is known by all, so op1 can be trimmed
            { "ReplicaB", 0 }  // ReplicaB 1 is not known by everyone, so op3 must be kept
        };
        await storage.TrimAsync(gmvv, CancellationToken.None);

        // Assert - Verify Trimming
        var postTrimOps = await storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        postTrimOps.Count.ShouldBe(2); // op2 and op3 should remain
        postTrimOps.Any(o => o.Operation.Id == op1Id).ShouldBeFalse();
        postTrimOps.Any(o => o.Operation.Id == op2Id).ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task CrdtTopologyObserver_ShouldBroadcastOnFirstPeer()
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
        await orchestrator.CreateDocumentAsync("storage-doc", "storage-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var observer = sp.GetRequiredKeyedService<IPeerTopologyObserver>(TestMeshId);
        var peerNode = new PeerNode(new PeerId(Guid.NewGuid()), new HttpPeerEndpoint("http://localhost", 5000));

        // Act - Trigger Peer Joined naturally
        await observer.OnPeerJoinedAsync(TestMeshId, peerNode, CancellationToken.None);
        
        // Assert - The observer should trigger document state sync broadcast
        mockSender.Verify(p => p.SendToRandomPeerAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once());
        
        // Act - Trigger again to test thread-safe connection check
        await observer.OnPeerJoinedAsync(TestMeshId, new PeerNode(new PeerId(Guid.NewGuid()), new HttpPeerEndpoint("http://localhost2", 5001)), CancellationToken.None);
        
        // Assert - Only triggered on the FIRST connected peer
        mockSender.Verify(p => p.SendToRandomPeerAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once());
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
        await orchestrator.CreateDocumentAsync("storage-doc", "storage-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docManager = orchestrator.GetDocument<StorageTestState>("storage-doc")!;
        
        // Prepare a valid empty patch
        var patch = new CrdtPatch(Array.Empty<CrdtOperation>());

        // Act
        await docManager.ApplyPatchAsync(patch, CancellationToken.None);

        var op1 = default(CrdtOperation) with { Id = Guid.NewGuid(), ReplicaId = "Replica1", JsonPath = "$.Field", Type = OperationType.Upsert, Value = "test" };
        var populatedPatch = new CrdtPatch(new[] { op1 });

        await docManager.ApplyPatchAsync(populatedPatch, CancellationToken.None);

        // Now verify it broadcasted
        mockP2p.Verify(p => p.BroadcastAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [IntegrationFact]
    public async Task CrdtGossipHandler_ProcessSnapshot_ShouldMerge_ResolvingGaps()
    {
        // Arrange
        var mockP2p = new Mock<IP2pAlgorithm>();
        var sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockP2p.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("storage-doc", "storage-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>("StorageMesh");
        var serializer = sp.GetRequiredService<ICrdtSerializer>();
        
        // Prepare a complete bound fallback snapshot
        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        var globalDvv = new DottedVersionVector();
        globalDvv.Versions["RemoteA"] = 10;
        
        lock (context.GlobalVersionVector)
        {
            globalDvv.Merge(context.GlobalVersionVector);
        }
        
        var docManager = orchestrator.GetDocument<StorageTestState>("storage-doc")!;
        var metadataManager = scope.ServiceProvider.GetRequiredService<ICrdtMetadataManager>();
        
        var metadata = metadataManager.Initialize(new StorageTestState());
        var snapshotDoc = new CrdtDocument<StorageTestState>(new StorageTestState { Id = "storage-doc", Field = "SnapshotData" }, metadata);
        var snapshotBytes = serializer.SerializeToBytes(snapshotDoc);
        
        var snapshotMsg = new CrdtSnapshotMessage("RemoteA", snapshotBytes, globalDvv);
        var payloadBytes = serializer.SerializeToBytes(snapshotMsg);
        var wrapper = new CrdtMessageWrapper("storage-doc", "CrdtSnapshot", payloadBytes);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);
        
        var gossipMsg = new GossipMessage("StorageMesh", Guid.NewGuid(), new PeerId(Guid.NewGuid()), 10, wrapperBytes);

        // Act - Process the unwrapped payload bridging payload states
        await handler.HandlePayloadAsync(gossipMsg.MeshId, gossipMsg.SenderId, gossipMsg.Payload, CancellationToken.None);

        // Assert
        docManager.Document.Data.Field.ShouldBe("SnapshotData");
        
        context.GlobalVersionVector.Versions["RemoteA"].ShouldBe(10);
    }

    [IntegrationFact]
    public async Task CrdtCheckpointService_Executes_SavingBounds()
    {
        // Arrange
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        var sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockStorage.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("storage-doc", "storage-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docManager = orchestrator.GetDocument<StorageTestState>("storage-doc")!;
        
        // Ensure the document is mutated so it gets flagged as Dirty and properly saved
        var op = default(CrdtOperation) with { Id = Guid.NewGuid(), ReplicaId = "Replica1", JsonPath = "$.Field", Type = OperationType.Upsert, Value = "DirtyData" };
        await docManager.ApplyPatchAsync(new CrdtPatch(new[] { op }), CancellationToken.None);

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();

        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMilliseconds(1500)); // Service interval is set to 1 sec

        // Act
        try
        {
            await checkpointService.StartAsync(CancellationToken.None);
            await Task.Delay(1500, cts.Token);
        }
        catch (TaskCanceledException)
        {
            // Expected
        }
        finally
        {
            await checkpointService.StopAsync(CancellationToken.None);
        }

        // Assert - Verifies bounds
        mockStorage.Verify(s => s.SaveGlobalVersionVectorAsync(It.IsAny<string>(), It.IsAny<DottedVersionVector>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        mockStorage.Verify(s => s.SaveDocumentAsync(It.IsAny<string>(), It.IsAny<CrdtDocument<StorageTestState>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }
}