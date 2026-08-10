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
using Ama.CRDT.Services.Journaling;
using Ama.CRDT.Services.Serialization;
using Ama.CRDT.Services.Versioning;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Models.Algorithms;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

[CrdtAotType(typeof(JournalingAndSnapshottingIntegrationTests.JournalTestState))]
[CrdtAotType(typeof(Dictionary<string, string>))]
public sealed partial class JournalTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(JournalingAndSnapshottingIntegrationTests.JournalTestState))]
[JsonSerializable(typeof(CrdtDocument<JournalingAndSnapshottingIntegrationTests.JournalTestState>))]
public sealed partial class JournalTestJsonContext : JsonSerializerContext
{
}

public sealed class JournalingAndSnapshottingIntegrationTests
{
    public sealed class JournalTestState
    {
        public string Id { get; set; } = "journal-doc";
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
            opt.ActiveSyncEnabled = true;
            opt.MaintenanceIntervalSeconds = 120;
        });

        services.AddDistributedCrdtReplica(replicaId);

        services.AddCrdt()
                .AddCrdtAotContext(new JournalTestAotContext())
                .AddCrdtJsonTypeInfoResolver(JournalTestJsonContext.Default);

        services.AddDistributedDocumentType<JournalTestState>("journal-doc");
        services.AddDistributedCrdtP2p("TestMesh", replicaId);
        services.AddSingleton(Mock.Of<IP2pAlgorithm>());

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
            default(CrdtOperation) with { Id = op1Id, ReplicaId = "ReplicaA", GlobalClock = 1 },
            default(CrdtOperation) with { Id = op2Id, ReplicaId = "ReplicaA", GlobalClock = 2 },
            default(CrdtOperation) with { Id = op3Id, ReplicaId = "ReplicaB", GlobalClock = 1 }
        };

        // Act - Append
        await storage.AppendAsync("journal-doc", ops, CancellationToken.None);

        // Assert
        var allOps = await storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        allOps.Count.ShouldBe(3);

        // Act - Trim based on Global Minimum Version Vector (GMVV) bounds
        var gmvv = new Dictionary<string, long>
        {
            { "ReplicaA", 1 }, // ReplicaA up to 1 is known by all, so op1 can be trimmed
            { "ReplicaB", 0 }  // ReplicaB 1 is not known by everyone, so op3 must be kept
        };
        await storage.TrimAsync(gmvv, CancellationToken.None);

        // Assert
        var postTrimOps = await storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        postTrimOps.Count.ShouldBe(2);
        postTrimOps.Any(o => o.Operation.Id == op1Id).ShouldBeFalse();
        postTrimOps.Any(o => o.Operation.Id == op2Id).ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task CrdtCheckpointService_Executes_SavingBounds()
    {
        // Arrange
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        var sp = BuildNode("Replica1", services =>
        {
            services.Configure<DistributedCrdtOptions>(opt =>
            {
                opt.CheckpointIntervalSeconds = 1;
            });
            services.Replace(ServiceDescriptor.Singleton(mockStorage.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher = scope.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        
        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docManager = orchestrator.GetDocument<JournalTestState>("journal-doc")!;
        
        var intent = new MapSetIntent("testKey", "DirtyData");
        var op = await patcher.GenerateOperationAsync(docManager.Document, x => x.DataMap, intent, CancellationToken.None);
        await docManager.ApplyPatchAsync(new CrdtPatch(new[] { op }), CancellationToken.None);

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();

        // Act
        await checkpointService.StartAsync(CancellationToken.None);
        await Task.Delay(1500);
        await checkpointService.StopAsync(CancellationToken.None);

        // Assert
        mockStorage.Verify(s => s.SaveGlobalVersionVectorAsync(It.IsAny<string>(), It.IsAny<DottedVersionVector>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        mockStorage.Verify(s => s.SaveDocumentAsync(It.IsAny<string>(), It.IsAny<CrdtDocument<JournalTestState>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [IntegrationFact]
    public async Task DistributedCrdtDocument_InitializeAndSnapshot_SendsDirectMessage()
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
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docManager = orchestrator.GetDocument<JournalTestState>("journal-doc")!;

        // Act
        await docManager.InitializeAsync(CancellationToken.None);

        // Assert
        docManager.DocumentId.ShouldBe("journal-doc");

        // Act
        await scope.Orchestrator.ProvideSnapshotAsync(docManager.DocumentId, "RemoteReplica2", new PeerId(Guid.NewGuid()), CancellationToken.None);

        // Assert
        mockSender.Verify(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once());
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
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh");
        var serializer = sp.GetRequiredService<ICrdtSerializer>();
        
        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        var globalDvv = new DottedVersionVector();
        globalDvv.Versions["RemoteA"] = 10;
        
        lock (context.GlobalVersionVector)
        {
            globalDvv.Merge(context.GlobalVersionVector);
        }
        
        var docManager = orchestrator.GetDocument<JournalTestState>("journal-doc")!;
        var metadataManager = scope.ServiceProvider.GetRequiredService<ICrdtMetadataManager>();
        
        var snapshotState = new JournalTestState { Id = "journal-doc" };
        snapshotState.DataMap["Field"] = "SnapshotData";

        var metadata = metadataManager.Initialize(snapshotState);
        var snapshotDoc = new CrdtDocument<JournalTestState>(snapshotState, metadata);
        var snapshotBytes = serializer.SerializeToBytes(snapshotDoc);
        
        var snapshotMsg = new CrdtSnapshotMessage("RemoteA", snapshotBytes, globalDvv);
        var payloadBytes = serializer.SerializeToBytes(snapshotMsg);
        var wrapper = new CrdtMessageWrapper("journal-doc", "CrdtSnapshot", payloadBytes);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);
        
        var gossipMsg = new GossipMessage("TestMesh", Guid.NewGuid(), new PeerId(Guid.NewGuid()), 10, wrapperBytes);

        // Act
        await handler.HandlePayloadAsync(gossipMsg.MeshId, gossipMsg.SenderId, gossipMsg.Payload, CancellationToken.None);

        // Assert
        docManager.Document.Data.DataMap.ShouldContainKeyAndValue("Field", "SnapshotData");
        context.GlobalVersionVector.Versions["RemoteA"].ShouldBe(10);
    }

    [IntegrationFact]
    public async Task Orchestrator_CreateDocument_ShouldJournalRegistryOperations()
    {
        // Arrange
        var sp = BuildNode("ReplicaA");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var storage = scope.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Act
        await orchestrator.CreateDocumentAsync("brand-new-doc", "journal-doc", CancellationToken.None);
        
        // Assert
        var allOps = await storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        var registryOps = allOps.Where(o => o.DocumentId == orchestrator.Registry.DocumentId).ToList();
        
        registryOps.ShouldNotBeEmpty();
        registryOps.Any(o => o.Operation.JsonPath.Contains("brand-new-doc") || 
                            (o.Operation.Value != null && o.Operation.Value.ToString()!.Contains("brand-new-doc")))
                   .ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task EvaluateJournalCompletion_ShouldReturnOperations_WhenJournalIsIntact()
    {
        // Arrange
        var sp = BuildNode("ReplicaA");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher = scope.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        var syncService = scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
        var journalManager = scope.ServiceProvider.GetRequiredService<IJournalManager>();
        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        
        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docA = orchestrator.GetDocument<JournalTestState>("journal-doc")!;

        var intent = new MapSetIntent("testKey", "Updated");
        var op = await patcher.GenerateOperationAsync(docA.Document, x => x.DataMap, intent, CancellationToken.None);
        await docA.ApplyPatchAsync(new CrdtPatch(new[] { op }), CancellationToken.None);

        var remoteDvv = new DottedVersionVector();

        // Act
        var requirement = syncService.CalculateRequirement("ReplicaB", remoteDvv, replicaContext.ReplicaId, replicaContext.GlobalVersionVector);
        var missingOpsStream = journalManager.GetMissingOperationsAsync(requirement, CancellationToken.None);
        var result = await syncService.EvaluateJournalCompletionAsync(missingOpsStream, requirement, CancellationToken.None);

        // Assert
        result.SnapshotRequired.ShouldBeFalse();
        result.Operations.Count.ShouldBeGreaterThan(0);
        result.Operations.Any(o => o.Operation.Id == op.Id).ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task EvaluateJournalCompletion_ShouldReturnSnapshotRequired_WhenJournalIsTrimmed()
    {
        // Arrange
        var sp = BuildNode("ReplicaA");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher = scope.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        var storage = scope.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        var syncService = scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
        var journalManager = scope.ServiceProvider.GetRequiredService<IJournalManager>();
        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docA = orchestrator.GetDocument<JournalTestState>("journal-doc")!;

        var intent = new MapSetIntent("testKey", "Updated");
        var op = await patcher.GenerateOperationAsync(docA.Document, x => x.DataMap, intent, CancellationToken.None);
        await docA.ApplyPatchAsync(new CrdtPatch(new[] { op }), CancellationToken.None);

        var gmvv = new Dictionary<string, long> { { "ReplicaA", 10 } }; 
        await storage.TrimAsync(gmvv, CancellationToken.None);

        var remoteDvv = new DottedVersionVector();
        
        // Act
        var requirement = syncService.CalculateRequirement("ReplicaB", remoteDvv, replicaContext.ReplicaId, replicaContext.GlobalVersionVector);
        var missingOpsStream = journalManager.GetMissingOperationsAsync(requirement, CancellationToken.None);
        var result = await syncService.EvaluateJournalCompletionAsync(missingOpsStream, requirement, CancellationToken.None);

        // Assert
        result.SnapshotRequired.ShouldBeTrue();
        result.Operations.ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task MergeSnapshot_ShouldExecuteTrueStateMerge_UnioningData()
    {
        // Arrange
        var sp = BuildNode("ReplicaA");
        var serializer = sp.GetRequiredService<ICrdtSerializer>();
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var metadataManager = scope.ServiceProvider.GetRequiredService<ICrdtMetadataManager>();
        var patcher = scope.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        var applicator = scope.ServiceProvider.GetRequiredService<IAsyncCrdtApplicator>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docA = orchestrator.GetDocument<JournalTestState>("journal-doc")!;

        var localIntent = new MapSetIntent("LocalKey", "LocalValue");
        var localOp = await patcher.GenerateOperationAsync(docA.Document, x => x.DataMap, localIntent, CancellationToken.None);
        await docA.ApplyPatchAsync(new CrdtPatch(new[] { localOp }), CancellationToken.None);

        var remoteState = new JournalTestState { Id = "journal-doc" };
        var remoteMetadata = metadataManager.Initialize(remoteState);
        var remoteDoc = new CrdtDocument<JournalTestState>(remoteState, remoteMetadata);
        
        var remoteIntent = new MapSetIntent("RemoteKey", "RemoteValue");
        var remoteOp = await patcher.GenerateOperationAsync(remoteDoc, x => x.DataMap, remoteIntent, CancellationToken.None);
        var remoteAppliedDoc = await applicator.ApplyPatchAsync(remoteDoc, new CrdtPatch(new[] { remoteOp }));
        
        var globalDvv = new DottedVersionVector();
        globalDvv.Versions["ReplicaB"] = 15;

        var snapshotBytes = serializer.SerializeToBytes(remoteAppliedDoc.Document);

        CrdtPatch? broadcastedPatch = null;
        docA.PatchGenerated += (s, p) => 
        {
            broadcastedPatch = p;
        };

        // Act
        await docA.MergeSnapshotAsync(snapshotBytes, globalDvv, CancellationToken.None);
        await Task.Delay(500);

        // Assert
        // In a true state CRDT merge, it shouldn't generate diff patches as it directly merges state in place natively
        broadcastedPatch.ShouldBeNull();

        // Both keys should exist as a true state merge unions divergent mapped states logically
        docA.Document.Data.DataMap.ShouldContainKeyAndValue("LocalKey", "LocalValue");
        docA.Document.Data.DataMap.ShouldContainKeyAndValue("RemoteKey", "RemoteValue");
        
        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        replicaContext.GlobalVersionVector.Versions["ReplicaB"].ShouldBe(15);
    }

    [IntegrationFact]
    public async Task CrdtInitializationService_ShouldReplayJournaledOperations_OnStartup()
    {
        // Arrange - Node 1
        var sharedStorage = new MemoryCrdtStorage();

        var sp1 = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton<IDistributedCrdtStorage>(sharedStorage));
        });

        var scopeManager1 = sp1.GetRequiredService<DistributedCrdtScopeManager>();
        var scope1 = scopeManager1.GetOrCreateScope("Replica1");
        var orchestrator1 = scope1.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher1 = scope1.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();

        await orchestrator1.InitializeAsync(CancellationToken.None);
        await orchestrator1.CreateDocumentAsync("test-replayed-doc", "journal-doc", CancellationToken.None);
        await orchestrator1.SyncDocumentsAsync(CancellationToken.None);

        var doc1 = orchestrator1.GetDocument<JournalTestState>("test-replayed-doc")!;
        
        var intent1 = new MapSetIntent("key1", "ReplayedData1");
        var op1 = await patcher1.GenerateOperationAsync(doc1.Document, x => x.DataMap, intent1, CancellationToken.None);

        var intent2 = new MapSetIntent("key2", "ReplayedData2");
        var op2 = await patcher1.GenerateOperationAsync(doc1.Document, x => x.DataMap, intent2, CancellationToken.None);

        await doc1.ApplyPatchAsync(new CrdtPatch(new[] { op1, op2 }), CancellationToken.None);

        var journalOps = await sharedStorage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        journalOps.Count.ShouldBeGreaterThan(2);

        // Act - Node 2
        var sp2 = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton<IDistributedCrdtStorage>(sharedStorage));
        });

        var initService = sp2.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        // Assert
        var scopeManager2 = sp2.GetRequiredService<DistributedCrdtScopeManager>();
        var scope2 = scopeManager2.GetOrCreateScope("Replica1");
        var orchestrator2 = scope2.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        
        var doc2 = orchestrator2.GetDocument<JournalTestState>("test-replayed-doc");

        doc2.ShouldNotBeNull();
        doc2!.DocumentId.ShouldBe("test-replayed-doc");
        doc2.Document.Data.DataMap.ShouldContainKeyAndValue("key1", "ReplayedData1");
        doc2.Document.Data.DataMap.ShouldContainKeyAndValue("key2", "ReplayedData2");
    }

    [IntegrationFact]
    public async Task CrdtCheckpointService_ShouldAggressivelyTrimJournal_WhenThresholdIsExceeded()
    {
        // Arrange
        var sharedStorage = new MemoryCrdtStorage();

        var sp = BuildNode("Replica1", services =>
        {
            services.Configure<DistributedCrdtOptions>(opt =>
            {
                opt.CheckpointIntervalSeconds = 1;
                opt.JournalSoftTrimThreshold = 5;
            });
            services.Replace(ServiceDescriptor.Singleton<IDistributedCrdtStorage>(sharedStorage));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher = scope.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("trim-test-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var doc = orchestrator.GetDocument<JournalTestState>("trim-test-doc")!;

        for (int i = 0; i < 10; i++)
        {
            var intent = new MapSetIntent($"key{i}", $"value{i}");
            var op = await patcher.GenerateOperationAsync(doc.Document, x => x.DataMap, intent, CancellationToken.None);
            await doc.ApplyPatchAsync(new CrdtPatch(new[] { op }), CancellationToken.None);
        }

        var initialJournalOps = await sharedStorage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        initialJournalOps.Count.ShouldBeGreaterThan(10);

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();
        
        // Act
        await checkpointService.StartAsync(CancellationToken.None);
        await Task.Delay(1500);
        await checkpointService.StopAsync(CancellationToken.None);

        // Assert
        var trimmedJournalOps = await sharedStorage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        
        trimmedJournalOps.Count.ShouldBeLessThan(initialJournalOps.Count);
        trimmedJournalOps.Count.ShouldBeLessThanOrEqualTo(5);
    }
}