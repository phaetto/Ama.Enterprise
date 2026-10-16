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
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
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
            opt.ActiveSyncDebounceMilliseconds = 0;
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
            default(CrdtOperation) with { Id = op1Id, ReplicaId = "ReplicaA", ShardClock = 1, ShardId = "default" },
            default(CrdtOperation) with { Id = op2Id, ReplicaId = "ReplicaA", ShardClock = 2, ShardId = "default" },
            default(CrdtOperation) with { Id = op3Id, ReplicaId = "ReplicaB", ShardClock = 1, ShardId = "default" }
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
        await storage.TrimAsync("default", gmvv, CancellationToken.None);

        // Assert
        var postTrimOps = await storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        postTrimOps.Count.ShouldBe(2);
        postTrimOps.Any(o => o.Operation.Id == op1Id).ShouldBeFalse();
        postTrimOps.Any(o => o.Operation.Id == op2Id).ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task MemoryCrdtStorage_TrimAsync_ShouldOnlyTrimTargetShard_WithoutMixing()
    {
        // Arrange
        var storage = new MemoryCrdtStorage();
        var op1Id = Guid.NewGuid();
        var op2Id = Guid.NewGuid();

        var opsA = new List<CrdtOperation>
        {
            default(CrdtOperation) with { Id = op1Id, ReplicaId = "ReplicaA", ShardClock = 1, ShardId = "shard-A" }
        };

        var opsB = new List<CrdtOperation>
        {
            default(CrdtOperation) with { Id = op2Id, ReplicaId = "ReplicaA", ShardClock = 1, ShardId = "shard-B" }
        };

        await storage.AppendAsync("doc-A", opsA, CancellationToken.None);
        await storage.AppendAsync("doc-B", opsB, CancellationToken.None);

        var gmvv = new Dictionary<string, long> { { "ReplicaA", 1 } };

        // Act - Trim only shard-A resolving tracking dependencies securely
        await storage.TrimAsync("shard-A", gmvv, CancellationToken.None);

        // Assert
        var allOps = await storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        allOps.Count.ShouldBe(1);
        
        allOps.Any(o => o.Operation.Id == op1Id).ShouldBeFalse(); // Trimmed
        allOps.Any(o => o.Operation.Id == op2Id).ShouldBeTrue();  // Intact ensuring generic target bounds are safely mapped correctly
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
        
        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var intent = new MapSetIntent("testKey", "DirtyData");
        await orchestrator.ApplyIntentAsync<JournalTestState>("journal-doc", intent, "DataMap", CancellationToken.None);

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();

        // Act
        await checkpointService.StartAsync(CancellationToken.None);
        await Task.Delay(1500);
        await checkpointService.StopAsync(CancellationToken.None);

        // Assert
        mockStorage.Verify(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        mockStorage.Verify(s => s.SaveDocumentAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CrdtDocument<JournalTestState>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
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

        var docManager = orchestrator.TryGetLocalDocument<JournalTestState>("journal-doc")!;

        // Act
        await docManager.InitializeAsync(CancellationToken.None);

        // Assert
        docManager.DocumentId.ShouldBe("journal-doc");

        // Act
        await orchestrator.ProvideSnapshotAsync("journal-doc", "RemoteReplica2", new PeerId(Guid.NewGuid()), CancellationToken.None);

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
        
        var globalMap = new ShardVersionVectorMap { { "journal-doc", globalDvv } };
        
        lock (context.ShardVersionVectors)
        {
            if (!context.ShardVersionVectors.TryGetValue("journal-doc", out var dvv))
            {
                dvv = new DottedVersionVector();
                context.ShardVersionVectors["journal-doc"] = dvv;
            }
            dvv.Merge(globalDvv);
        }
        
        var metadataManager = scope.ServiceProvider.GetRequiredService<ICrdtMetadataManager>();
        
        var snapshotState = new JournalTestState { Id = "journal-doc" };
        snapshotState.DataMap["Field"] = "SnapshotData";

        var metadata = metadataManager.Initialize(snapshotState);
        var snapshotDoc = new CrdtDocument<JournalTestState>(snapshotState, metadata);
        var snapshotBytes = serializer.SerializeToBytes(snapshotDoc);
        
        var snapshotMsg = new CrdtSnapshotMessage("RemoteA", snapshotBytes, globalMap);
        var payloadBytes = serializer.SerializeToBytes(snapshotMsg);
        var wrapper = new CrdtMessageWrapper("journal-doc", "CrdtSnapshot", payloadBytes);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);
        
        var gossipMsg = new GossipMessage("TestMesh", Guid.NewGuid(), new PeerId(Guid.NewGuid()), 10, wrapperBytes);

        // Act
        await handler.HandlePayloadAsync(gossipMsg.MeshId, gossipMsg.SenderId, gossipMsg.Payload, CancellationToken.None);

        // Assert
        var crdtDoc = await orchestrator.GetDocumentStateAsync<JournalTestState>("journal-doc", CancellationToken.None);
        crdtDoc.Data.DataMap.ShouldContainKeyAndValue("Field", "SnapshotData");
        context.ShardVersionVectors["journal-doc"].Versions["RemoteA"].ShouldBe(10);
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
        var syncService = scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
        var journalManager = scope.ServiceProvider.GetRequiredService<IJournalManager>();
        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        
        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var intent = new MapSetIntent("testKey", "Updated");
        await orchestrator.ApplyIntentAsync<JournalTestState>("journal-doc", intent, "DataMap", CancellationToken.None);

        var remoteDvv = new DottedVersionVector();

        var targetVector = replicaContext.ShardVersionVectors.TryGetValue("default", out var t) ? t : new DottedVersionVector();

        // Act
        var requirement = syncService.CalculateRequirement("ReplicaB", remoteDvv, replicaContext.ReplicaId, targetVector, "journal-doc");
        var missingOpsStream = journalManager.GetMissingOperationsAsync("default", requirement, CancellationToken.None);
        var result = await syncService.EvaluateJournalCompletionAsync(missingOpsStream, requirement, CancellationToken.None);

        // Assert
        result.SnapshotRequired.ShouldBeFalse();
        result.Operations.Count.ShouldBeGreaterThan(0);
        result.Operations.Any(o => o.Operation.JsonPath.Contains("dataMap")).ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task EvaluateJournalCompletion_ShouldReturnSnapshotRequired_WhenJournalIsTrimmed()
    {
        // Arrange
        var sp = BuildNode("ReplicaA");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var storage = scope.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        var syncService = scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
        var journalManager = scope.ServiceProvider.GetRequiredService<IJournalManager>();
        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var intent = new MapSetIntent("testKey", "Updated");
        await orchestrator.ApplyIntentAsync<JournalTestState>("journal-doc", intent, "DataMap", CancellationToken.None);

        var gmvv = new Dictionary<string, long> { { "ReplicaA", 10 } }; 
        await storage.TrimAsync("default", gmvv, CancellationToken.None);

        var remoteDvv = new DottedVersionVector();
        
        var targetVector = replicaContext.ShardVersionVectors.TryGetValue("default", out var t) ? t : new DottedVersionVector();

        // Act
        var requirement = syncService.CalculateRequirement("ReplicaB", remoteDvv, replicaContext.ReplicaId, targetVector, "journal-doc");
        var missingOpsStream = journalManager.GetMissingOperationsAsync("default", requirement, CancellationToken.None);
        var result = await syncService.EvaluateJournalCompletionAsync(missingOpsStream, requirement, CancellationToken.None);

        // Assert
        result.SnapshotRequired.ShouldBeTrue();
        result.Operations.ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task MergeSnapshot_ShouldExecuteTrueStateMerge_UnioningData()
    {
        // Arrange
        var patchObserverMock = new Mock<IDocumentPatchObserver>();
        
        var sp = BuildNode("ReplicaA", services =>
        {
            services.AddSingleton(patchObserverMock.Object);
        });
        
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

        var localIntent = new MapSetIntent("LocalKey", "LocalValue");
        await orchestrator.ApplyIntentAsync<JournalTestState>("journal-doc", localIntent, "DataMap", CancellationToken.None);

        var remoteState = new JournalTestState { Id = "journal-doc" };
        var remoteMetadata = metadataManager.Initialize(remoteState);
        var remoteDoc = new CrdtDocument<JournalTestState>(remoteState, remoteMetadata);
        
        var remoteIntent = new MapSetIntent("RemoteKey", "RemoteValue");
        var remoteOp = await patcher.GenerateOperationAsync(remoteDoc, x => x.DataMap, remoteIntent, CancellationToken.None);
        var remoteAppliedDoc = await applicator.ApplyPatchAsync(remoteDoc, new CrdtPatch(new[] { remoteOp }));
        
        var globalDvv = new DottedVersionVector();
        globalDvv.Versions["ReplicaB"] = 15;
        
        var globalMap = new ShardVersionVectorMap { { "default", globalDvv } };

        var snapshotBytes = serializer.SerializeToBytes(remoteAppliedDoc.Document);

        patchObserverMock.Invocations.Clear();

        var docA = orchestrator.TryGetLocalDocument<JournalTestState>("journal-doc")!;

        // Act
        await docA.MergeSnapshotAsync(snapshotBytes, globalMap, CancellationToken.None);
        await Task.Delay(500);

        // Assert
        // In a true state CRDT merge, it shouldn't generate diff patches as it directly merges state in place natively
        patchObserverMock.Verify(x => x.OnPatchGeneratedAsync(It.IsAny<string>(), It.IsAny<CrdtPatch>(), It.IsAny<CancellationToken>()), Times.Never);

        // Both keys should exist as a true state merge unions divergent mapped states logically
        var crdtDocAAfter = await orchestrator.GetDocumentStateAsync<JournalTestState>("journal-doc", CancellationToken.None);
        crdtDocAAfter.Data.DataMap.ShouldContainKeyAndValue("LocalKey", "LocalValue");
        crdtDocAAfter.Data.DataMap.ShouldContainKeyAndValue("RemoteKey", "RemoteValue");
        
        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        replicaContext.ShardVersionVectors["default"].Versions["ReplicaB"].ShouldBe(15);
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

        await orchestrator1.InitializeAsync(CancellationToken.None);
        await orchestrator1.CreateDocumentAsync("test-replayed-doc", "journal-doc", CancellationToken.None);
        await orchestrator1.SyncDocumentsAsync(CancellationToken.None);

        await orchestrator1.ApplyIntentAsync<JournalTestState>("test-replayed-doc", new MapSetIntent("key1", "ReplayedData1"), "DataMap", CancellationToken.None);
        await orchestrator1.ApplyIntentAsync<JournalTestState>("test-replayed-doc", new MapSetIntent("key2", "ReplayedData2"), "DataMap", CancellationToken.None);

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
        
        var doc2 = orchestrator2.TryGetLocalDocument<JournalTestState>("test-replayed-doc");

        doc2.ShouldNotBeNull();
        doc2!.DocumentId.ShouldBe("test-replayed-doc");

        var crdtDoc2 = await orchestrator2.GetDocumentStateAsync<JournalTestState>("test-replayed-doc", CancellationToken.None);
        crdtDoc2.Data.DataMap.ShouldContainKeyAndValue("key1", "ReplayedData1");
        crdtDoc2.Data.DataMap.ShouldContainKeyAndValue("key2", "ReplayedData2");
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

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("trim-test-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        for (int i = 0; i < 10; i++)
        {
            var intent = new MapSetIntent($"key{i}", $"value{i}");
            await orchestrator.ApplyIntentAsync<JournalTestState>("trim-test-doc", intent, "DataMap", CancellationToken.None);
        }

        var initialJournalOps = await sharedStorage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        initialJournalOps.Count.ShouldBeGreaterThan(10);

        var maintenanceService = sp.GetServices<IHostedService>().OfType<CrdtMaintenanceService>().First();
        
        // Act
        await maintenanceService.StartAsync(CancellationToken.None);
        await Task.Delay(1500);
        await maintenanceService.StopAsync(CancellationToken.None);

        // Assert
        var trimmedJournalOps = await sharedStorage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        
        trimmedJournalOps.Count.ShouldBeLessThan(initialJournalOps.Count);
        trimmedJournalOps.Count.ShouldBeLessThanOrEqualTo(5);
    }

    [IntegrationFact]
    public async Task StorageJournalForwarder_WhenHardThresholdExceededDuringBurst_TriggersImmediateBackgroundAggressiveTrim()
    {
        // Arrange
        var sharedStorage = new MemoryCrdtStorage();

        var sp = BuildNode("Replica1", services =>
        {
            services.Configure<DistributedCrdtOptions>(opt =>
            {
                opt.JournalHardTrimThreshold = 5;
                opt.MaintenanceIntervalSeconds = 3600; // Disable periodic maintenance so only StorageJournalForwarder can trim
            });
            services.Replace(ServiceDescriptor.Singleton<IDistributedCrdtStorage>(sharedStorage));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("hard-trim-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        // Act - Apply 8 intents to exceed the hard threshold of 5 on shard "default"
        for (int i = 0; i < 8; i++)
        {
            var intent = new MapSetIntent($"k{i}", $"v{i}");
            await orchestrator.ApplyIntentAsync<JournalTestState>("hard-trim-doc", intent, "DataMap", CancellationToken.None);
        }

        // Wait briefly for the background Task.Run aggressive trim in StorageJournalForwarder to complete
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!cts.Token.IsCancellationRequested)
        {
            var count = await sharedStorage.GetJournalCountAsync("default", CancellationToken.None);
            if (count < 5)
            {
                break;
            }
            await Task.Delay(50);
        }

        // Assert - Shard "default" journal was aggressively trimmed without waiting for CrdtMaintenanceService
        var remainingCount = await sharedStorage.GetJournalCountAsync("default", CancellationToken.None);
        remainingCount.ShouldBeLessThan(5);
    }

    [IntegrationFact]
    public async Task DistributedCrdtDocument_InitializeAsync_ShouldFallbackToOrphanedDocument_WhenPrimaryStorageReturnsNull()
    {
        // Arrange
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        mockStorage.SetReturnsDefault(Task.CompletedTask);

        var sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockStorage.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var metadataManager = scope.ServiceProvider.GetRequiredService<ICrdtMetadataManager>();

        var orphanedState = new JournalTestState { Id = "orphaned-doc" };
        orphanedState.DataMap["RecoveredKey"] = "RecoveredValue";
        var orphanedDoc = new CrdtDocument<JournalTestState>(orphanedState, metadataManager.Initialize(orphanedState));

        mockStorage.Setup(s => s.LoadDocumentAsync<JournalTestState>("default", "orphaned-doc", It.IsAny<CancellationToken>()))
            .ReturnsAsync((CrdtDocument<JournalTestState>?)null);

        mockStorage.Setup(s => s.LoadOrphanedDocumentAsync<JournalTestState>("default", "orphaned-doc", It.IsAny<CancellationToken>()))
            .ReturnsAsync(orphanedDoc);

        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        // Act
        await orchestrator.CreateDocumentAsync("orphaned-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        // Assert
        var loadedDoc = await orchestrator.GetDocumentStateAsync<JournalTestState>("orphaned-doc", CancellationToken.None);
        loadedDoc.Data.DataMap.ShouldContainKeyAndValue("RecoveredKey", "RecoveredValue");
        mockStorage.Verify(s => s.LoadOrphanedDocumentAsync<JournalTestState>("default", "orphaned-doc", It.IsAny<CancellationToken>()), Times.Once);
    }

    [IntegrationFact]
    public async Task CrdtP2pPayloadHandler_ProcessSnapshot_WhenRegistrySnapshotMerged_AutomaticallySyncsAndPromotesDiscoveredDocuments()
    {
        // Arrange - Node A creates documents in its registry and generates a registry snapshot
        var spA = BuildNode("ReplicaA");
        var scopeA = spA.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaA");
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchA.InitializeAsync(CancellationToken.None);

        await orchA.CreateDocumentAsync("snap-discovered-1", "journal-doc", CancellationToken.None);
        await orchA.CreateDocumentAsync("snap-discovered-2", "journal-doc", CancellationToken.None);
        await orchA.SyncDocumentsAsync(CancellationToken.None);

        var registrySnapshotDto = await orchA.Registry.GetSnapshotDataAsync(CancellationToken.None);

        // Node B starts completely empty
        var spB = BuildNode("ReplicaB");
        var scopeB = spB.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaB");
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchB.InitializeAsync(CancellationToken.None);

        orchB.TryGetLocalDocument<JournalTestState>("snap-discovered-1").ShouldBeNull();
        orchB.TryGetLocalDocument<JournalTestState>("snap-discovered-2").ShouldBeNull();

        var serializer = spB.GetRequiredService<ICrdtSerializer>();
        var handlerB = spB.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh");

        var snapshotMsg = new CrdtSnapshotMessage("ReplicaA", registrySnapshotDto.SnapshotData, registrySnapshotDto.GlobalState);
        var wrapper = new CrdtMessageWrapper(orchB.Registry.DocumentId, "CrdtSnapshot", serializer.SerializeToBytes(snapshotMsg));
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        // Act - Deliver the registry CrdtSnapshot to Node B
        await handlerB.HandlePayloadAsync("TestMesh", new PeerId(Guid.NewGuid()), wrapperBytes, CancellationToken.None);

        // Assert - ProcessSnapshotAsync merged the registry and automatically triggered SyncDocumentsAsync to promote both documents
        orchB.TryGetLocalDocument<JournalTestState>("snap-discovered-1").ShouldNotBeNull();
        orchB.TryGetLocalDocument<JournalTestState>("snap-discovered-2").ShouldNotBeNull();
    }

    [IntegrationFact]
    public async Task CrdtInitializationService_WhenZeroDocumentCheckpointsSaved_ReconstructsRegistryAndChildDocumentsSolelyFromWalJournal()
    {
        // Arrange - Node 1 applies registry creation and document mutations, but NEVER runs CrdtCheckpointService
        var walOnlyStorage = new MemoryCrdtStorage();

        var sp1 = BuildNode("ReplicaWal", services =>
        {
            services.Replace(ServiceDescriptor.Singleton<IDistributedCrdtStorage>(walOnlyStorage));
        });

        var scope1 = sp1.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaWal");
        var orch1 = scope1.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orch1.InitializeAsync(CancellationToken.None);

        await orch1.CreateDocumentAsync("wal-only-doc", "journal-doc", CancellationToken.None);
        await orch1.SyncDocumentsAsync(CancellationToken.None);
        await orch1.ApplyIntentAsync<JournalTestState>("wal-only-doc", new MapSetIntent("WalKey", "WalValue"), nameof(JournalTestState.DataMap), CancellationToken.None);

        // Confirm no document checkpoint exists in storage before restart
        var savedDocCheckpoint = await walOnlyStorage.LoadDocumentAsync<JournalTestState>("default", "wal-only-doc", CancellationToken.None);
        savedDocCheckpoint.ShouldBeNull();

        // Act - Cold-boot Node 2 against the WAL-only storage
        var sp2 = BuildNode("ReplicaWal", services =>
        {
            services.Replace(ServiceDescriptor.Singleton<IDistributedCrdtStorage>(walOnlyStorage));
        });

        var initService = sp2.GetServices<IHostedService>().OfType<CrdtInitializationService>().Single();
        await initService.StartAsync(CancellationToken.None);

        // Assert - Registry WAL ops were replayed first, child document was promoted, and child WAL ops were replayed onto it
        var scope2 = sp2.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaWal");
        var orch2 = scope2.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        var recoveredDoc = orch2.TryGetLocalDocument<JournalTestState>("wal-only-doc");
        recoveredDoc.ShouldNotBeNull();

        var state = await recoveredDoc!.GetDocumentAsync(CancellationToken.None);
        state.Data.DataMap.ShouldContainKeyAndValue("WalKey", "WalValue");
    }
}