namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using Ama.CRDT.Attributes;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

[CrdtAotType(typeof(DocumentOrchestratorIntegrationTests.DynamicTestState))]
[CrdtAotType(typeof(DocumentOrchestratorIntegrationTests.AlternateTestState))]
public sealed partial class DynamicTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(DocumentOrchestratorIntegrationTests.DynamicTestState))]
[JsonSerializable(typeof(CrdtDocument<DocumentOrchestratorIntegrationTests.DynamicTestState>))]
[JsonSerializable(typeof(DocumentOrchestratorIntegrationTests.AlternateTestState))]
[JsonSerializable(typeof(CrdtDocument<DocumentOrchestratorIntegrationTests.AlternateTestState>))]
public sealed partial class DynamicTestJsonContext : JsonSerializerContext
{
}

public sealed class DocumentOrchestratorIntegrationTests
{
    public sealed class DynamicTestState
    {
        public string Id { get; set; } = "default";
        public int Value { get; set; }
    }

    public sealed class AlternateTestState
    {
        public string Id { get; set; } = "default";
        public string Label { get; set; } = string.Empty;
    }

    private IServiceProvider BuildNode(Action<IServiceCollection>? configureExtra = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore();
        services.AddDistributedCrdtReplica("ReplicaA");

        services.AddCrdt()
                .AddCrdtAotContext(new DynamicTestAotContext())
                .AddCrdtJsonTypeInfoResolver(DynamicTestJsonContext.Default);

        services.AddDistributedDocumentType<DynamicTestState>("dynamic-type");
        services.AddDistributedDocumentType<AlternateTestState>("alternate-type");

        var mockP2p = new Mock<IP2pAlgorithm>();
        mockP2p.SetReturnsDefault(Task.CompletedTask);
        services.AddSingleton(mockP2p.Object);

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
    }

    [IntegrationFact]
    public async Task Orchestrator_ShouldIsolateShards_WhenDeletingDocuments()
    {
        // Arrange
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        var sp = BuildNode(s => 
        {
            s.Replace(ServiceDescriptor.Singleton(mockStorage.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        await orchestrator.CreateDocumentAsync("doc-shard-1", "dynamic-type", CancellationToken.None);
        await orchestrator.CreateDocumentAsync("doc-shard-2", "dynamic-type", CancellationToken.None);

        // Act - Assume document Id represents its default shard isolation mapping bounds
        await orchestrator.DeleteDocumentAsync("doc-shard-1", CancellationToken.None);

        // Assert - Validates ShardID properties route target structures properly natively 
        mockStorage.Verify(x => x.DeleteDocumentAsync(It.IsAny<string>(), "doc-shard-1", It.IsAny<CancellationToken>()), Times.Once);
        mockStorage.Verify(x => x.DeleteDocumentAsync(It.IsAny<string>(), "doc-shard-2", It.IsAny<CancellationToken>()), Times.Never);
        
        var remainingDocs = orchestrator.GetActiveDocuments();
        remainingDocs.Any(d => d.DocumentId == "doc-shard-1").ShouldBeFalse();
        remainingDocs.Any(d => d.DocumentId == "doc-shard-2").ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task Orchestrator_ShouldDynamicallyCreateAndTrackDocuments()
    {
        // Arrange
        var sp = BuildNode();
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Act
        await orchestrator.CreateDocumentAsync("dynamic-doc-1", "dynamic-type", CancellationToken.None);

        // Assert
        var doc = orchestrator.TryGetLocalDocument<DynamicTestState>("dynamic-doc-1");
        doc.ShouldNotBeNull();
        doc!.DocumentId.ShouldBe("dynamic-doc-1");

        var allDocs = orchestrator.GetActiveDocuments();
        allDocs.Any(d => d.DocumentId == "dynamic-doc-1").ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task Orchestrator_ShouldTombstoneAndDeleteDocuments_AcrossStorage()
    {
        // Arrange
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        var sp = BuildNode(s => 
        {
            s.Replace(ServiceDescriptor.Singleton(mockStorage.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        await orchestrator.CreateDocumentAsync("doc-to-delete", "dynamic-type", CancellationToken.None);
        
        orchestrator.TryGetLocalDocument<DynamicTestState>("doc-to-delete").ShouldNotBeNull();

        // Act
        await orchestrator.DeleteDocumentAsync("doc-to-delete", CancellationToken.None);

        // Assert
        var deletedDoc = orchestrator.TryGetLocalDocument<DynamicTestState>("doc-to-delete");
        deletedDoc.ShouldBeNull();
        
        var registryDocs = (await orchestrator.Registry.GetDocumentAsync()).Data!.Documents;
        registryDocs.TryGetValue(CrdtRegistryEntry.GetRegistryKey("dynamic-type", "doc-to-delete"), out var entry).ShouldBeTrue();
        entry!.IsDeleted.ShouldBeTrue();

        mockStorage.Verify(x => x.DeleteDocumentAsync(It.IsAny<string>(), "doc-to-delete", It.IsAny<CancellationToken>()), Times.Once);
    }

    [IntegrationFact]
    public async Task Orchestrator_ShouldIsolateSameDocumentId_AcrossDifferentDocumentTypes()
    {
        // Arrange
        var sp = BuildNode();
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        const string sharedId = "shared-doc-id";

        // Act - Create two documents with the exact same ID but different types
        await orchestrator.CreateDocumentAsync(sharedId, "dynamic-type", CancellationToken.None);
        await orchestrator.CreateDocumentAsync(sharedId, "alternate-type", CancellationToken.None);

        await orchestrator.ApplyIntentAsync<DynamicTestState>(sharedId, new Ama.CRDT.Models.Intents.SetIntent(42), nameof(DynamicTestState.Value), CancellationToken.None);
        await orchestrator.ApplyIntentAsync<AlternateTestState>(sharedId, new Ama.CRDT.Models.Intents.SetIntent("hello-world"), nameof(AlternateTestState.Label), CancellationToken.None);

        // Assert - Both documents exist simultaneously without colliding
        var dynamicDoc = orchestrator.TryGetLocalDocument<DynamicTestState>(sharedId);
        var alternateDoc = orchestrator.TryGetLocalDocument<AlternateTestState>(sharedId);

        dynamicDoc.ShouldNotBeNull();
        alternateDoc.ShouldNotBeNull();
        dynamicDoc!.TypeAlias.ShouldBe("dynamic-type");
        alternateDoc!.TypeAlias.ShouldBe("alternate-type");

        var dynamicState = await orchestrator.GetDocumentStateAsync<DynamicTestState>(sharedId, CancellationToken.None);
        var alternateState = await orchestrator.GetDocumentStateAsync<AlternateTestState>(sharedId, CancellationToken.None);

        dynamicState.Data.Value.ShouldBe(42);
        alternateState.Data.Label.ShouldBe("hello-world");

        // Act 2 - Delete only the DynamicTestState document with sharedId
        await orchestrator.DeleteDocumentAsync(sharedId, "dynamic-type", CancellationToken.None);

        // Assert 2 - DynamicTestState is deleted while AlternateTestState remains intact
        orchestrator.TryGetLocalDocument<DynamicTestState>(sharedId).ShouldBeNull();
        orchestrator.TryGetLocalDocument<AlternateTestState>(sharedId).ShouldNotBeNull();
    }

    [IntegrationFact]
    public async Task Orchestrator_ShouldIgnoreUnmappedTypeAliases_WithoutCrashing()
    {
        // Arrange
        var sp = BuildNode();
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Act
        await orchestrator.CreateDocumentAsync("unknown-doc", "missing-type", CancellationToken.None);

        // Assert
        var doc = orchestrator.GetActiveDocuments().FirstOrDefault(d => d.DocumentId == "unknown-doc");
        doc.ShouldBeNull(); // It should ignore unmapped aliases without application faults.
    }

    [IntegrationFact]
    public async Task Orchestrator_ResolveOrPromoteLocalDocumentAsync_ShouldRefusePromotionForTombstonedDocument()
    {
        // Arrange
        var sp = BuildNode();
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("tombstoned-doc", "dynamic-type", CancellationToken.None);

        await orchestrator.DeleteDocumentAsync("tombstoned-doc", CancellationToken.None);

        // Act - Attempt to lazily promote a document that was already deleted in the registry
        var promoted = await orchestrator.ResolveOrPromoteLocalDocumentAsync("tombstoned-doc", "dynamic-type", CancellationToken.None);

        // Assert
        promoted.ShouldBeNull();
        orchestrator.TryGetLocalDocument<DynamicTestState>("tombstoned-doc").ShouldBeNull();
    }

    [IntegrationFact]
    public async Task Orchestrator_ReevaluateTopologyAsync_ShouldDemoteNonLocalDocuments_WhilePreservingSharedLocalShardDvv()
    {
        // Arrange
        var mockDistribution = new Mock<IShardDistributionManager>();
        bool shardRemoteIsLocal = true;

        mockDistribution.Setup(d => d.IsShardLocalAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string shardId, CancellationToken _) =>
            {
                if (shardId == "remote-shard")
                {
                    return new ValueTask<bool>(shardRemoteIsLocal);
                }
                return new ValueTask<bool>(true);
            });

        var mockShardIdProvider = new Mock<Ama.CRDT.Services.Providers.IShardIdProvider>();
        mockShardIdProvider.Setup(p => p.GetShardId(It.IsAny<CrdtDocument<CrdtRegistryState>>()))
            .Returns((CrdtDocument<CrdtRegistryState> doc) => "system-document-registry");
        mockShardIdProvider.Setup(p => p.GetShardId(It.IsAny<CrdtDocument<DynamicTestState>>()))
            .Returns((CrdtDocument<DynamicTestState> doc) => doc.Data.Id.StartsWith("remote-", StringComparison.Ordinal) ? "remote-shard" : "local-shard");
        mockShardIdProvider.Setup(p => p.GetShardId<DynamicTestState>(It.IsAny<IComparable>()))
            .Returns((IComparable id) => id.ToString()!.StartsWith("remote-", StringComparison.Ordinal) ? "remote-shard" : "local-shard");
        mockShardIdProvider.Setup(p => p.GetShardId(It.IsAny<IComparable>(), It.IsAny<Type>()))
            .Returns((IComparable id, Type _) => id.ToString()!.StartsWith("remote-", StringComparison.Ordinal) ? "remote-shard" : "local-shard");

        var sp = BuildNode(s =>
        {
            s.Replace(ServiceDescriptor.Scoped(_ => mockDistribution.Object));
            s.Replace(ServiceDescriptor.Singleton(_ => mockShardIdProvider.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManager = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        await orchestrator.CreateDocumentAsync("remote-doc-1", "dynamic-type", CancellationToken.None);
        await orchestrator.CreateDocumentAsync("local-doc-1", "dynamic-type", CancellationToken.None);

        orchestrator.TryGetLocalDocument<DynamicTestState>("remote-doc-1").ShouldNotBeNull();
        orchestrator.TryGetLocalDocument<DynamicTestState>("local-doc-1").ShouldNotBeNull();

        // Act - Simulate cluster ring change where "remote-shard" is no longer local
        shardRemoteIsLocal = false;
        await orchestrator.ReevaluateTopologyAsync(CancellationToken.None);

        // Assert - "remote-doc-1" is demoted and its shard DVV removed, while "local-doc-1" and "local-shard" remain active
        orchestrator.TryGetLocalDocument<DynamicTestState>("remote-doc-1").ShouldBeNull();
        orchestrator.TryGetLocalDocument<DynamicTestState>("local-doc-1").ShouldNotBeNull();

        var dvvSnap = await dvvManager.GetFullSnapshotAsync(CancellationToken.None);
        dvvSnap.ContainsKey("remote-shard").ShouldBeFalse();
        dvvSnap.ContainsKey("local-shard").ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task Orchestrator_TryGetShardIdAsync_ResolvesRegistryAndRemoteRegistryEntries_AndClearsCacheOnDelete()
    {
        // Arrange
        var mockDistribution = new Mock<IShardDistributionManager>();
        mockDistribution.Setup(d => d.IsShardLocalAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string shardId, CancellationToken _) => new ValueTask<bool>(shardId == "system-document-registry"));
        mockDistribution.Setup(d => d.GetTargetNodesForShardAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<string>());

        var mockShardIdProvider = new Mock<Ama.CRDT.Services.Providers.IShardIdProvider>();
        mockShardIdProvider.Setup(p => p.GetShardId(It.IsAny<CrdtDocument<CrdtRegistryState>>()))
            .Returns((CrdtDocument<CrdtRegistryState> doc) => "system-document-registry");
        mockShardIdProvider.Setup(p => p.GetShardId(It.IsAny<CrdtDocument<DynamicTestState>>()))
            .Returns((CrdtDocument<DynamicTestState> doc) => $"custom-shard-{doc.Data.Id}");
        mockShardIdProvider.Setup(p => p.GetShardId<DynamicTestState>(It.IsAny<IComparable>()))
            .Returns((IComparable id) => $"custom-shard-{id}");
        mockShardIdProvider.Setup(p => p.GetShardId(It.IsAny<IComparable>(), It.IsAny<Type>()))
            .Returns((IComparable id, Type _) => $"custom-shard-{id}");

        var sp = BuildNode(s =>
        {
            s.Replace(ServiceDescriptor.Scoped(_ => mockDistribution.Object));
            s.Replace(ServiceDescriptor.Singleton(_ => mockShardIdProvider.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Act & Assert 1 - Registry shard ID resolves directly
        var registryShardId = await orchestrator.TryGetShardIdAsync("system-document-registry", CancellationToken.None);
        registryShardId.ShouldBe("system-document-registry");

        // Act & Assert 2 - Create a document whose shard is remote (so it is in Registry, but not in activeDocuments)
        await orchestrator.CreateDocumentAsync("remote-only-doc", "dynamic-type", CancellationToken.None);

        orchestrator.TryGetLocalDocument<DynamicTestState>("remote-only-doc").ShouldBeNull();
        var resolvedShardId = await orchestrator.TryGetShardIdAsync("remote-only-doc", CancellationToken.None);
        resolvedShardId.ShouldBe("custom-shard-remote-only-doc");

        // Act & Assert 3 - Deleting the document clears the shard ID cache and returns null for tombstoned entry
        await orchestrator.DeleteDocumentAsync("remote-only-doc", CancellationToken.None);

        var afterDeleteShardId = await orchestrator.TryGetShardIdAsync("remote-only-doc", CancellationToken.None);
        afterDeleteShardId.ShouldBeNull();
    }

    [IntegrationFact]
    public async Task Orchestrator_ApplyPatchAsync_WhenShardIsRemote_ForwardsDirectToTargetReplicaAndBroadcasts()
    {
        // Arrange
        var remotePeerGuid = Guid.NewGuid();
        var mockSender = new Mock<IDirectMessageSender>();
        var capturedDirectSends = new System.Collections.Generic.List<ReadOnlyMemory<byte>>();
        mockSender.Setup(s => s.SendDirectAsync(It.Is<Ama.Enterprise.P2p.Models.Core.PeerId>(p => p.Value == remotePeerGuid), Capture.In(capturedDirectSends), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var mockP2p = new Mock<IP2pAlgorithm>();
        mockP2p.SetReturnsDefault(Task.CompletedTask);

        var mockDistribution = new Mock<IShardDistributionManager>();
        mockDistribution.Setup(d => d.IsShardLocalAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string shardId, CancellationToken _) => new ValueTask<bool>(shardId == "system-document-registry"));
        mockDistribution.Setup(d => d.GetTargetNodesForShardAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new[] { "ReplicaB" });

        var sp = BuildNode(s =>
        {
            s.Replace(ServiceDescriptor.Scoped(_ => mockDistribution.Object));
            s.AddSingleton(mockSender.Object);
            s.Replace(ServiceDescriptor.Singleton(mockP2p.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        scope.ClusterTracker.UpdatePeerState("ReplicaB", remotePeerGuid.ToString(), new ShardVersionVectorMap());

        var dummyOp = default(CrdtOperation) with { Id = Guid.NewGuid(), ReplicaId = "ReplicaA", JsonPath = "$.value", ShardId = "default", ShardClock = 1 };
        var patch = new CrdtPatch([dummyOp]);

        // Act
        await orchestrator.ApplyPatchAsync<DynamicTestState>("remote-patch-doc", patch, CancellationToken.None);

        // Assert - Sent directly to ReplicaB's PeerId and broadcasted to the mesh
        capturedDirectSends.Count.ShouldBe(1);
        var serializer = sp.GetRequiredService<Ama.CRDT.Services.Serialization.ICrdtSerializer>();
        var wrapper = serializer.DeserializeFromBytes<Models.CrdtMessageWrapper>(capturedDirectSends[0].ToArray());
        wrapper.DocumentId.ShouldBe("remote-patch-doc");
        wrapper.MessageType.ShouldBe("CrdtPatch");
        wrapper.DocumentTypeAlias.ShouldBe("dynamic-type");

        mockP2p.Verify(p => p.BroadcastAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [IntegrationFact]
    public async Task Orchestrator_InitializationStage_ShouldTransitionFromNotStartedToInitializingToInitialized()
    {
        // Arrange
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var enteredLoadDocument = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var mockStorage = new Mock<IDistributedCrdtStorage>();
        mockStorage.Setup(s => s.LoadDocumentAsync<CrdtRegistryState>(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                enteredLoadDocument.TrySetResult();
                await tcs.Task;
                return (CrdtDocument<CrdtRegistryState>?)null;
            });

        var sp = BuildNode(s =>
        {
            s.Replace(ServiceDescriptor.Singleton(mockStorage.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        // Assert initial stage
        orchestrator.InitializationStage.ShouldBe(OrchestratorInitializationStage.NotStarted);

        // Act
        var initTask = orchestrator.InitializeAsync(CancellationToken.None);
        await enteredLoadDocument.Task;

        // Assert Initializing stage
        orchestrator.InitializationStage.ShouldBe(OrchestratorInitializationStage.Initializing);

        tcs.SetResult();
        await initTask;

        // Assert Initialized stage
        orchestrator.InitializationStage.ShouldBe(OrchestratorInitializationStage.Initialized);
    }

    [IntegrationFact]
    public async Task CrdtInitializationService_ShouldRetryOnTransientFailure_AndSucceed()
    {
        // Arrange
        int attempts = 0;
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        mockStorage.Setup(s => s.LoadShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                attempts++;
                if (attempts < 3)
                {
                    throw new InvalidOperationException("Transient storage error.");
                }
                return Task.FromResult<ShardVersionVectorMap?>(null);
            });
        mockStorage.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
            .Returns(AsyncEnumerable.Empty<JournaledOperation>());

        var sp = BuildNode(s =>
        {
            s.Configure<DistributedCrdtOptions>(o =>
            {
                o.InitializationMaxRetries = 3;
                o.InitializationRetryDelayMilliseconds = 10;
            });
            s.Replace(ServiceDescriptor.Singleton(mockStorage.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().Single();

        // Act
        await initService.StartAsync(CancellationToken.None);

        // Assert
        attempts.ShouldBe(3);
        scope.Orchestrator.InitializationStage.ShouldBe(OrchestratorInitializationStage.Initialized);
    }

    [IntegrationFact]
    public async Task CrdtInitializationService_ShouldThrowAndStopApplication_WhenAllRetriesFail()
    {
        // Arrange
        int attempts = 0;
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        mockStorage.Setup(s => s.LoadShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                attempts++;
                throw new InvalidOperationException("Persistent storage failure.");
            });

        var mockLifetime = new Mock<IHostApplicationLifetime>();

        var sp = BuildNode(s =>
        {
            s.Configure<DistributedCrdtOptions>(o =>
            {
                o.InitializationMaxRetries = 3;
                o.InitializationRetryDelayMilliseconds = 5;
            });
            s.Replace(ServiceDescriptor.Singleton(mockStorage.Object));
            s.AddSingleton(mockLifetime.Object);
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().Single();

        // Act & Assert
        await Should.ThrowAsync<InvalidOperationException>(() => initService.StartAsync(CancellationToken.None));

        attempts.ShouldBe(3);
        mockLifetime.Verify(l => l.StopApplication(), Times.Once);
        scope.Orchestrator.InitializationStage.ShouldBe(OrchestratorInitializationStage.NotStarted);
    }
}