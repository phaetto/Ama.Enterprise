namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Attributes;
using Ama.CRDT.Attributes.Strategies;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;
using Xunit;

public sealed class LegacyTaskItemV1 : IEquatable<LegacyTaskItemV1>, IExtensibleDistributedPayload
{
    public string Title { get; set; } = string.Empty;
    public bool Completed { get; set; }

    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    public bool Equals(LegacyTaskItemV1? other)
    {
        if (other is null) return false;
        return string.Equals(Title, other.Title, StringComparison.Ordinal) && Completed == other.Completed;
    }

    public override bool Equals(object? obj) => Equals(obj as LegacyTaskItemV1);
    public override int GetHashCode() => HashCode.Combine(Title, Completed);
}

public sealed class LegacyTaskListStateV1 : IEquatable<LegacyTaskListStateV1>, IExtensibleDistributedPayload
{
    public string Id { get; set; } = "default";

    [CrdtLwwMapStrategy]
    public Dictionary<string, LegacyTaskItemV1> Items { get; set; } = new(StringComparer.Ordinal);

    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    public bool Equals(LegacyTaskListStateV1? other)
    {
        if (other is null) return false;
        if (!string.Equals(Id, other.Id, StringComparison.Ordinal) || Items.Count != other.Items.Count) return false;
        foreach (var kvp in Items)
        {
            if (!other.Items.TryGetValue(kvp.Key, out var val) || !kvp.Value.Equals(val)) return false;
        }
        return true;
    }

    public override bool Equals(object? obj) => Equals(obj as LegacyTaskListStateV1);
    public override int GetHashCode() => HashCode.Combine(Id, Items.Count);
}

public sealed class ModernTaskItemV2 : IEquatable<ModernTaskItemV2>, IExtensibleDistributedPayload
{
    public string Description { get; set; } = string.Empty;
    public string Status { get; set; } = "Pending";
    public int SchemaVersion { get; set; } = 2;

    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    public bool Equals(ModernTaskItemV2? other)
    {
        if (other is null) return false;
        return string.Equals(Description, other.Description, StringComparison.Ordinal) &&
               string.Equals(Status, other.Status, StringComparison.Ordinal) &&
               SchemaVersion == other.SchemaVersion;
    }

    public override bool Equals(object? obj) => Equals(obj as ModernTaskItemV2);
    public override int GetHashCode() => HashCode.Combine(Description, Status, SchemaVersion);
}

public sealed class ModernTaskListStateV2 : IEquatable<ModernTaskListStateV2>, IExtensibleDistributedPayload
{
    public string Id { get; set; } = "default";

    [CrdtLwwMapStrategy]
    public Dictionary<string, ModernTaskItemV2> Tasks { get; set; } = new(StringComparer.Ordinal);

    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    public bool Equals(ModernTaskListStateV2? other)
    {
        if (other is null) return false;
        if (!string.Equals(Id, other.Id, StringComparison.Ordinal) || Tasks.Count != other.Tasks.Count) return false;
        foreach (var kvp in Tasks)
        {
            if (!other.Tasks.TryGetValue(kvp.Key, out var val) || !kvp.Value.Equals(val)) return false;
        }
        return true;
    }

    public override bool Equals(object? obj) => Equals(obj as ModernTaskListStateV2);
    public override int GetHashCode() => HashCode.Combine(Id, Tasks.Count);
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(LegacyTaskItemV1))]
[JsonSerializable(typeof(LegacyTaskListStateV1))]
[JsonSerializable(typeof(CrdtDocument<LegacyTaskListStateV1>))]
[JsonSerializable(typeof(ModernTaskItemV2))]
[JsonSerializable(typeof(ModernTaskListStateV2))]
[JsonSerializable(typeof(CrdtDocument<ModernTaskListStateV2>))]
internal partial class LiveMigrationTestJsonContext : JsonSerializerContext
{
}

[CrdtAotType(typeof(LegacyTaskItemV1))]
[CrdtAotType(typeof(LegacyTaskListStateV1))]
[CrdtAotType(typeof(Dictionary<string, LegacyTaskItemV1>))]
[CrdtAotType(typeof(ModernTaskItemV2))]
[CrdtAotType(typeof(ModernTaskListStateV2))]
[CrdtAotType(typeof(Dictionary<string, ModernTaskItemV2>))]
internal partial class LiveMigrationTestAotContext : CrdtAotContext
{
}

public sealed class LegacyToModernTaskMigrator : ICrdtDocumentMigrator<LegacyTaskListStateV1, ModernTaskListStateV2>
{
    public async Task MigrateDocumentAsync(
        string targetDocumentId,
        CrdtDocument<LegacyTaskListStateV1> sourceDocument,
        CrdtDocument<LegacyTaskListStateV1>? previousSourceDocument,
        CrdtDocument<ModernTaskListStateV2> targetDocument,
        ICrdtDocumentOrchestrator targetOrchestrator,
        IntentDeliveryMode deliveryMode,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetDocumentId);
        ArgumentNullException.ThrowIfNull(targetOrchestrator);

        if (sourceDocument.Data is null)
        {
            return;
        }

        var existingTasks = targetDocument.Data?.Tasks;
        var previousItems = previousSourceDocument?.Data?.Items;

        // 1. Upsert added or modified items from V1 -> V2
        foreach (var kvp in sourceDocument.Data.Items)
        {
            var expectedV2 = new ModernTaskItemV2
            {
                Description = kvp.Value.Title,
                Status = kvp.Value.Completed ? "Done" : "Pending",
                SchemaVersion = 2
            };

            if (existingTasks is not null &&
                existingTasks.TryGetValue(kvp.Key, out var currentV2) &&
                currentV2.Equals(expectedV2))
            {
                continue;
            }

            await targetOrchestrator.ApplyIntentAsync<ModernTaskListStateV2>(
                targetDocumentId,
                new MapSetIntent(kvp.Key, expectedV2),
                nameof(ModernTaskListStateV2.Tasks),
                deliveryMode,
                cancellationToken).ConfigureAwait(false);
        }

        // 2. Three-way merge removal: only delete from V2 if the key previously existed in V1
        // and was deleted from V1, preserving keys created directly in V2.
        if (previousItems is not null && existingTasks is not null)
        {
            foreach (var previousKey in previousItems.Keys)
            {
                if (!sourceDocument.Data.Items.ContainsKey(previousKey) &&
                    existingTasks.ContainsKey(previousKey))
                {
                    await targetOrchestrator.ApplyIntentAsync<ModernTaskListStateV2>(
                        targetDocumentId,
                        new MapRemoveIntent(previousKey),
                        nameof(ModernTaskListStateV2.Tasks),
                        deliveryMode,
                        cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }
}

public sealed class CustomPrefixAndFaultyTaskMigrator : ICrdtDocumentMigrator<LegacyTaskListStateV1, ModernTaskListStateV2>
{
    private readonly LegacyToModernTaskMigrator innerMigrator = new();

    public string MapDocumentId(string sourceDocumentId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceDocumentId);
        return $"v2-{sourceDocumentId}";
    }

    public Task MigrateDocumentAsync(
        string targetDocumentId,
        CrdtDocument<LegacyTaskListStateV1> sourceDocument,
        CrdtDocument<LegacyTaskListStateV1>? previousSourceDocument,
        CrdtDocument<ModernTaskListStateV2> targetDocument,
        ICrdtDocumentOrchestrator targetOrchestrator,
        IntentDeliveryMode deliveryMode,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetDocumentId);
        ArgumentNullException.ThrowIfNull(targetOrchestrator);

        if (sourceDocument.Data is not null && sourceDocument.Data.Items.ContainsKey("poison"))
        {
            throw new InvalidOperationException($"Simulated translation failure for document '{targetDocumentId}'.");
        }

        return innerMigrator.MigrateDocumentAsync(
            targetDocumentId,
            sourceDocument,
            previousSourceDocument,
            targetDocument,
            targetOrchestrator,
            deliveryMode,
            cancellationToken);
    }
}

public sealed class LiveMigrationIntegrationTests
{
    [Fact]
    public async Task MediatorNode_BackfillsExistingData_StreamsLiveChanges_AndDrainsWithoutDataLoss()
    {
        // Arrange: Set up a multi-mesh simulated network with Mesh V1 (legacy) and Mesh V2 (modern)
        var networkHandlers = new ConcurrentDictionary<(string MeshId, string ReplicaId), IApplicationPayloadHandler>();
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(options =>
        {
            options.ActiveSyncEnabled = true;
            options.EnableSharding = false;
            options.AntiEntropyInitialDelaySeconds = 60;
            options.AntiEntropyIntervalSeconds = 60;
        });

        services.AddCrdt()
            .AddCrdtJsonTypeInfoResolver(LiveMigrationTestJsonContext.Default)
            .AddCrdtAotContext(new LiveMigrationTestAotContext())
            .AddCrdtSerializableType<LegacyTaskItemV1>("legacy-task-v1")
            .AddCrdtSerializableType<ModernTaskItemV2>("modern-task-v2");

        // Register both V1 and V2 document types
        services.AddDistributedDocumentType<LegacyTaskListStateV1>("task-list-v1");
        services.AddDistributedDocumentType<ModernTaskListStateV2>("task-list-v2");

        // Register Legacy Replica (V1) and Modern Replica (V2) on the Mediator host
        RegisterInMemoryMeshMocks(services, "mesh-v1", "replica-v1", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-v2", "replica-v2", networkHandlers);

        services.AddDistributedCrdtReplica("replica-v1", "mesh-v1");
        services.AddDistributedCrdtReplica("replica-v2", "mesh-v2");

        // Register the Live Migration Bridge from replica-v1 (task-list-v1) -> replica-v2 (task-list-v2)
        services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
        {
            options.MigrationId = "tasks-v1-to-v2";
            options.SourceReplicaId = "replica-v1";
            options.SourceTypeAlias = "task-list-v1";
            options.TargetReplicaId = "replica-v2";
            options.TargetTypeAlias = "task-list-v2";
            options.BackfillBatchDelayMilliseconds = 5;
            options.ReconciliationScanIntervalSeconds = 1;
        });

        await using var provider = services.BuildServiceProvider();
        var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var scopeV1 = scopeManager.GetOrCreateScope("replica-v1");
        var scopeV2 = scopeManager.GetOrCreateScope("replica-v2");

        var orchestratorV1 = scopeV1.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var orchestratorV2 = scopeV2.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestratorV1.InitializeAsync();
        await orchestratorV2.InitializeAsync();

        // Pre-populate V1 with existing documents BEFORE starting the migration hosted service (simulating existing production data)
        await orchestratorV1.CreateDocumentAsync("project-alpha", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "project-alpha",
            new MapSetIntent("t1", new LegacyTaskItemV1 { Title = "Write specs", Completed = true }),
            nameof(LegacyTaskListStateV1.Items));
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "project-alpha",
            new MapSetIntent("t2", new LegacyTaskItemV1 { Title = "Implement V1", Completed = false }),
            nameof(LegacyTaskListStateV1.Items));

        await orchestratorV1.CreateDocumentAsync("project-beta", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "project-beta",
            new MapSetIntent("b1", new LegacyTaskItemV1 { Title = "Beta item", Completed = false }),
            nameof(LegacyTaskListStateV1.Items));

        // Act 1: Start hosted services (including CrdtMigrationBridgeService) to perform background backfill
        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        var migrationManager = provider.GetRequiredService<ICrdtMigrationManager>();
        await WaitUntilAsync(() =>
        {
            return migrationManager.TryGetStatus("tasks-v1-to-v2", out var status) &&
                   status.IsConverged &&
                   status.BackfilledDocuments >= 2;
        }, TimeSpan.FromSeconds(5));

        // Verify Phase 2 (Backfill): Both existing documents exist in V2 with transformed schema
        var alphaV2 = await orchestratorV2.GetDocumentStateAsync<ModernTaskListStateV2>("project-alpha");
        alphaV2.Data.ShouldNotBeNull();
        alphaV2.Data.Tasks.Count.ShouldBe(2);
        alphaV2.Data.Tasks["t1"].Description.ShouldBe("Write specs");
        alphaV2.Data.Tasks["t1"].Status.ShouldBe("Done");
        alphaV2.Data.Tasks["t2"].Description.ShouldBe("Implement V1");
        alphaV2.Data.Tasks["t2"].Status.ShouldBe("Pending");

        // Create a V2-native task directly on V2 while migration is still active
        await orchestratorV2.ApplyIntentAsync<ModernTaskListStateV2>(
            "project-alpha",
            new MapSetIntent("v2-native-task", new ModernTaskItemV2 { Description = "Created directly in V2", Status = "Active", SchemaVersion = 2 }),
            nameof(ModernTaskListStateV2.Tasks));

        // Act 2: Perform live mutations on V1 while the system is running (update, item delete, new doc, doc tombstone)
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "project-alpha",
            new MapSetIntent("t2", new LegacyTaskItemV1 { Title = "Implement V1", Completed = true }),
            nameof(LegacyTaskListStateV1.Items));

        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "project-alpha",
            new MapRemoveIntent("t1"),
            nameof(LegacyTaskListStateV1.Items));

        await orchestratorV1.CreateDocumentAsync("project-gamma", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "project-gamma",
            new MapSetIntent("g1", new LegacyTaskItemV1 { Title = "Live created doc", Completed = true }),
            nameof(LegacyTaskListStateV1.Items));

        await orchestratorV1.DeleteDocumentAsync("project-beta", "task-list-v1");

        // Act 3: Drain and flush before cutting over / closing V1
        await migrationManager.DrainAllAsync();

        // Assert: Verify all live changes and deletions converged onto V2 without data loss
        migrationManager.AreAllConverged().ShouldBeTrue();
        migrationManager.TryGetStatus("tasks-v1-to-v2", out var finalStatus).ShouldBeTrue();
        finalStatus.FailedTranslations.ShouldBe(0);
        finalStatus.PendingDocumentsInQueue.ShouldBe(0);

        var updatedAlphaV2 = await orchestratorV2.GetDocumentStateAsync<ModernTaskListStateV2>("project-alpha");
        updatedAlphaV2.Data.ShouldNotBeNull();
        updatedAlphaV2.Data.Tasks.Count.ShouldBe(2);
        updatedAlphaV2.Data.Tasks.ContainsKey("t1").ShouldBeFalse();
        updatedAlphaV2.Data.Tasks["t2"].Status.ShouldBe("Done");
        updatedAlphaV2.Data.Tasks.ShouldContainKey("v2-native-task");
        updatedAlphaV2.Data.Tasks["v2-native-task"].Description.ShouldBe("Created directly in V2");

        var gammaV2 = await orchestratorV2.GetDocumentStateAsync<ModernTaskListStateV2>("project-gamma");
        gammaV2.Data.ShouldNotBeNull();
        gammaV2.Data.Tasks.Count.ShouldBe(1);
        gammaV2.Data.Tasks["g1"].Description.ShouldBe("Live created doc");
        gammaV2.Data.Tasks["g1"].Status.ShouldBe("Done");

        var betaExistsInV2 = await orchestratorV2.DocumentExistsAsync("project-beta", "task-list-v2");
        betaExistsInV2.ShouldBeFalse();

        foreach (var hs in hostedServices)
        {
            await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task MediatorNode_WhenSourceDocumentliesOnRemoteShard_FetchesStateViaRpcAndMigratesToTargetReplica()
    {
        // Arrange - Mediator's source replica ("replica-v1-mediator") does NOT host the data shard locally;
        // the source document is hosted on a remote V1 shard owner ("replica-v1-remote-owner").
        var networkHandlers = new ConcurrentDictionary<(string MeshId, string ReplicaId), IApplicationPayloadHandler>();
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(options =>
        {
            options.ActiveSyncEnabled = true;
            options.EnableSharding = true;
            options.AntiEntropyInitialDelaySeconds = 60;
            options.AntiEntropyIntervalSeconds = 60;
        });

        services.AddCrdt()
            .AddCrdtJsonTypeInfoResolver(LiveMigrationTestJsonContext.Default)
            .AddCrdtAotContext(new LiveMigrationTestAotContext())
            .AddCrdtSerializableType<LegacyTaskItemV1>("legacy-task-v1")
            .AddCrdtSerializableType<ModernTaskItemV2>("modern-task-v2");

        services.AddDistributedDocumentType<LegacyTaskListStateV1>("task-list-v1");
        services.AddDistributedDocumentType<ModernTaskListStateV2>("task-list-v2");

        RegisterInMemoryMeshMocks(services, "mesh-v1", "replica-v1-mediator", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-v2", "replica-v2-target", networkHandlers);

        // Configure custom distribution manager on the mediator so "replica-v1-mediator" considers data shards remote
        // (owned by "replica-v1-remote-owner"), while "replica-v2-target" considers all shards local.
        services.AddScoped<IShardDistributionManager>(sp =>
        {
            var replicaCtx = sp.GetRequiredService<ReplicaContext>();
            var mockDist = new Mock<IShardDistributionManager>();
            if (string.Equals(replicaCtx.ReplicaId, "replica-v1-mediator", StringComparison.Ordinal))
            {
                mockDist.Setup(d => d.IsShardLocalAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .Returns((string shardId, CancellationToken _) =>
                        new ValueTask<bool>(string.Equals(shardId, Constants.SystemRegistryDocumentId, StringComparison.Ordinal)));
                mockDist.Setup(d => d.GetTargetNodesForShardAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new[] { "replica-v1-remote-owner" });
            }
            else
            {
                mockDist.Setup(d => d.IsShardLocalAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .Returns(new ValueTask<bool>(true));
                mockDist.Setup(d => d.GetTargetNodesForShardAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new[] { replicaCtx.ReplicaId });
            }

            return mockDist.Object;
        });

        var remoteOwnerPeerId = new PeerId(Guid.NewGuid());
        var directSenderMock = new Mock<IDirectMessageSender>();
        services.AddSingleton(directSenderMock.Object);
        services.AddSingleton<IP2pRpcCorrelator, P2pRpcCorrelator>();

        services.AddDistributedCrdtReplica("replica-v1-mediator", "mesh-v1");
        services.AddDistributedCrdtReplica("replica-v2-target", "mesh-v2");

        services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
        {
            options.MigrationId = "remote-shard-rpc-migration";
            options.SourceReplicaId = "replica-v1-mediator";
            options.SourceTypeAlias = "task-list-v1";
            options.TargetReplicaId = "replica-v2-target";
            options.TargetTypeAlias = "task-list-v2";
            options.PartitionBackfillByPrimaryShardOwner = false;
            options.BackfillBatchDelayMilliseconds = 1;
        });

        await using var provider = services.BuildServiceProvider();
        var serializer = provider.GetRequiredService<Ama.CRDT.Services.Serialization.ICrdtSerializer>();
        var handlerV1 = provider.GetRequiredKeyedService<IApplicationPayloadHandler>("mesh-v1");

        var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var scopeV1 = scopeManager.GetOrCreateScope("replica-v1-mediator");
        var scopeV2 = scopeManager.GetOrCreateScope("replica-v2-target");

        var metadataManager = scopeV1.ServiceProvider.GetRequiredService<Ama.CRDT.Services.ICrdtMetadataManager>();
        var remoteDocState = new LegacyTaskListStateV1 { Id = "remote-owned-doc" };
        remoteDocState.Items["r1"] = new LegacyTaskItemV1 { Title = "Fetched over P2P RPC", Completed = true };
        var remoteCrdtDoc = new CrdtDocument<LegacyTaskListStateV1>(remoteDocState, metadataManager.Initialize(remoteDocState));
        var remoteDocBytes = serializer.SerializeToBytes(remoteCrdtDoc);

        // Intercept CrdtReadReq sent to remoteOwnerPeerId and reply with CrdtReadRes
        directSenderMock
            .Setup(s => s.SendDirectAsync(It.Is<PeerId>(p => p == remoteOwnerPeerId), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns((PeerId _, ReadOnlyMemory<byte> payload, CancellationToken _) =>
            {
                var wrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(payload.ToArray());
                if (wrapper.MessageType == "CrdtReadReq")
                {
                    var req = serializer.DeserializeFromBytes<CrdtReadRequestDto>(wrapper.Payload!);
                    _ = Task.Run(async () =>
                    {
                        var res = new CrdtReadResponseDto
                        {
                            CorrelationId = req.CorrelationId,
                            StatePayload = remoteDocBytes
                        };
                        var resWrapper = new CrdtMessageWrapper(wrapper.DocumentId, "CrdtReadRes", serializer.SerializeToBytes(res), "replica-v1-remote-owner");
                        await handlerV1.HandlePayloadAsync("mesh-v1", remoteOwnerPeerId, serializer.SerializeToBytes(resWrapper), CancellationToken.None);
                    });
                }

                return Task.CompletedTask;
            });

        var orchestratorV1 = scopeV1.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var orchestratorV2 = scopeV2.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestratorV1.InitializeAsync();
        await orchestratorV2.InitializeAsync();

        scopeV1.ClusterTracker.UpdatePeerState("replica-v1-remote-owner", remoteOwnerPeerId.Value.ToString(), new ShardVersionVectorMap());

        // Register the document in V1's global registry (since the shard is remote, it is NOT instantiated locally on replica-v1-mediator)
        await orchestratorV1.CreateDocumentAsync("remote-owned-doc", "task-list-v1");
        orchestratorV1.TryGetLocalDocument<LegacyTaskListStateV1>("remote-owned-doc").ShouldBeNull();

        // Act
        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        var migrationManager = provider.GetRequiredService<ICrdtMigrationManager>();
        await migrationManager.DrainAllAsync();

        // Assert - Migration bridge fetched the non-local V1 document via GetDocumentStateAsync RPC and translated it into V2
        var migratedDocV2 = await orchestratorV2.GetDocumentStateAsync<ModernTaskListStateV2>("remote-owned-doc");
        migratedDocV2.Data.ShouldNotBeNull();
        migratedDocV2.Data.Tasks.ShouldContainKey("r1");
        migratedDocV2.Data.Tasks["r1"].Description.ShouldBe("Fetched over P2P RPC");
        migratedDocV2.Data.Tasks["r1"].Status.ShouldBe("Done");

        foreach (var hs in hostedServices)
        {
            await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task MediatorNode_EnforcesStrictlyUnidirectionalFlow_MutationsAndDeletionsOnNewReplica_NeverLeakBackToOldReplica()
    {
        // Arrange: Set up V1 and V2 sharing the same document Type ("task-list-v2") or distinct types to verify
        // that observer subscriptions, document creations, updates, and deletions on V2 NEVER flow back to V1.
        var networkHandlers = new ConcurrentDictionary<(string MeshId, string ReplicaId), IApplicationPayloadHandler>();
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(options =>
        {
            options.ActiveSyncEnabled = true;
            options.EnableSharding = false;
            options.AntiEntropyInitialDelaySeconds = 60;
            options.AntiEntropyIntervalSeconds = 60;
        });

        services.AddCrdt()
            .AddCrdtJsonTypeInfoResolver(LiveMigrationTestJsonContext.Default)
            .AddCrdtAotContext(new LiveMigrationTestAotContext())
            .AddCrdtSerializableType<LegacyTaskItemV1>("legacy-task-v1")
            .AddCrdtSerializableType<ModernTaskItemV2>("modern-task-v2");

        services.AddDistributedDocumentType<LegacyTaskListStateV1>("task-list-v1");
        services.AddDistributedDocumentType<ModernTaskListStateV2>("task-list-v2");

        RegisterInMemoryMeshMocks(services, "mesh-v1", "replica-v1", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-v2", "replica-v2", networkHandlers);

        services.AddDistributedCrdtReplica("replica-v1", "mesh-v1");
        services.AddDistributedCrdtReplica("replica-v2", "mesh-v2");

        services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
        {
            options.MigrationId = "strict-unidirectional-flow";
            options.SourceReplicaId = "replica-v1";
            options.SourceTypeAlias = "task-list-v1";
            options.TargetReplicaId = "replica-v2";
            options.TargetTypeAlias = "task-list-v2";
            options.BackfillBatchDelayMilliseconds = 1;
        });

        await using var provider = services.BuildServiceProvider();
        var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var scopeV1 = scopeManager.GetOrCreateScope("replica-v1");
        var scopeV2 = scopeManager.GetOrCreateScope("replica-v2");

        var orchestratorV1 = scopeV1.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var orchestratorV2 = scopeV2.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestratorV1.InitializeAsync();
        await orchestratorV2.InitializeAsync();

        // Populate initial V1 documents
        await orchestratorV1.CreateDocumentAsync("shared-doc", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "shared-doc",
            new MapSetIntent("item-1", new LegacyTaskItemV1 { Title = "V1 Original Title", Completed = false }),
            nameof(LegacyTaskListStateV1.Items));

        await orchestratorV1.CreateDocumentAsync("doc-deleted-in-v2-only", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "doc-deleted-in-v2-only",
            new MapSetIntent("k1", new LegacyTaskItemV1 { Title = "Stay alive in V1", Completed = true }),
            nameof(LegacyTaskListStateV1.Items));

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        var migrationManager = provider.GetRequiredService<ICrdtMigrationManager>();
        await migrationManager.DrainAllAsync();

        // Snapshot V1 state and DVV clock before mutating V2
        var v1DocBefore = await orchestratorV1.GetDocumentStateAsync<LegacyTaskListStateV1>("shared-doc");
        var v1VersionVectorSnapshot = new Dictionary<string, long>(v1DocBefore.Metadata.VersionVector, StringComparer.Ordinal);

        // Act: Perform extensive mutations directly on the NEW replica (V2)
        // 1. Modify a migrated item directly in V2
        await orchestratorV2.ApplyIntentAsync<ModernTaskListStateV2>(
            "shared-doc",
            new MapSetIntent("item-1", new ModernTaskItemV2 { Description = "Overridden in V2", Status = "Done", SchemaVersion = 2 }),
            nameof(ModernTaskListStateV2.Tasks));

        // 2. Add a brand new item to the migrated document in V2
        await orchestratorV2.ApplyIntentAsync<ModernTaskListStateV2>(
            "shared-doc",
            new MapSetIntent("item-v2-only", new ModernTaskItemV2 { Description = "Only in V2", Status = "Pending", SchemaVersion = 2 }),
            nameof(ModernTaskListStateV2.Tasks));

        // 3. Create a brand new document directly in V2
        await orchestratorV2.CreateDocumentAsync("v2-exclusive-doc", "task-list-v2");
        await orchestratorV2.ApplyIntentAsync<ModernTaskListStateV2>(
            "v2-exclusive-doc",
            new MapSetIntent("new-k", new ModernTaskItemV2 { Description = "Born in V2", Status = "Done", SchemaVersion = 2 }),
            nameof(ModernTaskListStateV2.Tasks));

        // 4. Delete a migrated document in V2
        await orchestratorV2.DeleteDocumentAsync("doc-deleted-in-v2-only", "task-list-v2");

        await migrationManager.DrainAllAsync();

        // Assert 1: V1 must be completely untouched by any V2 mutations, additions, or document deletions
        var v1ExclusiveExistsInV1 = await orchestratorV1.DocumentExistsAsync("v2-exclusive-doc", "task-list-v1");
        v1ExclusiveExistsInV1.ShouldBeFalse();

        var v1DeletedInV2StillExistsInV1 = await orchestratorV1.DocumentExistsAsync("doc-deleted-in-v2-only", "task-list-v1");
        v1DeletedInV2StillExistsInV1.ShouldBeTrue();
        var v1SurvivingDoc = await orchestratorV1.GetDocumentStateAsync<LegacyTaskListStateV1>("doc-deleted-in-v2-only");
        v1SurvivingDoc.Data.ShouldNotBeNull();
        v1SurvivingDoc.Data.Items.ShouldContainKey("k1");
        v1SurvivingDoc.Data.Items["k1"].Title.ShouldBe("Stay alive in V1");

        var v1DocAfterV2Mutations = await orchestratorV1.GetDocumentStateAsync<LegacyTaskListStateV1>("shared-doc");
        v1DocAfterV2Mutations.Data.ShouldNotBeNull();
        v1DocAfterV2Mutations.Data.Items.Count.ShouldBe(1);
        v1DocAfterV2Mutations.Data.Items.ContainsKey("item-v2-only").ShouldBeFalse();
        v1DocAfterV2Mutations.Data.Items["item-1"].Title.ShouldBe("V1 Original Title");
        v1DocAfterV2Mutations.Data.Items["item-1"].Completed.ShouldBeFalse();
        v1DocAfterV2Mutations.Metadata.VersionVector.ShouldBe(v1VersionVectorSnapshot);

        // Act 2: Now mutate a DIFFERENT key ("item-2") on V1 for "shared-doc" and verify V1 -> V2 still flows
        // without reverting "item-v2-only" on V2 or polluting V1.
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "shared-doc",
            new MapSetIntent("item-2", new LegacyTaskItemV1 { Title = "Second V1 Item", Completed = true }),
            nameof(LegacyTaskListStateV1.Items));

        await migrationManager.DrainAllAsync();

        var v2SharedDocFinal = await orchestratorV2.GetDocumentStateAsync<ModernTaskListStateV2>("shared-doc");
        v2SharedDocFinal.Data.ShouldNotBeNull();
        v2SharedDocFinal.Data.Tasks.ShouldContainKey("item-2");
        v2SharedDocFinal.Data.Tasks["item-2"].Description.ShouldBe("Second V1 Item");
        v2SharedDocFinal.Data.Tasks["item-2"].Status.ShouldBe("Done");
        v2SharedDocFinal.Data.Tasks.ShouldContainKey("item-v2-only");

        var v1SharedDocFinal = await orchestratorV1.GetDocumentStateAsync<LegacyTaskListStateV1>("shared-doc");
        v1SharedDocFinal.Data.ShouldNotBeNull();
        v1SharedDocFinal.Data.Items.Count.ShouldBe(2);
        v1SharedDocFinal.Data.Items.ContainsKey("item-v2-only").ShouldBeFalse();

        foreach (var hs in hostedServices)
        {
            await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task MediatorNode_SameStateTypeAcrossTwoReplicas_OnlyObservesSourceReplica_AndNeverReverseTriggersOnTargetReplicaMutations()
    {
        // Arrange: Both source ("replica-legacy-cluster") and target ("replica-modern-cluster") use the EXACT SAME C# state type
        // (ModernTaskListStateV2). This tests that MigrationSourceStateObserver<TState> strictly filters by ReplicaContext.ReplicaId
        // and never triggers reverse or infinite-loop migrations when the target replica mutates.
        var networkHandlers = new ConcurrentDictionary<(string MeshId, string ReplicaId), IApplicationPayloadHandler>();
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(options =>
        {
            options.ActiveSyncEnabled = true;
            options.EnableSharding = false;
            options.AntiEntropyInitialDelaySeconds = 60;
            options.AntiEntropyIntervalSeconds = 60;
        });

        services.AddCrdt()
            .AddCrdtJsonTypeInfoResolver(LiveMigrationTestJsonContext.Default)
            .AddCrdtAotContext(new LiveMigrationTestAotContext())
            .AddCrdtSerializableType<ModernTaskItemV2>("modern-task-v2");

        services.AddDistributedDocumentType<ModernTaskListStateV2>("task-list-same-type");

        RegisterInMemoryMeshMocks(services, "mesh-legacy", "replica-legacy-cluster", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-modern", "replica-modern-cluster", networkHandlers);

        services.AddDistributedCrdtReplica("replica-legacy-cluster", "mesh-legacy");
        services.AddDistributedCrdtReplica("replica-modern-cluster", "mesh-modern");

        services.AddDistributedCrdtMigration<ModernTaskListStateV2, ModernTaskListStateV2, SameTypePassthroughTaskMigrator>(options =>
        {
            options.MigrationId = "same-type-cluster-migration";
            options.SourceReplicaId = "replica-legacy-cluster";
            options.SourceTypeAlias = "task-list-same-type";
            options.TargetReplicaId = "replica-modern-cluster";
            options.TargetTypeAlias = "task-list-same-type";
            options.BackfillBatchDelayMilliseconds = 1;
        });

        await using var provider = services.BuildServiceProvider();
        var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var scopeLegacy = scopeManager.GetOrCreateScope("replica-legacy-cluster");
        var scopeModern = scopeManager.GetOrCreateScope("replica-modern-cluster");

        var orchestratorLegacy = scopeLegacy.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var orchestratorModern = scopeModern.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestratorLegacy.InitializeAsync();
        await orchestratorModern.InitializeAsync();

        await orchestratorLegacy.CreateDocumentAsync("cluster-doc-1", "task-list-same-type");
        await orchestratorLegacy.ApplyIntentAsync<ModernTaskListStateV2>(
            "cluster-doc-1",
            new MapSetIntent("t1", new ModernTaskItemV2 { Description = "From Legacy Cluster", Status = "Pending", SchemaVersion = 2 }),
            nameof(ModernTaskListStateV2.Tasks));

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        var migrationManager = provider.GetRequiredService<ICrdtMigrationManager>();
        await migrationManager.DrainAllAsync();

        migrationManager.TryGetStatus("same-type-cluster-migration", out var statusAfterBackfill).ShouldBeTrue();
        var liveEventsBaseline = statusAfterBackfill.LiveEventsProcessed;

        // Act: Mutate and create documents directly on the TARGET replica ("replica-modern-cluster")
        await orchestratorModern.ApplyIntentAsync<ModernTaskListStateV2>(
            "cluster-doc-1",
            new MapSetIntent("modern-only-key", new ModernTaskItemV2 { Description = "Written on Modern Cluster", Status = "Active", SchemaVersion = 2 }),
            nameof(ModernTaskListStateV2.Tasks));

        await orchestratorModern.CreateDocumentAsync("modern-only-doc", "task-list-same-type");
        await orchestratorModern.ApplyIntentAsync<ModernTaskListStateV2>(
            "modern-only-doc",
            new MapSetIntent("m1", new ModernTaskItemV2 { Description = "Never copy to legacy", Status = "Done", SchemaVersion = 2 }),
            nameof(ModernTaskListStateV2.Tasks));

        await migrationManager.DrainAllAsync();

        // Assert: Target mutations must NOT increment live events on the bridge and must NOT appear on the source replica
        migrationManager.TryGetStatus("same-type-cluster-migration", out var statusAfterModernWrites).ShouldBeTrue();
        statusAfterModernWrites.LiveEventsProcessed.ShouldBe(liveEventsBaseline);

        var modernOnlyExistsInLegacy = await orchestratorLegacy.DocumentExistsAsync("modern-only-doc", "task-list-same-type");
        modernOnlyExistsInLegacy.ShouldBeFalse();

        var legacyDoc = await orchestratorLegacy.GetDocumentStateAsync<ModernTaskListStateV2>("cluster-doc-1");
        legacyDoc.Data.ShouldNotBeNull();
        legacyDoc.Data.Tasks.Count.ShouldBe(1);
        legacyDoc.Data.Tasks.ContainsKey("modern-only-key").ShouldBeFalse();
        legacyDoc.Data.Tasks["t1"].Description.ShouldBe("From Legacy Cluster");

        foreach (var hs in hostedServices)
        {
            await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task MultiNodeP2pMeshes_LiveGossipOnMeshV1_FlowsThroughMediator_AndBroadcastsToRemoteNodeOnMeshV2_WithoutReversePollution()
    {
        // Arrange: 3 distinct hosts connected over simulated P2P networks:
        //   Host A: Pure Legacy V1 Node (on "mesh-v1")
        //   Host B: Mediator Node (participating in BOTH "mesh-v1" and "mesh-v2", running the Migration Bridge)
        //   Host C: Pure Modern V2 Node (on "mesh-v2")
        var meshBus = new InMemoryMultiMeshNetworkBus();

        var peerIdNodeV1 = new PeerId(Guid.NewGuid());
        var peerIdMediatorV1 = new PeerId(Guid.NewGuid());
        var peerIdMediatorV2 = new PeerId(Guid.NewGuid());
        var peerIdNodeV2 = new PeerId(Guid.NewGuid());

        await using var hostV1 = await CreateClusterNodeHostAsync(
            meshBus,
            configureReplicasAndMeshes: services =>
            {
                meshBus.RegisterNode(services, "mesh-v1", "node-v1-pure", peerIdNodeV1);
                services.AddDistributedCrdtReplica("node-v1-pure", "mesh-v1");
            });

        await using var hostMediator = await CreateClusterNodeHostAsync(
            meshBus,
            configureReplicasAndMeshes: services =>
            {
                meshBus.RegisterNode(services, "mesh-v1", "mediator-v1", peerIdMediatorV1);
                meshBus.RegisterNode(services, "mesh-v2", "mediator-v2", peerIdMediatorV2);
                services.AddDistributedCrdtReplica("mediator-v1", "mesh-v1");
                services.AddDistributedCrdtReplica("mediator-v2", "mesh-v2");

                services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
                {
                    options.MigrationId = "e2e-mesh-v1-to-v2";
                    options.SourceReplicaId = "mediator-v1";
                    options.SourceTypeAlias = "task-list-v1";
                    options.TargetReplicaId = "mediator-v2";
                    options.TargetTypeAlias = "task-list-v2";
                    options.BackfillBatchDelayMilliseconds = 1;
                });
            });

        await using var hostV2 = await CreateClusterNodeHostAsync(
            meshBus,
            configureReplicasAndMeshes: services =>
            {
                meshBus.RegisterNode(services, "mesh-v2", "node-v2-pure", peerIdNodeV2);
                services.AddDistributedCrdtReplica("node-v2-pure", "mesh-v2");
            });

        meshBus.BindComplete();

        var orchestratorPureV1 = hostV1.GetOrchestrator("node-v1-pure");
        var orchestratorMediatorV1 = hostMediator.GetOrchestrator("mediator-v1");
        var orchestratorMediatorV2 = hostMediator.GetOrchestrator("mediator-v2");
        var orchestratorPureV2 = hostV2.GetOrchestrator("node-v2-pure");

        var migrationManager = hostMediator.ServiceProvider.GetRequiredService<ICrdtMigrationManager>();

        // Act 1: Pure V1 node creates and updates a document; P2P gossip delivers patches to Mediator V1 -> Bridge -> Mediator V2 -> P2P gossip to Pure V2
        await orchestratorPureV1.CreateDocumentAsync("global-roadmap", "task-list-v1");
        await orchestratorPureV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "global-roadmap",
            new MapSetIntent("milestone-1", new LegacyTaskItemV1 { Title = "Launch V1 on Legacy Mesh", Completed = true }),
            nameof(LegacyTaskListStateV1.Items));
        await orchestratorPureV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "global-roadmap",
            new MapSetIntent("milestone-2", new LegacyTaskItemV1 { Title = "Prepare V2 Cutover", Completed = false }),
            nameof(LegacyTaskListStateV1.Items));

        await orchestratorPureV1.FlushAsync();
        await orchestratorMediatorV1.FlushAsync();
        await migrationManager.DrainAllAsync();
        await orchestratorMediatorV2.FlushAsync();
        await orchestratorPureV2.FlushAsync();

        // Wait for Pure V2 node to receive the translated V2 document over mesh-v2
        await WaitUntilAsync(() =>
        {
            var localDoc = orchestratorPureV2.TryGetLocalDocument<ModernTaskListStateV2>("global-roadmap");
            return localDoc != null;
        }, TimeSpan.FromSeconds(5));

        var pureV2Doc = await orchestratorPureV2.GetDocumentStateAsync<ModernTaskListStateV2>("global-roadmap");
        pureV2Doc.Data.ShouldNotBeNull();
        pureV2Doc.Data.Tasks.Count.ShouldBe(2);
        pureV2Doc.Data.Tasks["milestone-1"].Description.ShouldBe("Launch V1 on Legacy Mesh");
        pureV2Doc.Data.Tasks["milestone-1"].Status.ShouldBe("Done");
        pureV2Doc.Data.Tasks["milestone-2"].Description.ShouldBe("Prepare V2 Cutover");
        pureV2Doc.Data.Tasks["milestone-2"].Status.ShouldBe("Pending");

        // Act 2: Pure V2 node writes a V2-native task AND creates a V2-only document on mesh-v2.
        // Those changes gossip to Mediator V2, but must NEVER cross the bridge back to Mediator V1 or Pure V1.
        await orchestratorPureV2.ApplyIntentAsync<ModernTaskListStateV2>(
            "global-roadmap",
            new MapSetIntent("milestone-v2-native", new ModernTaskItemV2 { Description = "Added on Pure V2 Node", Status = "Active", SchemaVersion = 2 }),
            nameof(ModernTaskListStateV2.Tasks));

        await orchestratorPureV2.CreateDocumentAsync("v2-mesh-only-doc", "task-list-v2");
        await orchestratorPureV2.ApplyIntentAsync<ModernTaskListStateV2>(
            "v2-mesh-only-doc",
            new MapSetIntent("k-v2", new ModernTaskItemV2 { Description = "Mesh V2 Exclusive", Status = "Done", SchemaVersion = 2 }),
            nameof(ModernTaskListStateV2.Tasks));

        await orchestratorPureV2.FlushAsync();
        await orchestratorMediatorV2.FlushAsync();
        await migrationManager.DrainAllAsync();

        // Verify Mediator V2 received the V2 gossip from Pure V2
        var mediatorV2Doc = await orchestratorMediatorV2.GetDocumentStateAsync<ModernTaskListStateV2>("global-roadmap");
        mediatorV2Doc.Data.ShouldNotBeNull();
        mediatorV2Doc.Data.Tasks.ShouldContainKey("milestone-v2-native");
        (await orchestratorMediatorV2.DocumentExistsAsync("v2-mesh-only-doc", "task-list-v2")).ShouldBeTrue();

        // Verify neither Mediator V1 nor Pure V1 received any reverse data from mesh-v2
        (await orchestratorMediatorV1.DocumentExistsAsync("v2-mesh-only-doc", "task-list-v1")).ShouldBeFalse();
        (await orchestratorPureV1.DocumentExistsAsync("v2-mesh-only-doc", "task-list-v1")).ShouldBeFalse();

        var pureV1Doc = await orchestratorPureV1.GetDocumentStateAsync<LegacyTaskListStateV1>("global-roadmap");
        pureV1Doc.Data.ShouldNotBeNull();
        pureV1Doc.Data.Items.Count.ShouldBe(2);
        pureV1Doc.Data.Items.ContainsKey("milestone-v2-native").ShouldBeFalse();

        // Act 3: Pure V1 node completes milestone-2 and deletes milestone-1; verify Pure V2 converges while keeping milestone-v2-native
        await orchestratorPureV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "global-roadmap",
            new MapSetIntent("milestone-2", new LegacyTaskItemV1 { Title = "Prepare V2 Cutover", Completed = true }),
            nameof(LegacyTaskListStateV1.Items));
        await orchestratorPureV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "global-roadmap",
            new MapRemoveIntent("milestone-1"),
            nameof(LegacyTaskListStateV1.Items));

        await orchestratorPureV1.FlushAsync();
        await orchestratorMediatorV1.FlushAsync();
        await migrationManager.DrainAllAsync();
        await orchestratorMediatorV2.FlushAsync();
        await orchestratorPureV2.FlushAsync();

        await WaitUntilAsync(() =>
        {
            var doc = orchestratorPureV2.TryGetLocalDocument<ModernTaskListStateV2>("global-roadmap");
            if (doc == null) return false;
            var state = doc.GetDocumentAsync().AsTask().GetAwaiter().GetResult();
            return state.Data != null &&
                   !state.Data.Tasks.ContainsKey("milestone-1") &&
                   state.Data.Tasks.TryGetValue("milestone-2", out var m2) &&
                   m2.Status == "Done";
        }, TimeSpan.FromSeconds(5));

        var convergedPureV2Doc = await orchestratorPureV2.GetDocumentStateAsync<ModernTaskListStateV2>("global-roadmap");
        convergedPureV2Doc.Data.ShouldNotBeNull();
        convergedPureV2Doc.Data.Tasks.Count.ShouldBe(2);
        convergedPureV2Doc.Data.Tasks.ContainsKey("milestone-1").ShouldBeFalse();
        convergedPureV2Doc.Data.Tasks["milestone-2"].Status.ShouldBe("Done");
        convergedPureV2Doc.Data.Tasks.ShouldContainKey("milestone-v2-native");
        convergedPureV2Doc.Data.Tasks["milestone-v2-native"].Description.ShouldBe("Added on Pure V2 Node");
    }

    [Fact]
    public async Task MediatorNode_DocumentDeleteAndRecreateLifecycle_AndCustomIdMappingWithPoisonFaultIsolation_ConvergesReliably()
    {
        // Arrange: Test custom document ID mapping ("v2-{id}"), poison document exception isolation,
        // and full delete -> recreate -> re-migrate -> delete lifecycle.
        var networkHandlers = new ConcurrentDictionary<(string MeshId, string ReplicaId), IApplicationPayloadHandler>();
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(options =>
        {
            options.ActiveSyncEnabled = true;
            options.EnableSharding = false;
            options.AntiEntropyInitialDelaySeconds = 60;
            options.AntiEntropyIntervalSeconds = 60;
        });

        services.AddCrdt()
            .AddCrdtJsonTypeInfoResolver(LiveMigrationTestJsonContext.Default)
            .AddCrdtAotContext(new LiveMigrationTestAotContext())
            .AddCrdtSerializableType<LegacyTaskItemV1>("legacy-task-v1")
            .AddCrdtSerializableType<ModernTaskItemV2>("modern-task-v2");

        services.AddDistributedDocumentType<LegacyTaskListStateV1>("task-list-v1");
        services.AddDistributedDocumentType<ModernTaskListStateV2>("task-list-v2");

        RegisterInMemoryMeshMocks(services, "mesh-v1", "replica-v1", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-v2", "replica-v2", networkHandlers);

        services.AddDistributedCrdtReplica("replica-v1", "mesh-v1");
        services.AddDistributedCrdtReplica("replica-v2", "mesh-v2");

        services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, CustomPrefixAndFaultyTaskMigrator>(options =>
        {
            options.MigrationId = "lifecycle-and-fault-isolation";
            options.SourceReplicaId = "replica-v1";
            options.SourceTypeAlias = "task-list-v1";
            options.TargetReplicaId = "replica-v2";
            options.TargetTypeAlias = "task-list-v2";
            options.BackfillBatchDelayMilliseconds = 1;
        });

        await using var provider = services.BuildServiceProvider();
        var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var scopeV1 = scopeManager.GetOrCreateScope("replica-v1");
        var scopeV2 = scopeManager.GetOrCreateScope("replica-v2");

        var orchestratorV1 = scopeV1.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var orchestratorV2 = scopeV2.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestratorV1.InitializeAsync();
        await orchestratorV2.InitializeAsync();

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        var migrationManager = provider.GetRequiredService<ICrdtMigrationManager>();

        // Act 1: Create a poison document (which throws in CustomPrefixAndFaultyTaskMigrator) and a healthy lifecycle document
        await orchestratorV1.CreateDocumentAsync("bad-doc", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "bad-doc",
            new MapSetIntent("poison", new LegacyTaskItemV1 { Title = "Triggers failure", Completed = false }),
            nameof(LegacyTaskListStateV1.Items));

        await orchestratorV1.CreateDocumentAsync("phoenix-doc", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "phoenix-doc",
            new MapSetIntent("gen-1", new LegacyTaskItemV1 { Title = "First generation", Completed = true }),
            nameof(LegacyTaskListStateV1.Items));

        await migrationManager.DrainAllAsync();

        // Assert 1: Poison document failed without crashing the worker, and phoenix-doc migrated to "v2-phoenix-doc"
        migrationManager.TryGetStatus("lifecycle-and-fault-isolation", out var status1).ShouldBeTrue();
        status1.FailedTranslations.ShouldBeGreaterThanOrEqualTo(1);

        (await orchestratorV2.DocumentExistsAsync("v2-phoenix-doc", "task-list-v2")).ShouldBeTrue();
        var phoenixGen1 = await orchestratorV2.GetDocumentStateAsync<ModernTaskListStateV2>("v2-phoenix-doc");
        phoenixGen1.Data.ShouldNotBeNull();
        phoenixGen1.Data.Tasks.ShouldContainKey("gen-1");
        phoenixGen1.Data.Tasks["gen-1"].Description.ShouldBe("First generation");

        // Act 2: Delete "phoenix-doc" in V1 -> Verify tombstone propagates to "v2-phoenix-doc"
        await orchestratorV1.DeleteDocumentAsync("phoenix-doc", "task-list-v1");
        await migrationManager.DrainAllAsync();

        (await orchestratorV2.DocumentExistsAsync("v2-phoenix-doc", "task-list-v2")).ShouldBeFalse();

        // Act 3: Recreate "phoenix-doc" in V1 with new content -> Verify it is recreated in V2 without stale gen-1 data
        await orchestratorV1.CreateDocumentAsync("phoenix-doc", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "phoenix-doc",
            new MapSetIntent("gen-2", new LegacyTaskItemV1 { Title = "Reborn generation", Completed = false }),
            nameof(LegacyTaskListStateV1.Items));

        await migrationManager.DrainAllAsync();

        (await orchestratorV2.DocumentExistsAsync("v2-phoenix-doc", "task-list-v2")).ShouldBeTrue();
        var phoenixGen2 = await orchestratorV2.GetDocumentStateAsync<ModernTaskListStateV2>("v2-phoenix-doc");
        phoenixGen2.Data.ShouldNotBeNull();
        phoenixGen2.Data.Tasks.Count.ShouldBe(1);
        phoenixGen2.Data.Tasks.ContainsKey("gen-1").ShouldBeFalse();
        phoenixGen2.Data.Tasks.ShouldContainKey("gen-2");
        phoenixGen2.Data.Tasks["gen-2"].Description.ShouldBe("Reborn generation");
        phoenixGen2.Data.Tasks["gen-2"].Status.ShouldBe("Pending");

        // Act 4: Delete "phoenix-doc" a second time -> Verify tombstone propagation works again after recreation
        await orchestratorV1.DeleteDocumentAsync("phoenix-doc", "task-list-v1");
        await migrationManager.DrainAllAsync();

        (await orchestratorV2.DocumentExistsAsync("v2-phoenix-doc", "task-list-v2")).ShouldBeFalse();

        foreach (var hs in hostedServices)
        {
            await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task MediatorNode_PropagateDocumentTombstonesDisabled_PreservesTargetDocumentsWhenSourceDocumentsAreDeleted()
    {
        // Arrange: When PropagateDocumentTombstones = false (e.g., V1 is aggressive archival/TTL cleanup while V2 retains history),
        // deleting a document on V1 must NOT delete the migrated document on V2.
        var networkHandlers = new ConcurrentDictionary<(string MeshId, string ReplicaId), IApplicationPayloadHandler>();
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(options =>
        {
            options.ActiveSyncEnabled = true;
            options.EnableSharding = false;
            options.AntiEntropyInitialDelaySeconds = 60;
            options.AntiEntropyIntervalSeconds = 60;
        });

        services.AddCrdt()
            .AddCrdtJsonTypeInfoResolver(LiveMigrationTestJsonContext.Default)
            .AddCrdtAotContext(new LiveMigrationTestAotContext())
            .AddCrdtSerializableType<LegacyTaskItemV1>("legacy-task-v1")
            .AddCrdtSerializableType<ModernTaskItemV2>("modern-task-v2");

        services.AddDistributedDocumentType<LegacyTaskListStateV1>("task-list-v1");
        services.AddDistributedDocumentType<ModernTaskListStateV2>("task-list-v2");

        RegisterInMemoryMeshMocks(services, "mesh-v1", "replica-v1", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-v2", "replica-v2", networkHandlers);

        services.AddDistributedCrdtReplica("replica-v1", "mesh-v1");
        services.AddDistributedCrdtReplica("replica-v2", "mesh-v2");

        services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
        {
            options.MigrationId = "no-tombstone-propagation";
            options.SourceReplicaId = "replica-v1";
            options.SourceTypeAlias = "task-list-v1";
            options.TargetReplicaId = "replica-v2";
            options.TargetTypeAlias = "task-list-v2";
            options.PropagateDocumentTombstones = false;
            options.BackfillBatchDelayMilliseconds = 1;
        });

        await using var provider = services.BuildServiceProvider();
        var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var scopeV1 = scopeManager.GetOrCreateScope("replica-v1");
        var scopeV2 = scopeManager.GetOrCreateScope("replica-v2");

        var orchestratorV1 = scopeV1.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var orchestratorV2 = scopeV2.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestratorV1.InitializeAsync();
        await orchestratorV2.InitializeAsync();

        await orchestratorV1.CreateDocumentAsync("ephemeral-v1-doc", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "ephemeral-v1-doc",
            new MapSetIntent("keep-me", new LegacyTaskItemV1 { Title = "Retained in V2 after V1 purge", Completed = true }),
            nameof(LegacyTaskListStateV1.Items));

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        var migrationManager = provider.GetRequiredService<ICrdtMigrationManager>();
        await migrationManager.DrainAllAsync();

        (await orchestratorV2.DocumentExistsAsync("ephemeral-v1-doc", "task-list-v2")).ShouldBeTrue();

        // Act: Delete the document from V1
        await orchestratorV1.DeleteDocumentAsync("ephemeral-v1-doc", "task-list-v1");
        await migrationManager.DrainAllAsync();

        // Assert: Document is deleted in V1, but preserved in V2 with TombstonesProcessed == 0
        (await orchestratorV1.DocumentExistsAsync("ephemeral-v1-doc", "task-list-v1")).ShouldBeFalse();
        (await orchestratorV2.DocumentExistsAsync("ephemeral-v1-doc", "task-list-v2")).ShouldBeTrue();

        var retainedV2Doc = await orchestratorV2.GetDocumentStateAsync<ModernTaskListStateV2>("ephemeral-v1-doc");
        retainedV2Doc.Data.ShouldNotBeNull();
        retainedV2Doc.Data.Tasks.ShouldContainKey("keep-me");
        retainedV2Doc.Data.Tasks["keep-me"].Description.ShouldBe("Retained in V2 after V1 purge");

        migrationManager.TryGetStatus("no-tombstone-propagation", out var status).ShouldBeTrue();
        status.TombstonesProcessed.ShouldBe(0);

        foreach (var hs in hostedServices)
        {
            await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task MediatorNode_HighConcurrencyInterleavedBurstWrites_OnBothV1AndV2_ConvergesWithoutLostUpdatesOrReverseLeakage()
    {
        // Arrange: Stress test with concurrent writers hammering V1 (during backfill + live streaming) and V2 simultaneously.
        var networkHandlers = new ConcurrentDictionary<(string MeshId, string ReplicaId), IApplicationPayloadHandler>();
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(options =>
        {
            options.ActiveSyncEnabled = true;
            options.EnableSharding = false;
            options.AntiEntropyInitialDelaySeconds = 60;
            options.AntiEntropyIntervalSeconds = 60;
        });

        services.AddCrdt()
            .AddCrdtJsonTypeInfoResolver(LiveMigrationTestJsonContext.Default)
            .AddCrdtAotContext(new LiveMigrationTestAotContext())
            .AddCrdtSerializableType<LegacyTaskItemV1>("legacy-task-v1")
            .AddCrdtSerializableType<ModernTaskItemV2>("modern-task-v2");

        services.AddDistributedDocumentType<LegacyTaskListStateV1>("task-list-v1");
        services.AddDistributedDocumentType<ModernTaskListStateV2>("task-list-v2");
        RegisterInMemoryMeshMocks(services, "mesh-v1", "replica-v1", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-v2", "replica-v2", networkHandlers);

        services.AddDistributedCrdtReplica("replica-v1", "mesh-v1");
        services.AddDistributedCrdtReplica("replica-v2", "mesh-v2");

        services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
        {
            options.MigrationId = "high-concurrency-burst";
            options.SourceReplicaId = "replica-v1";
            options.SourceTypeAlias = "task-list-v1";
            options.TargetReplicaId = "replica-v2";
            options.TargetTypeAlias = "task-list-v2";
            options.MaxConcurrency = 4;
            options.BackfillBatchDelayMilliseconds = 2;
        });

        await using var provider = services.BuildServiceProvider();
        var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var scopeV1 = scopeManager.GetOrCreateScope("replica-v1");
        var scopeV2 = scopeManager.GetOrCreateScope("replica-v2");

        var orchestratorV1 = scopeV1.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var orchestratorV2 = scopeV2.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestratorV1.InitializeAsync();
        await orchestratorV2.InitializeAsync();

        const int documentCount = 12;
        const int itemsPerDocument = 15;

        // Pre-create half of the documents before starting the bridge
        for (int docIdx = 0; docIdx < documentCount / 2; docIdx++)
        {
            var docId = $"burst-doc-{docIdx}";
            await orchestratorV1.CreateDocumentAsync(docId, "task-list-v1");
            await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
                docId,
                new MapSetIntent("init", new LegacyTaskItemV1 { Title = $"Init-{docIdx}", Completed = false }),
                nameof(LegacyTaskListStateV1.Items));
        }

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        // Act: Concurrently create the remaining V1 documents, fire rapid item updates/deletes on V1,
        // and simultaneously write V2-native items on V2.
        var writerTasks = Enumerable.Range(0, documentCount).Select(async docIdx =>
        {
            var docId = $"burst-doc-{docIdx}";
            if (docIdx >= documentCount / 2)
            {
                await orchestratorV1.CreateDocumentAsync(docId, "task-list-v1");
            }

            // Also ensure doc exists in V2 concurrently and write a V2-native key
            await orchestratorV2.CreateDocumentAsync(docId, "task-list-v2");
            await orchestratorV2.ApplyIntentAsync<ModernTaskListStateV2>(
                docId,
                new MapSetIntent($"v2-native-{docIdx}", new ModernTaskItemV2 { Description = $"V2Native-{docIdx}", Status = "Active", SchemaVersion = 2 }),
                nameof(ModernTaskListStateV2.Tasks));

            for (int itemIdx = 0; itemIdx < itemsPerDocument; itemIdx++)
            {
                var key = $"k-{itemIdx}";
                await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
                    docId,
                    new MapSetIntent(key, new LegacyTaskItemV1 { Title = $"Step1-{docIdx}-{itemIdx}", Completed = false }),
                    nameof(LegacyTaskListStateV1.Items));

                // Mutate the same key a second time to test rapid in-place updates
                await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
                    docId,
                    new MapSetIntent(key, new LegacyTaskItemV1 { Title = $"Final-{docIdx}-{itemIdx}", Completed = itemIdx % 2 == 0 }),
                    nameof(LegacyTaskListStateV1.Items));
            }

            // Delete the very first key ("k-0") from V1 after writing it
            await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
                docId,
                new MapRemoveIntent("k-0"),
                nameof(LegacyTaskListStateV1.Items));
        });

        await Task.WhenAll(writerTasks);

        var migrationManager = provider.GetRequiredService<ICrdtMigrationManager>();
        await migrationManager.DrainAllAsync();

        // Assert: Every single document on V2 must have the exact final state from V1 plus its V2-native key,
        // and V1 must NEVER contain any "v2-native-*" keys.
        migrationManager.AreAllConverged().ShouldBeTrue();
        migrationManager.TryGetStatus("high-concurrency-burst", out var finalStatus).ShouldBeTrue();
        finalStatus.FailedTranslations.ShouldBe(0);

        for (int docIdx = 0; docIdx < documentCount; docIdx++)
        {
            var docId = $"burst-doc-{docIdx}";
            var v1Doc = await orchestratorV1.GetDocumentStateAsync<LegacyTaskListStateV1>(docId);
            var v2Doc = await orchestratorV2.GetDocumentStateAsync<ModernTaskListStateV2>(docId);

            v1Doc.Data.ShouldNotBeNull();
            v2Doc.Data.ShouldNotBeNull();

            // V1 must not have V2-native key
            v1Doc.Data.Items.ContainsKey($"v2-native-{docIdx}").ShouldBeFalse();
            v1Doc.Data.Items.ContainsKey("k-0").ShouldBeFalse();

            // V2 must have V2-native key, must not have deleted "k-0", and must match all final V1 items
            v2Doc.Data.Tasks.ShouldContainKey($"v2-native-{docIdx}");
            v2Doc.Data.Tasks[$"v2-native-{docIdx}"].Description.ShouldBe($"V2Native-{docIdx}");
            v2Doc.Data.Tasks.ContainsKey("k-0").ShouldBeFalse();

            for (int itemIdx = 1; itemIdx < itemsPerDocument; itemIdx++)
            {
                var key = $"k-{itemIdx}";
                v2Doc.Data.Tasks.ShouldContainKey(key);
                v2Doc.Data.Tasks[key].Description.ShouldBe($"Final-{docIdx}-{itemIdx}");
                v2Doc.Data.Tasks[key].Status.ShouldBe(itemIdx % 2 == 0 ? "Done" : "Pending");
            }
        }

        foreach (var hs in hostedServices)
        {
            await hs.StopAsync(CancellationToken.None);
        }
    }

    private sealed class SameTypePassthroughTaskMigrator : ICrdtDocumentMigrator<ModernTaskListStateV2, ModernTaskListStateV2>
    {
        public async Task MigrateDocumentAsync(
            string targetDocumentId,
            CrdtDocument<ModernTaskListStateV2> sourceDocument,
            CrdtDocument<ModernTaskListStateV2>? previousSourceDocument,
            CrdtDocument<ModernTaskListStateV2> targetDocument,
            ICrdtDocumentOrchestrator targetOrchestrator,
            IntentDeliveryMode deliveryMode,
            CancellationToken cancellationToken = default)
        {
            if (sourceDocument.Data is null) return;

            var existingTasks = targetDocument.Data?.Tasks;
            foreach (var kvp in sourceDocument.Data.Tasks)
            {
                if (existingTasks is not null &&
                    existingTasks.TryGetValue(kvp.Key, out var current) &&
                    current.Equals(kvp.Value))
                {
                    continue;
                }

                await targetOrchestrator.ApplyIntentAsync<ModernTaskListStateV2>(
                    targetDocumentId,
                    new MapSetIntent(kvp.Key, kvp.Value),
                    nameof(ModernTaskListStateV2.Tasks),
                    deliveryMode,
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private sealed class ClusterNodeHost : IAsyncDisposable
    {
        public required ServiceProvider ServiceProvider { get; init; }
        public required IReadOnlyList<IHostedService> HostedServices { get; init; }

        public ICrdtDocumentOrchestrator GetOrchestrator(string replicaId)
        {
            var scopeManager = ServiceProvider.GetRequiredService<DistributedCrdtScopeManager>();
            return scopeManager.GetOrCreateScope(replicaId).Orchestrator;
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var hs in HostedServices)
            {
                try
                {
                    await hs.StopAsync(CancellationToken.None);
                }
                catch
                {
                }
            }

            await ServiceProvider.DisposeAsync();
        }
    }

    private sealed class InMemoryMultiMeshNetworkBus
    {
        private readonly record struct NodeRegistration(
            string MeshId,
            string ReplicaId,
            PeerId PeerId,
            Func<IApplicationPayloadHandler> HandlerResolver);

        private readonly List<NodeRegistration> registrations = new();

        public void RegisterNode(IServiceCollection services, string meshId, string replicaId, PeerId localPeerId)
        {
            ServiceProvider? resolvedProvider = null;
            registrations.Add(new NodeRegistration(
                meshId,
                replicaId,
                localPeerId,
                () => resolvedProvider!.GetRequiredKeyedService<IApplicationPayloadHandler>(meshId)));

            services.Configure<P2pNodeOptions>(meshId, opts => opts.LocalPeerId = localPeerId.Value);

            var algorithmMock = new Mock<IP2pAlgorithm>();
            algorithmMock
                .Setup(a => a.BroadcastAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns((ReadOnlyMemory<byte> payload, CancellationToken ct) =>
                    BroadcastToMeshPeersAsync(meshId, localPeerId, payload, ct));

            var directSenderMock = new Mock<IDirectMessageSender>();
            directSenderMock
                .Setup(d => d.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns((PeerId targetPeer, ReadOnlyMemory<byte> payload, CancellationToken ct) =>
                    SendDirectToMeshPeerAsync(meshId, localPeerId, targetPeer, payload, ct));

            var peerRegistryMock = new Mock<IPeerRegistry>();
            var peerSelectorMock = new Mock<IPeerSelector>();
            var rpcCorrelatorMock = new Mock<IP2pRpcCorrelator>();

            peerRegistryMock.Setup(r => r.GetAllPeersAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(Array.Empty<PeerNode>());
            peerRegistryMock.Setup(r => r.GetPeersByStatusAsync(It.IsAny<string>(), It.IsAny<PeerStatus>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(Array.Empty<PeerNode>());
            peerSelectorMock.Setup(s => s.GetPeersAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(Array.Empty<PeerNode>());

            services.AddSingleton(algorithmMock.Object);
            services.AddSingleton(directSenderMock.Object);
            services.AddKeyedSingleton(meshId, directSenderMock.Object);
            services.AddKeyedSingleton(meshId, peerSelectorMock.Object);
            services.AddKeyedSingleton(meshId, peerRegistryMock.Object);
            services.AddKeyedSingleton(meshId, rpcCorrelatorMock.Object);

            services.AddSingleton(new ServiceProviderCapture(sp => resolvedProvider = sp));
        }

        public void BindComplete()
        {
        }

        private async Task BroadcastToMeshPeersAsync(string meshId, PeerId senderPeerId, ReadOnlyMemory<byte> payload, CancellationToken ct)
        {
            var targets = registrations.Where(r => string.Equals(r.MeshId, meshId, StringComparison.Ordinal) && r.PeerId != senderPeerId).ToList();
            foreach (var target in targets)
            {
                var handler = target.HandlerResolver();
                await handler.HandlePayloadAsync(meshId, senderPeerId, payload, ct).ConfigureAwait(false);
            }
        }

        private async Task SendDirectToMeshPeerAsync(string meshId, PeerId senderPeerId, PeerId targetPeerId, ReadOnlyMemory<byte> payload, CancellationToken ct)
        {
            var targets = registrations.Where(r => string.Equals(r.MeshId, meshId, StringComparison.Ordinal) && r.PeerId == targetPeerId).ToList();
            foreach (var target in targets)
            {
                var handler = target.HandlerResolver();
                await handler.HandlePayloadAsync(meshId, senderPeerId, payload, ct).ConfigureAwait(false);
            }
        }
    }

    private sealed class ServiceProviderCapture
    {
        public Action<ServiceProvider> Register { get; }
        public ServiceProviderCapture(Action<ServiceProvider> register) => Register = register;
    }

    private static async Task<ClusterNodeHost> CreateClusterNodeHostAsync(
        InMemoryMultiMeshNetworkBus meshBus,
        Action<IServiceCollection> configureReplicasAndMeshes)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(options =>
        {
            options.ActiveSyncEnabled = true;
            options.ActiveSyncDebounceMilliseconds = 0;
            options.EnableSharding = false;
            options.AntiEntropyInitialDelaySeconds = 60;
            options.AntiEntropyIntervalSeconds = 60;
        });

        services.AddCrdt()
            .AddCrdtJsonTypeInfoResolver(LiveMigrationTestJsonContext.Default)
            .AddCrdtAotContext(new LiveMigrationTestAotContext())
            .AddCrdtSerializableType<LegacyTaskItemV1>("legacy-task-v1")
            .AddCrdtSerializableType<ModernTaskItemV2>("modern-task-v2");

        services.AddDistributedDocumentType<LegacyTaskListStateV1>("task-list-v1");
        services.AddDistributedDocumentType<ModernTaskListStateV2>("task-list-v2");

        configureReplicasAndMeshes(services);

        var provider = services.BuildServiceProvider();
        foreach (var capture in provider.GetServices<ServiceProviderCapture>())
        {
            capture.Register(provider);
        }

        var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
        foreach (var scope in scopeManager.GetActiveScopes())
        {
            await scope.Orchestrator.InitializeAsync();
        }

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        return new ClusterNodeHost
        {
            ServiceProvider = provider,
            HostedServices = hostedServices
        };
    }

    private static void RegisterInMemoryMeshMocks(
        IServiceCollection services,
        string meshId,
        string replicaId,
        ConcurrentDictionary<(string MeshId, string ReplicaId), IApplicationPayloadHandler> networkHandlers)
    {
        var algorithmMock = new Mock<IP2pAlgorithm>();
        var directSenderMock = new Mock<IDirectMessageSender>();
        var peerSelectorMock = new Mock<IPeerSelector>();
        var peerRegistryMock = new Mock<IPeerRegistry>();
        var rpcCorrelatorMock = new Mock<IP2pRpcCorrelator>();

        peerRegistryMock.Setup(r => r.GetAllPeersAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(Array.Empty<PeerNode>());
        peerSelectorMock.Setup(s => s.GetPeersAsync(It.IsAny<int>(), It.IsAny<CancellationToken>())).ReturnsAsync(Array.Empty<PeerNode>());

        services.AddSingleton(algorithmMock.Object);
        services.AddKeyedSingleton(meshId, directSenderMock.Object);
        services.AddKeyedSingleton(meshId, peerSelectorMock.Object);
        services.AddKeyedSingleton(meshId, peerRegistryMock.Object);
        services.AddKeyedSingleton(meshId, rpcCorrelatorMock.Object);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!condition())
        {
            await Task.Delay(20, cts.Token);
        }
    }
}