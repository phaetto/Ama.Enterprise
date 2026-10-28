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

        // 2. Propagate item removals from V1 -> V2
        if (existingTasks is not null)
        {
            foreach (var existingKey in existingTasks.Keys.ToList())
            {
                if (!sourceDocument.Data.Items.ContainsKey(existingKey))
                {
                    await targetOrchestrator.ApplyIntentAsync<ModernTaskListStateV2>(
                        targetDocumentId,
                        new MapRemoveIntent(existingKey),
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
        services.AddDistributedCrdtReplica("replica-v1");
        services.AddDistributedCrdtReplica("replica-v2");

        RegisterInMemoryMeshMocks(services, "mesh-v1", "replica-v1", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-v2", "replica-v2", networkHandlers);

        services.AddDistributedCrdtP2p("mesh-v1", "replica-v1");
        services.AddDistributedCrdtP2p("mesh-v2", "replica-v2");

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
        updatedAlphaV2.Data.Tasks.Count.ShouldBe(1);
        updatedAlphaV2.Data.Tasks.ContainsKey("t1").ShouldBeFalse();
        updatedAlphaV2.Data.Tasks["t2"].Status.ShouldBe("Done");

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

        services.AddDistributedCrdtReplica("replica-v1-mediator");
        services.AddDistributedCrdtReplica("replica-v2-target");

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

        services.AddDistributedCrdtP2p("mesh-v1", "replica-v1-mediator");
        services.AddDistributedCrdtP2p("mesh-v2", "replica-v2-target");

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
                        var resWrapper = new CrdtMessageWrapper(wrapper.DocumentId, "CrdtReadRes", serializer.SerializeToBytes(res));
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