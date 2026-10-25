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
    public async Task MediatorNode_CustomDocumentIdMapping_AndFaultIsolation_ContinuesMigratingHealthyDocuments()
    {
        // Arrange
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

        services.AddDistributedCrdtReplica("replica-v1");
        services.AddDistributedCrdtReplica("replica-v2");

        RegisterInMemoryMeshMocks(services, "mesh-v1", "replica-v1", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-v2", "replica-v2", networkHandlers);

        services.AddDistributedCrdtP2p("mesh-v1", "replica-v1");
        services.AddDistributedCrdtP2p("mesh-v2", "replica-v2");

        services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, CustomPrefixAndFaultyTaskMigrator>(options =>
        {
            options.MigrationId = "custom-id-and-fault-test";
            options.SourceReplicaId = "replica-v1";
            options.SourceTypeAlias = "task-list-v1";
            options.TargetReplicaId = "replica-v2";
            options.TargetTypeAlias = "task-list-v2";
            options.BackfillBatchDelayMilliseconds = 1;
            options.ReconciliationScanIntervalSeconds = 60;
        });

        await using var provider = services.BuildServiceProvider();
        var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var orchestratorV1 = scopeManager.GetOrCreateScope("replica-v1").ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var orchestratorV2 = scopeManager.GetOrCreateScope("replica-v2").ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestratorV1.InitializeAsync();
        await orchestratorV2.InitializeAsync();

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        // Act: Create a healthy doc, a poisoned doc that throws, and another healthy doc
        await orchestratorV1.CreateDocumentAsync("doc-1", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "doc-1",
            new MapSetIntent("k1", new LegacyTaskItemV1 { Title = "Healthy 1", Completed = true }),
            nameof(LegacyTaskListStateV1.Items));

        await orchestratorV1.CreateDocumentAsync("doc-poison", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "doc-poison",
            new MapSetIntent("poison", new LegacyTaskItemV1 { Title = "Corrupt payload", Completed = false }),
            nameof(LegacyTaskListStateV1.Items));

        await orchestratorV1.CreateDocumentAsync("doc-2", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "doc-2",
            new MapSetIntent("k2", new LegacyTaskItemV1 { Title = "Healthy 2", Completed = false }),
            nameof(LegacyTaskListStateV1.Items));

        var migrationManager = provider.GetRequiredService<ICrdtMigrationManager>();
        await migrationManager.DrainAllAsync();

        // Assert: Failed translation is recorded, worker did not crash, and mapped IDs exist in target
        migrationManager.TryGetStatus("custom-id-and-fault-test", out var status).ShouldBeTrue();
        status.FailedTranslations.ShouldBeGreaterThanOrEqualTo(1);
        status.BackfilledDocuments.ShouldBe(2);

        (await orchestratorV2.DocumentExistsAsync("doc-1", "task-list-v2")).ShouldBeFalse();
        (await orchestratorV2.DocumentExistsAsync("v2-doc-1", "task-list-v2")).ShouldBeTrue();
        (await orchestratorV2.DocumentExistsAsync("v2-doc-2", "task-list-v2")).ShouldBeTrue();

        var doc1V2 = await orchestratorV2.GetDocumentStateAsync<ModernTaskListStateV2>("v2-doc-1");
        doc1V2.Data.ShouldNotBeNull();
        doc1V2.Data.Tasks["k1"].Description.ShouldBe("Healthy 1");

        // Fix the poison document and verify it recovers and migrates on subsequent mutation
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "doc-poison",
            new MapRemoveIntent("poison"),
            nameof(LegacyTaskListStateV1.Items));
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "doc-poison",
            new MapSetIntent("recovered", new LegacyTaskItemV1 { Title = "Fixed item", Completed = true }),
            nameof(LegacyTaskListStateV1.Items));

        await migrationManager.DrainAllAsync();

        var recoveredDocV2 = await orchestratorV2.GetDocumentStateAsync<ModernTaskListStateV2>("v2-doc-poison");
        recoveredDocV2.Data.ShouldNotBeNull();
        recoveredDocV2.Data.Tasks.ContainsKey("recovered").ShouldBeTrue();
        recoveredDocV2.Data.Tasks["recovered"].Status.ShouldBe("Done");

        // Delete doc-1 in source and verify the mapped target ID ("v2-doc-1") is deleted
        await orchestratorV1.DeleteDocumentAsync("doc-1", "task-list-v1");
        await migrationManager.DrainAllAsync();

        (await orchestratorV2.DocumentExistsAsync("v2-doc-1", "task-list-v2")).ShouldBeFalse();

        foreach (var hs in hostedServices)
        {
            await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task MediatorNode_WhenPropagateDocumentTombstonesDisabled_RetainsTargetDocumentsAndFiltersUnrelatedTypes()
    {
        // Arrange
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
        services.AddDistributedDocumentType<LegacyTaskListStateV1>("other-unrelated-type-v1");
        services.AddDistributedDocumentType<ModernTaskListStateV2>("task-list-v2");

        services.AddDistributedCrdtReplica("replica-v1");
        services.AddDistributedCrdtReplica("replica-v2");

        RegisterInMemoryMeshMocks(services, "mesh-v1", "replica-v1", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-v2", "replica-v2", networkHandlers);

        services.AddDistributedCrdtP2p("mesh-v1", "replica-v1");
        services.AddDistributedCrdtP2p("mesh-v2", "replica-v2");

        services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
        {
            options.MigrationId = "no-tombstones-migration";
            options.SourceReplicaId = "replica-v1";
            options.SourceTypeAlias = "task-list-v1";
            options.TargetReplicaId = "replica-v2";
            options.TargetTypeAlias = "task-list-v2";
            options.PropagateDocumentTombstones = false;
            options.BackfillBatchDelayMilliseconds = 1;
        });

        await using var provider = services.BuildServiceProvider();
        var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var orchestratorV1 = scopeManager.GetOrCreateScope("replica-v1").ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var orchestratorV2 = scopeManager.GetOrCreateScope("replica-v2").ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestratorV1.InitializeAsync();
        await orchestratorV2.InitializeAsync();

        // Create an unrelated document type on V1 before backfill
        await orchestratorV1.CreateDocumentAsync("unrelated-doc", "other-unrelated-type-v1");
        await orchestratorV1.CreateDocumentAsync("retained-doc", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "retained-doc",
            new MapSetIntent("t1", new LegacyTaskItemV1 { Title = "Keep me in V2", Completed = true }),
            nameof(LegacyTaskListStateV1.Items));

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        var migrationManager = provider.GetRequiredService<ICrdtMigrationManager>();
        await migrationManager.DrainAllAsync();

        // Verify unrelated type was ignored and target document was migrated
        (await orchestratorV2.DocumentExistsAsync("unrelated-doc", "task-list-v2")).ShouldBeFalse();
        (await orchestratorV2.DocumentExistsAsync("retained-doc", "task-list-v2")).ShouldBeTrue();

        // Act: Delete the document in V1 while PropagateDocumentTombstones is false
        await orchestratorV1.DeleteDocumentAsync("retained-doc", "task-list-v1");
        await migrationManager.DrainAllAsync();

        // Assert: Document remains intact in V2 and TombstonesProcessed is 0
        (await orchestratorV2.DocumentExistsAsync("retained-doc", "task-list-v2")).ShouldBeTrue();
        migrationManager.TryGetStatus("no-tombstones-migration", out var status).ShouldBeTrue();
        status.TombstonesProcessed.ShouldBe(0);

        foreach (var hs in hostedServices)
        {
            await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task MediatorNode_DocumentDeleteAndRecreateLifecycle_PropagatesBothDirections()
    {
        // Arrange
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

        services.AddDistributedCrdtReplica("replica-v1");
        services.AddDistributedCrdtReplica("replica-v2");

        RegisterInMemoryMeshMocks(services, "mesh-v1", "replica-v1", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-v2", "replica-v2", networkHandlers);

        services.AddDistributedCrdtP2p("mesh-v1", "replica-v1");
        services.AddDistributedCrdtP2p("mesh-v2", "replica-v2");

        services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
        {
            options.MigrationId = "recreate-lifecycle";
            options.SourceReplicaId = "replica-v1";
            options.SourceTypeAlias = "task-list-v1";
            options.TargetReplicaId = "replica-v2";
            options.TargetTypeAlias = "task-list-v2";
            options.PropagateDocumentTombstones = true;
            options.BackfillBatchDelayMilliseconds = 1;
        });

        await using var provider = services.BuildServiceProvider();
        var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var orchestratorV1 = scopeManager.GetOrCreateScope("replica-v1").ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var orchestratorV2 = scopeManager.GetOrCreateScope("replica-v2").ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestratorV1.InitializeAsync();
        await orchestratorV2.InitializeAsync();

        // Pre-delete a document before migration starts (never existed in V2)
        await orchestratorV1.CreateDocumentAsync("pre-deleted-doc", "task-list-v1");
        await orchestratorV1.DeleteDocumentAsync("pre-deleted-doc", "task-list-v1");

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        var migrationManager = provider.GetRequiredService<ICrdtMigrationManager>();
        await migrationManager.DrainAllAsync();

        // Pre-deleted doc that never existed in V2 should not increment TombstonesProcessed
        migrationManager.TryGetStatus("recreate-lifecycle", out var initialStatus).ShouldBeTrue();
        initialStatus.TombstonesProcessed.ShouldBe(0);

        // Step 1: Create -> Migrate -> Delete -> Verify Tombstone
        await orchestratorV1.CreateDocumentAsync("flapping-doc", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "flapping-doc",
            new MapSetIntent("item1", new LegacyTaskItemV1 { Title = "First incarnation", Completed = false }),
            nameof(LegacyTaskListStateV1.Items));
        await migrationManager.DrainAllAsync();
        (await orchestratorV2.DocumentExistsAsync("flapping-doc", "task-list-v2")).ShouldBeTrue();

        await orchestratorV1.DeleteDocumentAsync("flapping-doc", "task-list-v1");
        await migrationManager.DrainAllAsync();
        (await orchestratorV2.DocumentExistsAsync("flapping-doc", "task-list-v2")).ShouldBeFalse();

        // Step 2: Recreate the same document ID -> Migrate -> Verify Recreated in V2
        await orchestratorV1.CreateDocumentAsync("flapping-doc", "task-list-v1");
        await orchestratorV1.ApplyIntentAsync<LegacyTaskListStateV1>(
            "flapping-doc",
            new MapSetIntent("item2", new LegacyTaskItemV1 { Title = "Second incarnation", Completed = true }),
            nameof(LegacyTaskListStateV1.Items));
        await migrationManager.DrainAllAsync();

        (await orchestratorV2.DocumentExistsAsync("flapping-doc", "task-list-v2")).ShouldBeTrue();
        var recreatedState = await orchestratorV2.GetDocumentStateAsync<ModernTaskListStateV2>("flapping-doc");
        recreatedState.Data.ShouldNotBeNull();
        recreatedState.Data.Tasks.ContainsKey("item2").ShouldBeTrue();
        recreatedState.Data.Tasks["item2"].Description.ShouldBe("Second incarnation");

        // Step 3: Delete a second time -> Verify Tombstone is propagated again
        await orchestratorV1.DeleteDocumentAsync("flapping-doc", "task-list-v1");
        await migrationManager.DrainAllAsync();

        (await orchestratorV2.DocumentExistsAsync("flapping-doc", "task-list-v2")).ShouldBeFalse();
        migrationManager.TryGetStatus("recreate-lifecycle", out var finalStatus).ShouldBeTrue();
        finalStatus.TombstonesProcessed.ShouldBe(2);

        foreach (var hs in hostedServices)
        {
            await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task MediatorNode_WhenPartitionBackfillByPrimaryShardOwnerEnabled_OnlyPrimaryOwnerBackfillsEachDocument()
    {
        // Arrange: Set up two Mediator nodes where V1 is sharded with ReplicationFactor = 2 (both nodes hold all docs, but primary owner is partitioned)
        var networkHandlers = new ConcurrentDictionary<(string MeshId, string ReplicaId), IApplicationPayloadHandler>();
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(options =>
        {
            options.ActiveSyncEnabled = true;
            options.EnableSharding = true;
            options.ShardReplicationFactor = 2;
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

        // Mediator Node A (replica-v1-a -> replica-v2-a) and Mediator Node B (replica-v1-b -> replica-v2-b)
        services.AddDistributedCrdtReplica("replica-v1-a");
        services.AddDistributedCrdtReplica("replica-v2-a");
        services.AddDistributedCrdtReplica("replica-v1-b");
        services.AddDistributedCrdtReplica("replica-v2-b");

        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        RegisterInMemoryMeshMocks(services, "mesh-v1-a", "replica-v1-a", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-v2-a", "replica-v2-a", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-v1-b", "replica-v1-b", networkHandlers);
        RegisterInMemoryMeshMocks(services, "mesh-v2-b", "replica-v2-b", networkHandlers);

        var sharedPeerRegistryMock = new Mock<IPeerRegistry>();
        sharedPeerRegistryMock
            .Setup(r => r.GetPeersByStatusAsync("mesh-v1-a", PeerStatus.Active, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new[] { new PeerNode { Id = peerBId } });
        sharedPeerRegistryMock
            .Setup(r => r.GetPeersByStatusAsync("mesh-v1-b", PeerStatus.Active, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new[] { new PeerNode { Id = peerAId } });
        sharedPeerRegistryMock
            .Setup(r => r.GetPeersByStatusAsync(It.Is<string>(m => m != "mesh-v1-a" && m != "mesh-v1-b"), PeerStatus.Active, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<PeerNode>());
        services.AddSingleton<IPeerRegistry>(sharedPeerRegistryMock.Object);

        services.AddDistributedCrdtP2p("mesh-v1-a", "replica-v1-a");
        services.AddDistributedCrdtP2p("mesh-v2-a", "replica-v2-a");
        services.AddDistributedCrdtP2p("mesh-v1-b", "replica-v1-b");
        services.AddDistributedCrdtP2p("mesh-v2-b", "replica-v2-b");

        services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
        {
            options.MigrationId = "bridge-node-a";
            options.SourceReplicaId = "replica-v1-a";
            options.SourceTypeAlias = "task-list-v1";
            options.TargetReplicaId = "replica-v2-a";
            options.TargetTypeAlias = "task-list-v2";
            options.PartitionBackfillByPrimaryShardOwner = true;
            options.BackfillBatchDelayMilliseconds = 0;
        });

        services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
        {
            options.MigrationId = "bridge-node-b";
            options.SourceReplicaId = "replica-v1-b";
            options.SourceTypeAlias = "task-list-v1";
            options.TargetReplicaId = "replica-v2-b";
            options.TargetTypeAlias = "task-list-v2";
            options.PartitionBackfillByPrimaryShardOwner = true;
            options.BackfillBatchDelayMilliseconds = 0;
        });

        await using var provider = services.BuildServiceProvider();
        var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();

        var scopeV1A = scopeManager.GetOrCreateScope("replica-v1-a");
        var scopeV1B = scopeManager.GetOrCreateScope("replica-v1-b");
        var scopeV2A = scopeManager.GetOrCreateScope("replica-v2-a");
        var scopeV2B = scopeManager.GetOrCreateScope("replica-v2-b");

        // Inform both V1 cluster trackers that both replica-v1-a and replica-v1-b are active in the V1 cluster
        var trackerA = scopeV1A.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        var trackerB = scopeV1B.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        trackerA.UpdatePeerState("replica-v1-b", peerBId.Value.ToString(), new ShardVersionVectorMap());
        trackerB.UpdatePeerState("replica-v1-a", peerAId.Value.ToString(), new ShardVersionVectorMap());

        var orchestratorV1A = scopeV1A.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var orchestratorV1B = scopeV1B.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var orchestratorV2A = scopeV2A.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var orchestratorV2B = scopeV2B.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestratorV1A.InitializeAsync();
        await orchestratorV1B.InitializeAsync();
        await orchestratorV2A.InitializeAsync();
        await orchestratorV2B.InitializeAsync();

        // Populate 20 identical documents on both V1 replicas (since ReplicationFactor = 2)
        const int totalDocs = 20;
        for (int i = 0; i < totalDocs; i++)
        {
            var docId = $"sharded-doc-{i}";
            await orchestratorV1A.CreateDocumentAsync(docId, "task-list-v1");
            await orchestratorV1B.CreateDocumentAsync(docId, "task-list-v1");
        }

        // Act: Start bridges and drain
        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        var migrationManager = provider.GetRequiredService<ICrdtMigrationManager>();
        await migrationManager.DrainAllAsync();

        // Assert: Each document was backfilled by strictly ONE primary owner (no duplicate backfill across A and B)
        migrationManager.TryGetStatus("bridge-node-a", out var statusA).ShouldBeTrue();
        migrationManager.TryGetStatus("bridge-node-b", out var statusB).ShouldBeTrue();

        // Only one shard exists: default, so it ggoes to one node only
        (statusA.BackfilledDocuments + statusB.BackfilledDocuments).ShouldBe(totalDocs);
        statusA.BackfilledDocuments.ShouldBe(0);
        statusB.BackfilledDocuments.ShouldBe(20);

        foreach (var hs in hostedServices)
        {
            await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public void AddDistributedCrdtMigration_WithInvalidOptions_ThrowsArgumentException()
    {
        var services = new ServiceCollection();

        Should.Throw<ArgumentException>(() =>
            services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
            {
                options.SourceReplicaId = "";
                options.SourceTypeAlias = "v1";
                options.TargetReplicaId = "replica-v2";
                options.TargetTypeAlias = "v2";
            }));

        Should.Throw<ArgumentException>(() =>
            services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
            {
                options.SourceReplicaId = "replica-v1";
                options.SourceTypeAlias = " ";
                options.TargetReplicaId = "replica-v2";
                options.TargetTypeAlias = "v2";
            }));

        Should.Throw<ArgumentException>(() =>
            services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
            {
                options.SourceReplicaId = "replica-v1";
                options.SourceTypeAlias = "v1";
                options.TargetReplicaId = "";
                options.TargetTypeAlias = "v2";
            }));

        Should.Throw<ArgumentException>(() =>
            services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
            {
                options.SourceReplicaId = "replica-v1";
                options.SourceTypeAlias = "v1";
                options.TargetReplicaId = "replica-v2";
                options.TargetTypeAlias = "";
            }));

        Should.Throw<ArgumentOutOfRangeException>(() =>
            services.AddDistributedCrdtMigration<LegacyTaskListStateV1, ModernTaskListStateV2, LegacyToModernTaskMigrator>(options =>
            {
                options.SourceReplicaId = "replica-v1";
                options.SourceTypeAlias = "v1";
                options.TargetReplicaId = "replica-v2";
                options.TargetTypeAlias = "v2";
                options.MaxConcurrency = 0;
            }));
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