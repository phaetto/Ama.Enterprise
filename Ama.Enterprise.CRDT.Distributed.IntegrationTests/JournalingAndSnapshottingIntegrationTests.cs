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
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
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
        // Switched to Dictionary explicitly allowing MapSetIntent to correctly and safely map natively via the core Patcher
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
        });

        services.AddDistributedCrdtReplica(replicaId);

        services.AddCrdt()
                .AddCrdtAotContext(new JournalTestAotContext())
                .AddCrdtJsonTypeInfoResolver(JournalTestJsonContext.Default);

        services.AddDistributedDocumentType<JournalTestState>("journal-doc");
        services.AddSingleton(Mock.Of<IP2pAlgorithm>());

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
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

        // Act - Create a brand new document across the active matrix dynamically
        await orchestrator.CreateDocumentAsync("brand-new-doc", "journal-doc", CancellationToken.None);
        
        // Assert - The creation relies on the system-document-registry bounds natively
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
        var patcher = scope.ServiceProvider.GetRequiredService<ICrdtPatcher>();
        var syncService = scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
        var journalManager = scope.ServiceProvider.GetRequiredService<IJournalManager>();
        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        
        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docA = orchestrator.GetDocument<JournalTestState>("journal-doc")!;

        // Apply a mapped patch using the Patcher saving to the active journal.
        // This ensures the operation is structurally valid, preventing the applicator from rejecting it as "Unapplied"
        var intent = new MapSetIntent("testKey", "Updated");
        var op = patcher.GenerateOperation(docA.Document, x => x.DataMap, intent);
        
        await docA.ApplyPatchAsync(new CrdtPatch(new[] { op }), CancellationToken.None);

        // Remote node asks for missing operations indicating it has completely empty bounds mapping natively
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
        var patcher = scope.ServiceProvider.GetRequiredService<ICrdtPatcher>();
        var storage = scope.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        var syncService = scope.ServiceProvider.GetRequiredService<IVersionVectorSyncService>();
        var journalManager = scope.ServiceProvider.GetRequiredService<IJournalManager>();
        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docA = orchestrator.GetDocument<JournalTestState>("journal-doc")!;

        // Apply a mapped patch using the Patcher explicitly
        var intent = new MapSetIntent("testKey", "Updated");
        var op = patcher.GenerateOperation(docA.Document, x => x.DataMap, intent);
        
        await docA.ApplyPatchAsync(new CrdtPatch(new[] { op }), CancellationToken.None);

        // Intentionally trim the journal aggressively beyond the operation clock simulating background garbage collection cleanly mathematically
        var gmvv = new Dictionary<string, long> { { "ReplicaA", 10 } }; 
        await storage.TrimAsync(gmvv, CancellationToken.None);

        // Act - Remote node with empty bounds asks for missing operations triggering gap logic
        var remoteDvv = new DottedVersionVector();
        
        var requirement = syncService.CalculateRequirement("ReplicaB", remoteDvv, replicaContext.ReplicaId, replicaContext.GlobalVersionVector);
        var missingOpsStream = journalManager.GetMissingOperationsAsync(requirement, CancellationToken.None);
        var result = await syncService.EvaluateJournalCompletionAsync(missingOpsStream, requirement, CancellationToken.None);

        // Assert - The mechanism detects causal truncation and requests a complete fallback snapshot.
        result.SnapshotRequired.ShouldBeTrue();
        result.Operations.ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task MergeSnapshot_ShouldOverrideLocalState_AndMergeGlobalVersionVector()
    {
        // Arrange
        var sp = BuildNode("ReplicaA");
        var serializer = sp.GetRequiredService<ICrdtSerializer>();
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("ReplicaA");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docA = orchestrator.GetDocument<JournalTestState>("journal-doc")!;
        
        // Materialize fallback structural bounds
        docA.Document.Data.DataMap["testKey"] = "Materialized Snapshot State";

        var globalDvv = new DottedVersionVector();
        globalDvv.Versions["ReplicaB"] = 5;

        var snapshotBytes = serializer.SerializeToBytes(docA.Document);

        // Act - Overwrite underlying local dependencies
        await docA.MergeSnapshotAsync(snapshotBytes, globalDvv, CancellationToken.None);

        // Assert
        docA.Document.Data.DataMap["testKey"].ShouldBe("Materialized Snapshot State");

        var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        replicaContext.GlobalVersionVector.Versions["ReplicaB"].ShouldBe(5);
    }

    [IntegrationFact]
    public async Task CrdtInitializationService_ShouldReplayJournaledOperations_OnStartup()
    {
        // Arrange - Node 1 (Simulates initial application run writing to the WAL without checkpointing)
        var sharedStorage = new MemoryCrdtStorage();

        var sp1 = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton<IDistributedCrdtStorage>(sharedStorage));
        });

        var scopeManager1 = sp1.GetRequiredService<DistributedCrdtScopeManager>();
        var scope1 = scopeManager1.GetOrCreateScope("Replica1");
        var orchestrator1 = scope1.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher1 = scope1.ServiceProvider.GetRequiredService<ICrdtPatcher>();

        // Orchestrator initialization creates the empty registry.
        await orchestrator1.InitializeAsync(CancellationToken.None);
        
        // Creating a document mutates the registry and appends a valid patch to the shared storage WAL inherently via decorators.
        await orchestrator1.CreateDocumentAsync("test-replayed-doc", "journal-doc", CancellationToken.None);
        await orchestrator1.SyncDocumentsAsync(CancellationToken.None);

        var doc1 = orchestrator1.GetDocument<JournalTestState>("test-replayed-doc")!;
        
        // Generate multiple operations safely using the Patcher to explicitly map true logical clock values natively
        var intent1 = new MapSetIntent("key1", "ReplayedData1");
        var op1 = patcher1.GenerateOperation(doc1.Document, x => x.DataMap, intent1);

        var intent2 = new MapSetIntent("key2", "ReplayedData2");
        var op2 = patcher1.GenerateOperation(doc1.Document, x => x.DataMap, intent2);

        // Apply patches correctly invoking the decorators natively explicit writing structurally valid WAL entries.
        await doc1.ApplyPatchAsync(new CrdtPatch(new[] { op1, op2 }), CancellationToken.None);

        // Verify the uncheckpointed operations hit the underlying storage correctly inherently
        var journalOps = await sharedStorage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        journalOps.Count.ShouldBeGreaterThan(2);

        // Act - Node 2 (Simulates a restart binding the same storage mapping natively to recover the state)
        var sp2 = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton<IDistributedCrdtStorage>(sharedStorage));
        });

        // The HostedService startup simulates the application boot sequence natively reading uncheckpointed fallback operations.
        var initService = sp2.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        // Assert - The orchestrator should have rebuilt the registry and replayed operations.
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
            // Override the default options injected by BuildNode to trigger fast checkpoints and tight bounds limits
            services.Configure<DistributedCrdtOptions>(opt =>
            {
                opt.CheckpointIntervalSeconds = 1;
                opt.JournalTrimThreshold = 5;
            });
            services.Replace(ServiceDescriptor.Singleton<IDistributedCrdtStorage>(sharedStorage));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher = scope.ServiceProvider.GetRequiredService<ICrdtPatcher>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("trim-test-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var doc = orchestrator.GetDocument<JournalTestState>("trim-test-doc")!;

        // Generate and apply more than 5 operations to exceed the aggressive trim threshold natively
        for (int i = 0; i < 10; i++)
        {
            var intent = new MapSetIntent($"key{i}", $"value{i}");
            var op = patcher.GenerateOperation(doc.Document, x => x.DataMap, intent);
            await doc.ApplyPatchAsync(new CrdtPatch(new[] { op }), CancellationToken.None);
        }

        // Verify journal has accumulated operations successfully before background trimming begins
        var initialJournalOps = await sharedStorage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        initialJournalOps.Count.ShouldBeGreaterThan(10);

        // Fetch the active background service managing the checkpoints
        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();
        
        // Act - Start the background service manually executing the threshold bounds logic
        await checkpointService.StartAsync(CancellationToken.None);

        // Provide enough time to trigger the periodic check-pointing background tick (1 second configured interval)
        await Task.Delay(1500);

        // Stop the service gracefully natively dropping active loops
        await checkpointService.StopAsync(CancellationToken.None);

        // Assert - The mechanism detects unbounded lists exceeding the threshold and drops trailing limits natively
        var trimmedJournalOps = await sharedStorage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        
        trimmedJournalOps.Count.ShouldBeLessThan(initialJournalOps.Count);
        
        // Since there is only one localized node tracking this global structural matrix natively, the overarching GMVV
        // evaluates exclusively up to the local logical head clock. Meaning it strictly forces aggressive log truncation
        // fully dropping limits to or below the enforced capacity thresholds efficiently correctly.
        trimmedJournalOps.Count.ShouldBeLessThanOrEqualTo(5);
    }
}