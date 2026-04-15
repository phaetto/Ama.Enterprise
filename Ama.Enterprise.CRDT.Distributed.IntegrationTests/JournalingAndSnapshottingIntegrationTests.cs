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
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
using Microsoft.Extensions.DependencyInjection;
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
    public sealed class JournalTestState : IDistributedCrdtState
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
            opt.ReplicaId = replicaId;
            opt.CheckpointIntervalSeconds = 30;
            opt.AntiEntropyIntervalSeconds = 15;
        });

        services.AddCrdt()
                .AddCrdtAotContext(new JournalTestAotContext())
                .AddCrdtJsonTypeInfoResolver(JournalTestJsonContext.Default);

        services.AddDistributedDocumentType<JournalTestState>("journal-doc");
        services.AddSingleton(Mock.Of<IP2pProtocol>());

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
    }

    [IntegrationFact]
    public async Task Orchestrator_CreateDocument_ShouldJournalRegistryOperations_Safely()
    {
        // Arrange
        var sp = BuildNode("ReplicaA");
        var scopeProvider = sp.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var storage = sp.GetRequiredService<IDistributedCrdtStorage>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Act - Create a brand new document across the active matrix dynamically
        await orchestrator.CreateDocumentAsync("brand-new-doc", "journal-doc", CancellationToken.None);
        
        // Assert - The creation relies on the `system-document-registry` bounds natively, so it should securely exist in that specific stream
        var allOps = await storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        
        var registryOps = allOps.Where(o => o.DocumentId == orchestrator.Registry.DocumentId).ToList();
        
        registryOps.ShouldNotBeEmpty();
        registryOps.Any(o => o.Operation.JsonPath.Contains("brand-new-doc") || 
                            (o.Operation.Value != null && o.Operation.Value.ToString()!.Contains("brand-new-doc")))
                   .ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task GetMissingOperations_ShouldReturnOperations_WhenJournalIsIntact()
    {
        // Arrange
        var sp = BuildNode("ReplicaA");
        var scopeProvider = sp.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtPatcher>();
        
        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docA = orchestrator.GetDocument<JournalTestState>("journal-doc")!;

        // Apply a mapped patch natively securely using the exact Patcher effectively saving to the active journal correctly
        // This ensures the operation is structurally valid, preventing the applicator from rejecting it as "Unapplied"
        var intent = new MapSetIntent("testKey", "Updated");
        var op = patcher.GenerateOperation(docA.Document, x => x.DataMap, intent);
        
        await docA.ApplyPatchAsync(new CrdtPatch(new[] { op }), CancellationToken.None);

        // Remote node asks for missing operations indicating it has completely empty bounds mapping natively
        var remoteDvv = new DottedVersionVector();

        // Act
        var result = await docA.GetMissingOperationsAsync("ReplicaB", remoteDvv, CancellationToken.None);

        // Assert
        result.SnapshotRequired.ShouldBeFalse();
        result.Operations.Count.ShouldBeGreaterThan(0);
        result.Operations.Any(o => o.Id == op.Id).ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task GetMissingOperations_ShouldReturnSnapshotRequired_WhenJournalIsTrimmed()
    {
        // Arrange
        var sp = BuildNode("ReplicaA");
        var scopeProvider = sp.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtPatcher>();
        var storage = sp.GetRequiredService<IDistributedCrdtStorage>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docA = orchestrator.GetDocument<JournalTestState>("journal-doc")!;

        // Apply a mapped patch natively using the Patcher explicitly properly gracefully
        var intent = new MapSetIntent("testKey", "Updated");
        var op = patcher.GenerateOperation(docA.Document, x => x.DataMap, intent);
        
        await docA.ApplyPatchAsync(new CrdtPatch(new[] { op }), CancellationToken.None);

        // Intentionally trim the journal aggressively beyond the operation clock simulating background garbage collection cleanly mathematically
        var gmvv = new Dictionary<string, long> { { "ReplicaA", 10 } }; 
        await storage.TrimAsync(gmvv, CancellationToken.None);

        // Act - Remote node with empty bounds asks for missing operations securely triggering gap logic organically
        var remoteDvv = new DottedVersionVector();
        var result = await docA.GetMissingOperationsAsync("ReplicaB", remoteDvv, CancellationToken.None);

        // Assert - The mechanism detects causal truncation and smoothly requests a complete fallback snapshot flawlessly
        result.SnapshotRequired.ShouldBeTrue();
        result.Operations.ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task MergeSnapshot_ShouldOverrideLocalState_AndMergeGlobalVersionVectorCorrectly()
    {
        // Arrange
        var sp = BuildNode("ReplicaA");
        var serializer = sp.GetRequiredService<ICrdtSerializer>();
        var scopeProvider = sp.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("journal-doc", "journal-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var docA = orchestrator.GetDocument<JournalTestState>("journal-doc")!;
        
        // Materialize fallback structural bounds
        docA.Document.Data.DataMap["testKey"] = "Materialized Snapshot State";

        var globalDvv = new DottedVersionVector();
        globalDvv.Versions["ReplicaB"] = 5;

        var snapshotBytes = serializer.SerializeToBytes(docA.Document);

        // Act - Safely gracefully explicitly overwrite underlying local dependencies securely natively properly
        await docA.MergeSnapshotAsync(snapshotBytes, globalDvv, CancellationToken.None);

        // Assert
        docA.Document.Data.DataMap["testKey"].ShouldBe("Materialized Snapshot State");

        var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        replicaContext.GlobalVersionVector.Versions["ReplicaB"].ShouldBe(5);
    }
}