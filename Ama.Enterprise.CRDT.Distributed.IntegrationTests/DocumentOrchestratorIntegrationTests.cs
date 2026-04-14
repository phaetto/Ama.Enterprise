namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Attributes;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.UnitTests.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;
using Shouldly;

[CrdtAotType(typeof(DocumentOrchestratorIntegrationTests.DynamicTestState))]
public sealed partial class DynamicTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(DocumentOrchestratorIntegrationTests.DynamicTestState))]
[JsonSerializable(typeof(CrdtDocument<DocumentOrchestratorIntegrationTests.DynamicTestState>))]
public sealed partial class DynamicTestJsonContext : JsonSerializerContext
{
}

public sealed class DocumentOrchestratorIntegrationTests
{
    public sealed class DynamicTestState : IDistributedCrdtState
    {
        public string Id { get; set; } = "default";
        public int Value { get; set; }
    }

    private IServiceProvider BuildNode(Action<IServiceCollection>? configureExtra = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            opt.ReplicaId = "ReplicaA";
        });

        services.AddCrdt()
                .AddCrdtAotContext(new DynamicTestAotContext())
                .AddCrdtJsonTypeInfoResolver(DynamicTestJsonContext.Default);

        services.AddDistributedDocumentType<DynamicTestState>("dynamic-type");

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
    }

    [IntegrationFact]
    public async Task Orchestrator_ShouldDynamicallyCreateAndTrackDocuments_Successfully()
    {
        // Arrange
        var sp = BuildNode();
        var scopeProvider = sp.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Act
        await orchestrator.CreateDocumentAsync("dynamic-doc-1", "dynamic-type", CancellationToken.None);

        // Await manual sync execution to effectively bypass background thread execution timing inherently properly gracefully
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        // Assert
        var doc = orchestrator.GetDocument<DynamicTestState>("dynamic-doc-1");
        doc.ShouldNotBeNull();
        doc!.DocumentId.ShouldBe("dynamic-doc-1");

        var allDocs = orchestrator.GetActiveDocuments();
        allDocs.Any(d => d.DocumentId == "dynamic-doc-1").ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task Orchestrator_ShouldTombstoneAndDeleteDocumentsCorrectly_AcrossStorage()
    {
        // Arrange
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        var sp = BuildNode(s => 
        {
            s.Replace(ServiceDescriptor.Singleton(mockStorage.Object));
        });

        var scopeProvider = sp.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        await orchestrator.CreateDocumentAsync("doc-to-delete", "dynamic-type", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);
        
        orchestrator.GetDocument<DynamicTestState>("doc-to-delete").ShouldNotBeNull();

        // Act
        await orchestrator.DeleteDocumentAsync("doc-to-delete", CancellationToken.None);
        
        // Ensure synchronization lock handles background pipeline mapping sequentially natively
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        // Assert
        var deletedDoc = orchestrator.GetDocument<DynamicTestState>("doc-to-delete");
        deletedDoc.ShouldBeNull();
        
        var registryDocs = orchestrator.Registry.Document.Data.Documents;
        registryDocs.TryGetValue("doc-to-delete", out var entry).ShouldBeTrue();
        entry.IsDeleted.ShouldBeTrue();

        mockStorage.Verify(x => x.DeleteDocumentAsync("doc-to-delete", It.IsAny<CancellationToken>()), Times.Once);
    }

    [IntegrationFact]
    public async Task Orchestrator_ShouldIgnoreUnmappedTypeAliases_WithoutCrashing()
    {
        // Arrange
        var sp = BuildNode();
        var scopeProvider = sp.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Act
        await orchestrator.CreateDocumentAsync("unknown-doc", "missing-type", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        // Assert
        var doc = orchestrator.GetActiveDocuments().FirstOrDefault(d => d.DocumentId == "unknown-doc");
        doc.ShouldBeNull(); // It should securely effectively appropriately logically flawlessly ignore unmapped aliases natively without destructive application faults gracefully.
    }
}