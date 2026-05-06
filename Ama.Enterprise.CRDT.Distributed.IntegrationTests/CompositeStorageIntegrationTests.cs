namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
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
using Moq;

[CrdtAotType(typeof(CompositeStorageIntegrationTests.TestState))]
public sealed partial class CompositeTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(CompositeStorageIntegrationTests.TestState))]
[JsonSerializable(typeof(CrdtDocument<CompositeStorageIntegrationTests.TestState>))]
public sealed partial class CompositeTestJsonContext : JsonSerializerContext
{
}

public sealed class CompositeStorageIntegrationTests
{
    public sealed class TestState
    {
        public string Id { get; set; } = "default";
    }

    private readonly record struct TestNodeContext(IServiceProvider Provider, Mock<IDistributedCrdtStorage> PrimaryMock, Mock<IDistributedCrdtStorage> SpecificMock);

    private static async IAsyncEnumerable<JournaledOperation> GetEmptyJournaledOperationsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield break;
    }

    private TestNodeContext BuildTestNode()
    {
        var primaryMock = new Mock<IDistributedCrdtStorage>();
        var specificMock = new Mock<IDistributedCrdtStorage>();

        // Setup default mocks for IAsyncEnumerable returning methods to prevent NullReferenceExceptions during await foreach
        primaryMock.Setup(x => x.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(GetEmptyJournaledOperationsAsync());
        specificMock.Setup(x => x.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                    .Returns(GetEmptyJournaledOperationsAsync());

        primaryMock.Setup(x => x.GetOperationsByRangeAsync(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                   .Returns(GetEmptyJournaledOperationsAsync());
        specificMock.Setup(x => x.GetOperationsByRangeAsync(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                    .Returns(GetEmptyJournaledOperationsAsync());

        primaryMock.Setup(x => x.GetOperationsByDotsAsync(It.IsAny<string>(), It.IsAny<IEnumerable<long>>(), It.IsAny<CancellationToken>()))
                   .Returns(GetEmptyJournaledOperationsAsync());
        specificMock.Setup(x => x.GetOperationsByDotsAsync(It.IsAny<string>(), It.IsAny<IEnumerable<long>>(), It.IsAny<CancellationToken>()))
                    .Returns(GetEmptyJournaledOperationsAsync());

        var services = new ServiceCollection();
        services.AddLogging();

        // Register explicit mocks prior to AddDistributedCrdtCore so TryAddKeyedSingleton does not bypass them
        services.AddKeyedSingleton("primary", primaryMock.Object);
        services.AddKeyedSingleton("special-type", specificMock.Object);
        services.AddSingleton(new CrdtStorageRegistration("special-type"));

        services.AddDistributedCrdtCore(opt =>
        {
            opt.ReplicaId = "ReplicaTest";
        });

        services.AddCrdt()
                .AddCrdtAotContext(new CompositeTestAotContext())
                .AddCrdtJsonTypeInfoResolver(CompositeTestJsonContext.Default);

        services.AddDistributedDocumentType<TestState>("special-type");
        services.AddDistributedDocumentType<TestState>("unknown-type");

        return new TestNodeContext(services.BuildServiceProvider(), primaryMock, specificMock);
    }

    [IntegrationFact]
    public async Task CompositeStorage_ShouldRouteToSpecificStorage_WhenAliasMatchesCorrectly()
    {
        // Arrange
        var (provider, primaryMock, specificMock) = BuildTestNode();
        
        var scopeProvider = provider.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var compositeStorage = provider.GetRequiredService<IDistributedCrdtStorage>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Act
        await orchestrator.CreateDocumentAsync("doc-specific", "special-type", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var dummyDoc = new CrdtDocument<TestState>(new TestState { Id = "doc-specific" }, new CrdtMetadata());
        await compositeStorage.SaveDocumentAsync("doc-specific", dummyDoc, CancellationToken.None);
        await compositeStorage.DeleteDocumentAsync("doc-specific", CancellationToken.None);

        // Assert
        specificMock.Verify(x => x.SaveDocumentAsync("doc-specific", It.IsAny<CrdtDocument<TestState>>(), It.IsAny<CancellationToken>()), Times.Once);
        specificMock.Verify(x => x.DeleteDocumentAsync("doc-specific", It.IsAny<CancellationToken>()), Times.Once);

        primaryMock.Verify(x => x.SaveDocumentAsync(It.IsAny<string>(), It.IsAny<CrdtDocument<TestState>>(), It.IsAny<CancellationToken>()), Times.Never);
        primaryMock.Verify(x => x.DeleteDocumentAsync("doc-specific", It.IsAny<CancellationToken>()), Times.Never);
    }

    [IntegrationFact]
    public async Task CompositeStorage_ShouldRouteToPrimaryStorage_WhenAliasIsNotMapped()
    {
        // Arrange
        var (provider, primaryMock, specificMock) = BuildTestNode();
        
        var scopeProvider = provider.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var compositeStorage = provider.GetRequiredService<IDistributedCrdtStorage>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Act
        await orchestrator.CreateDocumentAsync("doc-unknown", "unknown-type", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var dummyDoc = new CrdtDocument<TestState>(new TestState { Id = "doc-unknown" }, new CrdtMetadata());
        await compositeStorage.SaveDocumentAsync("doc-unknown", dummyDoc, CancellationToken.None);

        // Assert
        primaryMock.Verify(x => x.SaveDocumentAsync("doc-unknown", It.IsAny<CrdtDocument<TestState>>(), It.IsAny<CancellationToken>()), Times.Once);
        specificMock.Verify(x => x.SaveDocumentAsync(It.IsAny<string>(), It.IsAny<CrdtDocument<TestState>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [IntegrationFact]
    public async Task CompositeStorage_ShouldBroadcastGlobalOperations_ToAllRegisteredStorages()
    {
        // Arrange
        var (provider, primaryMock, specificMock) = BuildTestNode();
        var compositeStorage = provider.GetRequiredService<IDistributedCrdtStorage>();

        var gmvv = new Dictionary<string, long>
        {
            { "ReplicaTest", 10 }
        };

        // Act
        await compositeStorage.TrimAsync(gmvv, CancellationToken.None);
        
        var operationsEnumerator = compositeStorage.GetAllJournaledOperationsAsync(CancellationToken.None);
        await foreach (var _ in operationsEnumerator) { }

        // Assert
        primaryMock.Verify(x => x.TrimAsync(gmvv, It.IsAny<CancellationToken>()), Times.Once);
        specificMock.Verify(x => x.TrimAsync(gmvv, It.IsAny<CancellationToken>()), Times.Once);
        
        primaryMock.Verify(x => x.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()), Times.Once);
        specificMock.Verify(x => x.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()), Times.Once);
    }
}