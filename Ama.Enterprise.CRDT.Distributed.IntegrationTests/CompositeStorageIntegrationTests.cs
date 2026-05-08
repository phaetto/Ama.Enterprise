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

    private readonly record struct TestNodeContext(IServiceProvider Provider, Mock<IDistributedCrdtStorage> PrimaryMock, Mock<IDistributedCrdtStorage> SpecificTypeMock, Mock<IDistributedCrdtStorage> SpecificDocMock);

    private static async IAsyncEnumerable<JournaledOperation> GetEmptyJournaledOperationsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield break;
    }

    private TestNodeContext BuildTestNode()
    {
        var primaryMock = new Mock<IDistributedCrdtStorage>();
        var specificTypeMock = new Mock<IDistributedCrdtStorage>();
        var specificDocMock = new Mock<IDistributedCrdtStorage>();

        // Setup default mocks for IAsyncEnumerable returning methods to prevent NullReferenceExceptions during await foreach
        primaryMock.Setup(x => x.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(GetEmptyJournaledOperationsAsync());
        specificTypeMock.Setup(x => x.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                    .Returns(GetEmptyJournaledOperationsAsync());
        specificDocMock.Setup(x => x.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                    .Returns(GetEmptyJournaledOperationsAsync());

        primaryMock.Setup(x => x.GetOperationsByRangeAsync(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                   .Returns(GetEmptyJournaledOperationsAsync());
        specificTypeMock.Setup(x => x.GetOperationsByRangeAsync(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                    .Returns(GetEmptyJournaledOperationsAsync());
        specificDocMock.Setup(x => x.GetOperationsByRangeAsync(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                    .Returns(GetEmptyJournaledOperationsAsync());

        primaryMock.Setup(x => x.GetOperationsByDotsAsync(It.IsAny<string>(), It.IsAny<IEnumerable<long>>(), It.IsAny<CancellationToken>()))
                   .Returns(GetEmptyJournaledOperationsAsync());
        specificTypeMock.Setup(x => x.GetOperationsByDotsAsync(It.IsAny<string>(), It.IsAny<IEnumerable<long>>(), It.IsAny<CancellationToken>()))
                    .Returns(GetEmptyJournaledOperationsAsync());
        specificDocMock.Setup(x => x.GetOperationsByDotsAsync(It.IsAny<string>(), It.IsAny<IEnumerable<long>>(), It.IsAny<CancellationToken>()))
                    .Returns(GetEmptyJournaledOperationsAsync());

        var services = new ServiceCollection();
        services.AddLogging();

        // Register explicit mocks prior to AddDistributedCrdtCore so TryAddKeyedSingleton does not bypass them
        services.AddKeyedSingleton("primary", primaryMock.Object);
        
        services.AddKeyedSingleton("type:special-type", specificTypeMock.Object);
        services.AddSingleton(new CrdtStorageRegistration("type:special-type", "special-type", CrdtStorageRoutingType.DocumentType));

        services.AddKeyedSingleton("doc:specific-doc-id", specificDocMock.Object);
        services.AddSingleton(new CrdtStorageRegistration("doc:specific-doc-id", "specific-doc-id", CrdtStorageRoutingType.DocumentId));

        services.AddDistributedCrdtCore(opt =>
        {
            opt.ReplicaId = "ReplicaTest";
        });

        services.AddCrdt()
                .AddCrdtAotContext(new CompositeTestAotContext())
                .AddCrdtJsonTypeInfoResolver(CompositeTestJsonContext.Default);

        services.AddDistributedDocumentType<TestState>("special-type");
        services.AddDistributedDocumentType<TestState>("unknown-type");

        return new TestNodeContext(services.BuildServiceProvider(), primaryMock, specificTypeMock, specificDocMock);
    }

    [IntegrationFact]
    public async Task CompositeStorage_ShouldRouteToSpecificTypeStorage_WhenAliasMatchesCorrectly()
    {
        // Arrange
        var (provider, primaryMock, specificTypeMock, specificDocMock) = BuildTestNode();
        
        var scopeProvider = provider.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var compositeStorage = provider.GetRequiredService<IDistributedCrdtStorage>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Act
        await orchestrator.CreateDocumentAsync("doc-normal", "special-type", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var dummyDoc = new CrdtDocument<TestState>(new TestState { Id = "doc-normal" }, new CrdtMetadata());
        await compositeStorage.SaveDocumentAsync("doc-normal", dummyDoc, CancellationToken.None);
        await compositeStorage.DeleteDocumentAsync("doc-normal", CancellationToken.None);

        // Assert
        specificTypeMock.Verify(x => x.SaveDocumentAsync("doc-normal", It.IsAny<CrdtDocument<TestState>>(), It.IsAny<CancellationToken>()), Times.Once);
        specificTypeMock.Verify(x => x.DeleteDocumentAsync("doc-normal", It.IsAny<CancellationToken>()), Times.Once);

        primaryMock.Verify(x => x.SaveDocumentAsync(It.IsAny<string>(), It.IsAny<CrdtDocument<TestState>>(), It.IsAny<CancellationToken>()), Times.Never);
        specificDocMock.Verify(x => x.SaveDocumentAsync(It.IsAny<string>(), It.IsAny<CrdtDocument<TestState>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [IntegrationFact]
    public async Task CompositeStorage_ShouldRouteToSpecificDocumentStorage_WhenIdMatchesCorrectly()
    {
        // Arrange
        var (provider, primaryMock, specificTypeMock, specificDocMock) = BuildTestNode();
        
        var scopeProvider = provider.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var compositeStorage = provider.GetRequiredService<IDistributedCrdtStorage>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Act
        await orchestrator.CreateDocumentAsync("specific-doc-id", "unknown-type", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var dummyDoc = new CrdtDocument<TestState>(new TestState { Id = "specific-doc-id" }, new CrdtMetadata());
        await compositeStorage.SaveDocumentAsync("specific-doc-id", dummyDoc, CancellationToken.None);
        await compositeStorage.DeleteDocumentAsync("specific-doc-id", CancellationToken.None);

        // Assert
        specificDocMock.Verify(x => x.SaveDocumentAsync("specific-doc-id", It.IsAny<CrdtDocument<TestState>>(), It.IsAny<CancellationToken>()), Times.Once);
        specificDocMock.Verify(x => x.DeleteDocumentAsync("specific-doc-id", It.IsAny<CancellationToken>()), Times.Once);

        primaryMock.Verify(x => x.SaveDocumentAsync(It.IsAny<string>(), It.IsAny<CrdtDocument<TestState>>(), It.IsAny<CancellationToken>()), Times.Never);
        specificTypeMock.Verify(x => x.SaveDocumentAsync(It.IsAny<string>(), It.IsAny<CrdtDocument<TestState>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [IntegrationFact]
    public async Task CompositeStorage_ShouldRouteToPrimaryStorage_WhenNoMappingIsFound()
    {
        // Arrange
        var (provider, primaryMock, specificTypeMock, specificDocMock) = BuildTestNode();
        
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
        specificTypeMock.Verify(x => x.SaveDocumentAsync(It.IsAny<string>(), It.IsAny<CrdtDocument<TestState>>(), It.IsAny<CancellationToken>()), Times.Never);
        specificDocMock.Verify(x => x.SaveDocumentAsync(It.IsAny<string>(), It.IsAny<CrdtDocument<TestState>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [IntegrationFact]
    public async Task CompositeStorage_ShouldBroadcastGlobalOperations_ToAllRegisteredStorages()
    {
        // Arrange
        var (provider, primaryMock, specificTypeMock, specificDocMock) = BuildTestNode();
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
        specificTypeMock.Verify(x => x.TrimAsync(gmvv, It.IsAny<CancellationToken>()), Times.Once);
        specificDocMock.Verify(x => x.TrimAsync(gmvv, It.IsAny<CancellationToken>()), Times.Once);
        
        primaryMock.Verify(x => x.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()), Times.Once);
        specificTypeMock.Verify(x => x.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()), Times.Once);
        specificDocMock.Verify(x => x.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()), Times.Once);
    }
}