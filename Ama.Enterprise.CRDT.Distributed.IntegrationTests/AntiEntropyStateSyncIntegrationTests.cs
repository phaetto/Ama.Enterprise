namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Generic;
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
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;
using Shouldly;

[CrdtAotType(typeof(AntiEntropyStateSyncIntegrationTests.SyncTestState))]
[CrdtAotType(typeof(Dictionary<string, string>))]
public sealed partial class AntiEntropyStateSyncTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(AntiEntropyStateSyncIntegrationTests.SyncTestState))]
[JsonSerializable(typeof(CrdtDocument<AntiEntropyStateSyncIntegrationTests.SyncTestState>))]
public sealed partial class AntiEntropyStateSyncTestJsonContext : JsonSerializerContext
{
}

public sealed class AntiEntropyStateSyncIntegrationTests
{
    private const string TestMeshId = "SyncMesh";

    public sealed class SyncTestState
    {
        public string Id { get; set; } = "sync-doc";
        public Dictionary<string, string> DataMap { get; set; } = new(StringComparer.Ordinal);
    }

    private IServiceProvider BuildNode(string replicaId, Action<IServiceCollection>? configureExtra = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            opt.ReplicaId = replicaId;
            opt.ActiveSyncEnabled = false;
        });

        services.AddCrdt()
                .AddCrdtAotContext(new AntiEntropyStateSyncTestAotContext())
                .AddCrdtJsonTypeInfoResolver(AntiEntropyStateSyncTestJsonContext.Default);

        services.AddDistributedDocumentType<SyncTestState>("sync-doc-1");
        services.AddDistributedDocumentType<SyncTestState>("sync-doc-2");
        services.AddDistributedCrdtP2p(TestMeshId);

        services.AddSingleton(Mock.Of<IP2pProtocol>());
        services.AddSingleton(Mock.Of<IDirectMessageSender>());

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
    }

    [IntegrationFact]
    public async Task ProcessStateSyncAsync_ShouldRetrieveMissingOpsOnce_AndBroadcastPerDocument_Correctly()
    {
        // Arrange
        var mockSender = new Mock<IDirectMessageSender>();
        var capturedDirectSends = new List<ReadOnlyMemory<byte>>();
        mockSender.Setup(p => p.SendDirectAsync(It.IsAny<PeerId>(), Capture.In(capturedDirectSends), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var sp = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
        });

        var scopeProvider = sp.GetRequiredService<DistributedCrdtScopeProvider>();
        var orchestrator = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher = scopeProvider.Scope.ServiceProvider.GetRequiredService<ICrdtPatcher>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        
        await orchestrator.CreateDocumentAsync("sync-doc-1", "sync-doc-1", CancellationToken.None);
        await orchestrator.CreateDocumentAsync("sync-doc-2", "sync-doc-2", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var doc1 = orchestrator.GetDocument<SyncTestState>("sync-doc-1")!;
        var doc2 = orchestrator.GetDocument<SyncTestState>("sync-doc-2")!;

        var intent1 = new MapSetIntent("testKey", "Val1");
        var op1 = patcher.GenerateOperation(doc1.Document, x => x.DataMap, intent1);

        var intent2 = new MapSetIntent("testKey", "Val2");
        var op2 = patcher.GenerateOperation(doc2.Document, x => x.DataMap, intent2);

        await doc1.ApplyPatchAsync(new CrdtPatch(new[] { op1 }), CancellationToken.None);
        await doc2.ApplyPatchAsync(new CrdtPatch(new[] { op2 }), CancellationToken.None);

        var handler = sp.GetRequiredKeyedService<IApplicationPayloadHandler>(TestMeshId);
        var serializer = sp.GetRequiredService<ICrdtSerializer>();

        var remoteState = new DottedVersionVector();
        var syncMsg = new CrdtStateSyncMessage("RemoteReplica", remoteState);
        var syncBytes = serializer.SerializeToBytes(syncMsg);
        var wrapper = new CrdtMessageWrapper("Cluster", "CrdtSync", syncBytes);
        var wrapperBytes = serializer.SerializeToBytes(wrapper);

        // Act
        await handler.HandlePayloadAsync(TestMeshId, new PeerId(Guid.NewGuid()), wrapperBytes, CancellationToken.None);

        // Assert
        capturedDirectSends.ShouldNotBeEmpty();

        var broadcastedDocIds = new HashSet<string>();
        foreach (var payload in capturedDirectSends)
        {
            var msgWrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(payload.ToArray());
            msgWrapper.MessageType.ShouldBe("CrdtOps");
            broadcastedDocIds.Add(msgWrapper.DocumentId);
        }

        broadcastedDocIds.ShouldContain("sync-doc-1");
        broadcastedDocIds.ShouldContain("sync-doc-2");
        broadcastedDocIds.ShouldContain(orchestrator.Registry.DocumentId);
    }
}