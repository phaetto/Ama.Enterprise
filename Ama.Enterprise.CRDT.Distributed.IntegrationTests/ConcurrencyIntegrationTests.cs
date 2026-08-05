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
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;
using Shouldly;

[CrdtAotType(typeof(ConcurrencyIntegrationTests.ConcurrencyTestState))]
public sealed partial class ConcurrencyTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(ConcurrencyIntegrationTests.ConcurrencyTestState))]
[JsonSerializable(typeof(CrdtDocument<ConcurrencyIntegrationTests.ConcurrencyTestState>))]
public sealed partial class ConcurrencyTestJsonContext : JsonSerializerContext
{
}

public sealed class ConcurrencyIntegrationTests
{
    public sealed class ConcurrencyTestState : IEquatable<ConcurrencyTestState>
    {
        public string Id { get; set; } = "concurrent-doc";
        public string Data { get; set; } = string.Empty;

        public bool Equals(ConcurrencyTestState? other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            return Id == other.Id && Data == other.Data;
        }

        public override bool Equals(object? obj) => Equals(obj as ConcurrencyTestState);
        public override int GetHashCode() => HashCode.Combine(Id, Data);
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
                .AddCrdtAotContext(new ConcurrencyTestAotContext())
                .AddCrdtJsonTypeInfoResolver(ConcurrencyTestJsonContext.Default);

        services.AddDistributedDocumentType<ConcurrencyTestState>("concurrent-doc");
        services.AddDistributedCrdtP2p("TestMesh", replicaId);
        
        services.AddSingleton(Mock.Of<IP2pAlgorithm>());
        services.AddSingleton(Mock.Of<IDirectMessageSender>());

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
    }

    [IntegrationFact]
    public async Task CrdtDocumentOrchestrator_ProcessConcurrentCommands_WithoutDeadlockOrCorruption()
    {
        // Arrange
        var serviceProvider = BuildNode("Replica1");
        var scopeManager = serviceProvider.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        const int DocumentCount = 100;

        // Act - Concurrent Document Creations
        var createTasks = Enumerable.Range(0, DocumentCount).Select(i => 
            Task.Run(async () =>
            {
                await orchestrator.CreateDocumentAsync($"doc-{i}", "concurrent-doc", CancellationToken.None);
            })
        ).ToArray();

        await Task.WhenAll(createTasks);
        
        // Sync explicitly to instantiate the document instances locally
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        // Assert - Validating thread-safe channel queueing captured all creations
        var activeDocuments = orchestrator.GetActiveDocuments();
        activeDocuments.Count.ShouldBe(DocumentCount + 1); // +1 accounts for the Registry document

        for (var i = 0; i < DocumentCount; i++)
        {
            var doc = orchestrator.GetDocument<ConcurrencyTestState>($"doc-{i}");
            doc.ShouldNotBeNull();
        }

        // Act - Concurrent Deletions Interleaved with Syncs
        var mixedTasks = Enumerable.Range(0, DocumentCount).Select(i => 
            Task.Run(async () =>
            {
                // Deletions executed completely asynchronously
                await orchestrator.DeleteDocumentAsync($"doc-{i}", CancellationToken.None);
                
                // Intentionally interleaving background Sync calls to ensure the internal single-reader pipeline handles multiplexed states securely
                if (i % 10 == 0)
                {
                    await orchestrator.SyncDocumentsAsync(CancellationToken.None);
                }
            })
        ).ToArray();

        await Task.WhenAll(mixedTasks);

        // Final sync evaluation catching any straggling document states resolving active deletion mapping
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        // Assert
        var remainingDocuments = orchestrator.GetActiveDocuments();
        
        // Only the core CrdtRegistryState document should remain
        remainingDocuments.Count.ShouldBe(1);
        remainingDocuments[0].DocumentId.ShouldBe("system-document-registry");
    }

    [IntegrationFact]
    public async Task CrdtDocumentOrchestrator_ConcurrentMessageDispatching_EvaluatesWithoutBlocking()
    {
        // Arrange
        var mockP2p = new Mock<IP2pAlgorithm>();
        var mockSender = new Mock<IDirectMessageSender>();

        var serviceProvider = BuildNode("Replica1", services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockP2p.Object));
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
        });

        var scopeManager = serviceProvider.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var clusterTracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        await orchestrator.InitializeAsync(CancellationToken.None);
        
        // Map an initial target document natively
        await orchestrator.CreateDocumentAsync("doc-msg-1", "concurrent-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var targetPeerId = new PeerId(Guid.NewGuid());
        const int TotalTasks = 600;

        // Register the target peer in the cluster tracker so DispatchAntiEntropyStateAsync actually routes targeted messages natively
        clusterTracker.UpdatePeerState("RemoteRep", targetPeerId.ToString(), new DottedVersionVector());

        // Act - Flood orchestrator channel with distinct network bound intent messages
        var broadcastTasks = Enumerable.Range(0, TotalTasks).Select(i => Task.Run(async () =>
        {
            // Evenly distribute 3 distinct concurrent pipeline commands
            if (i % 3 == 0)
            {
                await orchestrator.BroadcastPatchAsync("doc-msg-1", It.IsAny<CrdtPatch>(), CancellationToken.None);
            }
            else if (i % 3 == 1)
            {
                await orchestrator.ProvideSnapshotAsync("doc-msg-1", "RemoteRep", targetPeerId, CancellationToken.None);
            }
            else
            {
                await orchestrator.DispatchAntiEntropyStateAsync(CancellationToken.None);
            }
        })).ToArray();

        await Task.WhenAll(broadcastTasks);

        // Assert - The single reader should gracefully process all generic network constraints natively without dropping or blocking
        mockP2p.Verify(x => x.BroadcastAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Exactly(TotalTasks / 3));
        
        // Direct sender is actively bounded by ProvideSnapshot and DispatchAntiEntropyState ensuring direct point-to-point payload pushes securely avoiding Thundering Herd
        mockSender.Verify(x => x.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Exactly(TotalTasks / 3));
    }

    [IntegrationFact]
    public async Task CrdtDocumentOrchestrator_CommandCancellation_SafelyDropsCancelledCommands()
    {
        // Arrange
        var serviceProvider = BuildNode("Replica1");
        var scopeManager = serviceProvider.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        using var preCancelledTokenSource = new CancellationTokenSource();
        preCancelledTokenSource.Cancel();

        // Act - Pass a cancelled token enforcing Task exceptions mapping through the IValueTaskSource natively
        var exception = await Record.ExceptionAsync(() => 
            orchestrator.CreateDocumentAsync("cancel-doc", "concurrent-doc", preCancelledTokenSource.Token));

        // Assert - Specific invocation correctly throws
        exception.ShouldNotBeNull();
        exception.ShouldBeOfType<TaskCanceledException>();

        // Act - Prove the overarching long-lived channel processor loop did NOT crash
        await orchestrator.CreateDocumentAsync("success-doc", "concurrent-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        // Assert - Subsequent command succeeds securely
        var successDoc = orchestrator.GetDocument<ConcurrencyTestState>("success-doc");
        successDoc.ShouldNotBeNull();
        successDoc.DocumentId.ShouldBe("success-doc");
    }

    [IntegrationFact]
    public async Task CrdtDocumentOrchestrator_HeavyMixedOperations_MaintainsStableChannelState()
    {
        // Arrange
        var serviceProvider = BuildNode("Replica1");
        var scopeManager = serviceProvider.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        const int BatchSize = 150;
        var peerId = new PeerId(Guid.NewGuid());

        // Phase 1: Establish baseline generic array concurrently
        var setupTasks = Enumerable.Range(0, BatchSize).Select(i => 
            orchestrator.CreateDocumentAsync($"heavy-doc-{i}", "concurrent-doc", CancellationToken.None)
        ).ToArray();
        
        await Task.WhenAll(setupTasks);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        // Assert Phase 1 completeness
        orchestrator.GetActiveDocuments().Count.ShouldBe(BatchSize + 1);

        // Phase 2: High contention distinct operations hitting single multiplexer sequentially correctly gracefully
        var heavyTasks = new List<Task>();
        for (var i = 0; i < BatchSize; i++)
        {
            var currentIndex = i;
            heavyTasks.Add(Task.Run(async () =>
            {
                // Dispatch network payload
                await orchestrator.ProvideSnapshotAsync($"heavy-doc-{currentIndex}", "TargetReplica", peerId, CancellationToken.None);
                
                // Immediately delete the mapping
                await orchestrator.DeleteDocumentAsync($"heavy-doc-{currentIndex}", CancellationToken.None);
            }));
        }

        // Overlay with concurrent sync routines enforcing strict mapping updates explicitly cleanly
        for (var s = 0; s < 20; s++)
        {
            heavyTasks.Add(Task.Run(() => orchestrator.SyncDocumentsAsync(CancellationToken.None)));
        }

        // Act
        await Task.WhenAll(heavyTasks);

        // Enforce final state mapping execution
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        // Assert - Cleanly deleted documents securely despite aggressive parallel processing overlaps
        var finalDocs = orchestrator.GetActiveDocuments();
        finalDocs.Count.ShouldBe(1);
        finalDocs[0].DocumentId.ShouldBe("system-document-registry");
    }
}