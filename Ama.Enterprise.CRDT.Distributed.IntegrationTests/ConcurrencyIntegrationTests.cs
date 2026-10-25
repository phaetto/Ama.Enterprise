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
            opt.ActiveSyncEnabled = true;
            opt.ActiveSyncDebounceMilliseconds = 0; // Disable debouncing for immediate observer broadcast tracking in tests
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
    public async Task MemoryCrdtStorage_ConcurrentAppends_ShouldIsolateShards_WithoutMixing()
    {
        // Arrange
        var storage = new MemoryCrdtStorage();
        const int TaskCount = 100;
        const string ShardA = "shard-A";
        const string ShardB = "shard-B";

        // Act - Concurrently inject array mappings tracking bounds cleanly isolated by target shards
        var tasks = Enumerable.Range(0, TaskCount).Select(i => Task.Run(async () =>
        {
            var shardId = i % 2 == 0 ? ShardA : ShardB;
            var op = default(CrdtOperation) with { Id = Guid.NewGuid(), ReplicaId = "Replica1", ShardClock = i, ShardId = shardId };
            await storage.AppendAsync("doc-id", new[] { op }, CancellationToken.None);
        })).ToArray();

        await Task.WhenAll(tasks);

        // Assert - Evaluates execution lists retaining completely isolated limits dynamically natively
        var allOps = await storage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        allOps.Count.ShouldBe(TaskCount);

        var opsA = allOps.Where(o => o.Operation.ShardId == ShardA).ToList();
        var opsB = allOps.Where(o => o.Operation.ShardId == ShardB).ToList();

        opsA.Count.ShouldBe(TaskCount / 2);
        opsB.Count.ShouldBe(TaskCount / 2);

        opsA.Any(o => o.Operation.ShardId == ShardB).ShouldBeFalse();
        opsB.Any(o => o.Operation.ShardId == ShardA).ShouldBeFalse();
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

        // Assert - Validating thread-safe channel queueing captured all creations
        var activeDocuments = orchestrator.GetActiveDocuments();
        activeDocuments.Count.ShouldBe(DocumentCount + 1); // +1 accounts for the Registry document

        for (var i = 0; i < DocumentCount; i++)
        {
            var doc = orchestrator.TryGetLocalDocument<ConcurrencyTestState>($"doc-{i}");
            doc.ShouldNotBeNull();
        }

        // Act - Concurrent Deletions Interleaved with Syncs
        var mixedTasks = Enumerable.Range(0, DocumentCount).Select(i => 
            Task.Run(async () =>
            {
                // Deletions executed completely asynchronously
                await orchestrator.DeleteDocumentAsync($"doc-{i}", CancellationToken.None);
            })
        ).ToArray();

        await Task.WhenAll(mixedTasks);

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
        
        var targetDoc = orchestrator.TryGetLocalDocument<ConcurrencyTestState>("doc-msg-1");
        targetDoc.ShouldNotBeNull();

        var targetPeerId = new PeerId(Guid.NewGuid());
        const int TotalTasks = 600;

        // Register the target peer in the cluster tracker so DispatchAntiEntropyStateAsync actually routes targeted messages natively
        clusterTracker.UpdatePeerState("RemoteRep", targetPeerId.ToString(), new ShardVersionVectorMap());

        // Wait to allow any background observers (e.g. CrdtTopologyObserver) tracking new peers to finish firing state syncs natively avoiding verification counts accumulating implicitly.
        await Task.Delay(200);
        mockP2p.Invocations.Clear();

        // Act - Flood orchestrator channel with distinct network bound intent messages
        var broadcastTasks = Enumerable.Range(0, TotalTasks).Select(i => Task.Run(async () =>
        {
            // Evenly distribute 3 distinct concurrent pipeline commands
            if (i % 3 == 0)
            {
                var dummyOp = default(CrdtOperation) with { Id = Guid.NewGuid(), ReplicaId = "Replica1", JsonPath = "$.data" };
                await orchestrator.ApplyPatchAsync<ConcurrencyTestState>("doc-msg-1", new CrdtPatch(new[] { dummyOp }), CancellationToken.None);
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

        // CrdtPatchBroadcaster dispatches outbound patches on an asynchronous background channel;
        // wait briefly for the last enqueued items to be consumed by ProcessBroadcastChannelAsync.
        var expectedCount = TotalTasks / 3;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!cts.Token.IsCancellationRequested)
        {
            if (mockP2p.Invocations.Count >= expectedCount && mockSender.Invocations.Count >= expectedCount)
            {
                break;
            }
            await Task.Delay(25);
        }

        // Assert - The single reader should process all network constraints without dropping or blocking
        mockP2p.Verify(x => x.BroadcastAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Exactly(expectedCount));
        
        // Direct sender is actively bounded by ProvideSnapshot and DispatchAntiEntropyState ensuring direct point-to-point payload pushes avoiding Thundering Herd
        mockSender.Verify(x => x.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Exactly(expectedCount));
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

        // Assert - Subsequent command succeeds securely
        var successDoc = orchestrator.TryGetLocalDocument<ConcurrencyTestState>("success-doc");
        successDoc.ShouldNotBeNull();
        successDoc!.DocumentId.ShouldBe("success-doc");
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

        // Act
        await Task.WhenAll(heavyTasks);

        // Assert - Cleanly deleted documents securely despite aggressive parallel processing overlaps
        var finalDocs = orchestrator.GetActiveDocuments();
        finalDocs.Count.ShouldBe(1);
        finalDocs[0].DocumentId.ShouldBe("system-document-registry");
    }

    [IntegrationFact]
    public async Task CrdtDocumentOrchestrator_ConcurrentFireAndForgetAndAcknowledgedIntents_ShouldConvergeWithoutDroppingOperations()
    {
        // Arrange
        var serviceProvider = BuildNode("Replica1");
        var scopeManager = serviceProvider.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");

        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManager = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        const int DocCount = 10;
        const int OpsPerDoc = 20;

        for (int d = 0; d < DocCount; d++)
        {
            await orchestrator.CreateDocumentAsync($"ff-doc-{d}", "concurrent-doc", CancellationToken.None);
        }

        // Act - Mix FireAndForget and Acknowledged intents concurrently across all documents sharing the "default" shard
        var tasks = new List<Task>();
        for (int d = 0; d < DocCount; d++)
        {
            var docId = $"ff-doc-{d}";
            for (int op = 0; op < OpsPerDoc; op++)
            {
                var currentOp = op;
                tasks.Add(Task.Run(async () =>
                {
                    var mode = currentOp % 2 == 0
                        ? Models.IntentDeliveryMode.FireAndForget
                        : Models.IntentDeliveryMode.Acknowledged;

                    var intent = new Ama.CRDT.Models.Intents.SetIntent($"Val_{currentOp}");
                    await orchestrator.ApplyIntentAsync<ConcurrencyTestState>(docId, intent, nameof(ConcurrencyTestState.Data), mode, CancellationToken.None);
                }));
            }
        }

        await Task.WhenAll(tasks);

        // Flush each document queue with a final Acknowledged intent so all preceding FireAndForget commands have drained
        for (int d = 0; d < DocCount; d++)
        {
            var finalIntent = new Ama.CRDT.Models.Intents.SetIntent($"Final_{d}");
            await orchestrator.ApplyIntentAsync<ConcurrencyTestState>(
                $"ff-doc-{d}",
                finalIntent,
                nameof(ConcurrencyTestState.Data),
                Models.IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
        }

        // Assert - Every single operation incremented the shard clock contiguously with zero gaps or lost dots
        var expectedTotalOps = (DocCount * OpsPerDoc) + DocCount;
        var shardSnapshot = await dvvManager.GetFullSnapshotAsync(CancellationToken.None);

        shardSnapshot.ShouldContainKey("default");
        shardSnapshot["default"].Versions["Replica1"].ShouldBe(expectedTotalOps);
        (shardSnapshot["default"].Dots.TryGetValue("Replica1", out var dots) ? dots.Count : 0).ShouldBe(0);
    }

    [IntegrationFact]
    public async Task DistributedCrdtDocument_SmallBoundedChannelCapacity_AppliesBackpressureWithoutDroppingOperationsOrDeadlocking()
    {
        // Arrange - Set a very small bounded channel capacity (4) to force BoundedChannelFullMode.Wait backpressure
        var serviceProvider = BuildNode("Replica1", services =>
        {
            services.Configure<Models.DistributedCrdtOptions>(opt =>
            {
                opt.ChannelCapacity = 4;
            });
        });

        var scopeManager = serviceProvider.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManager = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        await orchestrator.CreateDocumentAsync("bounded-doc", "concurrent-doc", CancellationToken.None);

        const int TotalWriters = 80;

        // Act - 80 concurrent writers flood a channel of capacity 4 with mixed intents, snapshots, and checkpoints
        var tasks = Enumerable.Range(0, TotalWriters).Select(i => Task.Run(async () =>
        {
            var mode = i % 3 == 0
                ? Models.IntentDeliveryMode.FireAndForget
                : Models.IntentDeliveryMode.Acknowledged;

            var intent = new Ama.CRDT.Models.Intents.SetIntent($"Backpressure_{i}");
            await orchestrator.ApplyIntentAsync<ConcurrencyTestState>("bounded-doc", intent, nameof(ConcurrencyTestState.Data), mode, CancellationToken.None);

            if (i % 10 == 0)
            {
                var doc = orchestrator.TryGetLocalDocument<ConcurrencyTestState>("bounded-doc")!;
                _ = await doc.GetSnapshotDataAsync(CancellationToken.None);
                await doc.CheckpointAsync(CancellationToken.None);
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        // Drain any remaining FireAndForget commands with one final Acknowledged intent
        await orchestrator.ApplyIntentAsync<ConcurrencyTestState>(
            "bounded-doc",
            new Ama.CRDT.Models.Intents.SetIntent("FinalDrain"),
            nameof(ConcurrencyTestState.Data),
            Models.IntentDeliveryMode.Acknowledged,
            CancellationToken.None);

        // Assert - All 80 + 1 operations were processed without loss
        var dvv = await dvvManager.GetFullSnapshotAsync(CancellationToken.None);
        dvv["default"].Versions["Replica1"].ShouldBe(TotalWriters + 1);
    }

    [IntegrationFact]
    public async Task CrdtDocumentOrchestrator_ConcurrentLazyPromotionFromMultipleThreads_InstantiatesSingleDocumentAndAppliesAllIntents()
    {
        // Arrange
        var serviceProvider = BuildNode("Replica1");
        var scopeManager = serviceProvider.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManager = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        // Verify document is not yet in activeDocuments or Registry
        orchestrator.TryGetLocalDocument<ConcurrencyTestState>("race-promote-doc").ShouldBeNull();

        const int ConcurrentCallers = 50;

        // Act - 50 concurrent threads simultaneously trigger lazy promotion and intent application on the same uncreated document
        var tasks = Enumerable.Range(0, ConcurrentCallers).Select(i => Task.Run(async () =>
        {
            var intent = new Ama.CRDT.Models.Intents.SetIntent($"RaceVal_{i}");
            return await orchestrator.ApplyIntentAsync<ConcurrencyTestState>(
                "race-promote-doc",
                intent,
                nameof(ConcurrencyTestState.Data),
                Models.IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
        })).ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - Every caller succeeded, only 1 instance exists, and the shard clock advanced exactly 50 times
        results.ShouldAllBe(r => r);

        var activeDocs = orchestrator.GetActiveDocuments().Where(d => d.DocumentId == "race-promote-doc").ToList();
        activeDocs.Count.ShouldBe(1);

        var dvv = await dvvManager.GetFullSnapshotAsync(CancellationToken.None);
        dvv["default"].Versions["Replica1"].ShouldBe(ConcurrentCallers);
    }

    [IntegrationFact]
    public async Task DistributedCrdtDocument_AfterFlushAsyncCompletesChannel_SubsequentCommandsFailGracefullyWithoutHanging()
    {
        // Arrange
        var serviceProvider = BuildNode("Replica1");
        var scopeManager = serviceProvider.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var commandPipeline = scope.ServiceProvider.GetRequiredService<IDocumentCommandPipeline>();
        var patcher = scope.ServiceProvider.GetRequiredService<Ama.CRDT.Services.IAsyncCrdtPatcher>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        await orchestrator.CreateDocumentAsync("post-flush-doc", "concurrent-doc", CancellationToken.None);
        var doc = orchestrator.TryGetLocalDocument<ConcurrencyTestState>("post-flush-doc")!;

        // Act 1 - Flush the document command pipeline (which completes the underlying command channel)
        await commandPipeline.FlushAsync(CancellationToken.None);

        // Act 2 - Enqueue a FireAndForget intent and an Acknowledged intent after the channel is closed
        var ffException = await Record.ExceptionAsync(() =>
            doc.ApplyRemoteIntentAsync(
                new SetIntent("AfterFlushFF"),
                nameof(ConcurrencyTestState.Data),
                patcher,
                Models.IntentDeliveryMode.FireAndForget,
                CancellationToken.None));

        var ackException = await Record.ExceptionAsync(() =>
            doc.ApplyRemoteIntentAsync(
                new SetIntent("AfterFlushAck"),
                nameof(ConcurrencyTestState.Data),
                patcher,
                Models.IntentDeliveryMode.Acknowledged,
                CancellationToken.None));

        // Assert - Both FireAndForget and Acknowledged commands fail immediately with ChannelClosedException (InvalidOperationException) without deadlocking
        ffException.ShouldNotBeNull();
        ffException.ShouldBeAssignableTo<InvalidOperationException>();
        ackException.ShouldNotBeNull();
        ackException.ShouldBeAssignableTo<InvalidOperationException>();
    }
}