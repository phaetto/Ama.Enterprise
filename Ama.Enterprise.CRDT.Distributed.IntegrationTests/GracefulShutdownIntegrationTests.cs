namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Attributes;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

[CrdtAotType(typeof(GracefulShutdownIntegrationTests.ShutdownTestState))]
[CrdtAotType(typeof(Dictionary<string, string>))]
public sealed partial class GracefulShutdownTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(GracefulShutdownIntegrationTests.ShutdownTestState))]
[JsonSerializable(typeof(CrdtDocument<GracefulShutdownIntegrationTests.ShutdownTestState>))]
public sealed partial class GracefulShutdownTestJsonContext : JsonSerializerContext
{
}

public sealed class GracefulShutdownIntegrationTests
{
    private const string TestMeshId = "ShutdownMesh";
    private const string AzureWebJobsShutdownFileEnvVar = "WEBJOBS_SHUTDOWN_FILE";

    public sealed class ShutdownTestState : IEquatable<ShutdownTestState>
    {
        public string Id { get; set; } = "shutdown-doc";

        public Dictionary<string, string> DataMap { get; set; } = new(StringComparer.Ordinal);

        public bool Equals(ShutdownTestState? other)
        {
            if (other is null)
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            if (!string.Equals(Id, other.Id, StringComparison.Ordinal) || DataMap.Count != other.DataMap.Count)
            {
                return false;
            }

            foreach (var kvp in DataMap)
            {
                if (!other.DataMap.TryGetValue(kvp.Key, out var val) || !string.Equals(kvp.Value, val, StringComparison.Ordinal))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj) => Equals(obj as ShutdownTestState);

        public override int GetHashCode() => HashCode.Combine(Id, DataMap.Count);
    }

    [IntegrationFact]
    public async Task CrdtShutdownCoordinator_RequestShutdownAsync_WithNullOrWhitespaceReason_ThrowsArgumentException()
    {
        // Arrange
        var sp = BuildNode("ReplicaValidate");
        var coordinator = sp.GetRequiredService<ICrdtShutdownCoordinator>();

        // Act & Assert
        await Should.ThrowAsync<ArgumentException>(() => coordinator.FlushAllScopesAsync(null!, CancellationToken.None));
        await Should.ThrowAsync<ArgumentException>(() => coordinator.FlushAllScopesAsync(string.Empty, CancellationToken.None));
        await Should.ThrowAsync<ArgumentException>(() => coordinator.FlushAllScopesAsync("   ", CancellationToken.None));
        await Should.ThrowAsync<ArgumentException>(() => coordinator.RequestShutdownAsync(null!, CancellationToken.None));
        await Should.ThrowAsync<ArgumentException>(() => coordinator.RequestShutdownAsync("   ", CancellationToken.None));
    }

    [IntegrationFact]
    public async Task CrdtShutdownCoordinator_FlushModeJournalOnly_DrainsPipelinesDebouncedPatchesAndWal_WithoutSavingCheckpoints()
    {
        // Arrange
        var innerStorage = new MemoryCrdtStorage();
        var capturedBroadcasts = new List<ReadOnlyMemory<byte>>();
        var mockP2p = new Mock<IP2pAlgorithm>();
        mockP2p.Setup(p => p.BroadcastAsync(Capture.In(capturedBroadcasts), It.IsAny<CancellationToken>()))
               .Returns(Task.CompletedTask);

        var sp = BuildNode("ReplicaJournalOnly", opt =>
        {
            opt.FlushModeOnShutdown = CrdtShutdownFlushMode.JournalOnly;
            opt.ActiveSyncEnabled = true;
            opt.ActiveSyncDebounceMilliseconds = 60_000; // Very long debounce window so only FlushAsync triggers it
            opt.JournalMode = CrdtJournalMode.InMemoryBuffer;
            opt.WalFlushIntervalMilliseconds = 60_000;
        }, services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockP2p.Object));
            services.Replace(ServiceDescriptor.Scoped<IDistributedCrdtStorage>(provider =>
                new BufferedDistributedCrdtStorageDecorator(
                    innerStorage,
                    ActivatorUtilities.CreateInstance<InMemoryWriteAheadBuffer>(provider))));
        });

        var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaJournalOnly");
        await scope.Orchestrator.InitializeAsync(CancellationToken.None);
        await scope.Orchestrator.CreateDocumentAsync("doc-journal-only", "shutdown-doc", CancellationToken.None);
        await scope.Orchestrator.ApplyIntentAsync<ShutdownTestState>("doc-journal-only", new MapSetIntent("K1", "V1"), nameof(ShutdownTestState.DataMap), CancellationToken.None);

        // Before shutdown flush: WAL has not drained to innerStorage and debounce timer has not fired
        (await innerStorage.GetJournalCountAsync("default", CancellationToken.None)).ShouldBe(0);
        capturedBroadcasts.Clear();

        var coordinator = sp.GetRequiredService<ICrdtShutdownCoordinator>();

        // Act
        await coordinator.FlushAllScopesAsync("ManualJournalOnlyTest", CancellationToken.None);

        // Assert - WAL drained to innerStorage and debounced active-sync patch was broadcast
        coordinator.IsShuttingDown.ShouldBeTrue();
        coordinator.ShutdownToken.IsCancellationRequested.ShouldBeTrue();
        (await innerStorage.GetJournalCountAsync("default", CancellationToken.None)).ShouldBeGreaterThan(0);
        capturedBroadcasts.ShouldNotBeEmpty();

        // Checkpoints, DVV map, and ClusterState were NOT persisted in JournalOnly mode
        var savedDoc = await innerStorage.LoadDocumentAsync<ShutdownTestState>("default", CrdtRegistryEntry.GetRegistryKey("shutdown-doc", "doc-journal-only"), CancellationToken.None);
        savedDoc.ShouldBeNull();
        (await innerStorage.LoadShardVersionVectorMapAsync("ReplicaJournalOnly", CancellationToken.None)).ShouldBeNull();
        (await innerStorage.LoadClusterStateAsync("ReplicaJournalOnly", CancellationToken.None)).ShouldBeNull();
    }

    [IntegrationFact]
    public async Task CrdtShutdownCoordinator_FlushModeJournalAndCheckpoints_SavesDocumentsDvvAndClusterState_WithoutAntiEntropyDispatch()
    {
        // Arrange
        var innerStorage = new MemoryCrdtStorage();
        var mockSender = new Mock<IDirectMessageSender>();

        var sp = BuildNode("ReplicaCheckpoints", opt =>
        {
            opt.FlushModeOnShutdown = CrdtShutdownFlushMode.JournalAndCheckpoints;
            opt.JournalMode = CrdtJournalMode.InMemoryBuffer;
        }, services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
            services.Replace(ServiceDescriptor.Scoped<IDistributedCrdtStorage>(provider =>
                new BufferedDistributedCrdtStorageDecorator(
                    innerStorage,
                    ActivatorUtilities.CreateInstance<InMemoryWriteAheadBuffer>(provider))));
        });

        var peerRegistry = sp.GetRequiredService<IPeerRegistry>();
        var peerNode = new PeerNode(new PeerId(Guid.NewGuid()), new TcpPeerEndpoint("127.0.0.1", 5050));
        await peerRegistry.AddOrUpdatePeerAsync(TestMeshId, peerNode, PeerStatus.Active, CancellationToken.None);

        var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaCheckpoints");
        await scope.Orchestrator.InitializeAsync(CancellationToken.None);
        await scope.Orchestrator.CreateDocumentAsync("doc-ckpt", "shutdown-doc", CancellationToken.None);
        await scope.Orchestrator.ApplyIntentAsync<ShutdownTestState>("doc-ckpt", new MapSetIntent("CkptKey", "CkptVal"), nameof(ShutdownTestState.DataMap), CancellationToken.None);

        scope.ClusterTracker.TombstoneReplica("EvictedPeerX");
        mockSender.Invocations.Clear();

        var coordinator = sp.GetRequiredService<ICrdtShutdownCoordinator>();

        // Act
        await coordinator.FlushAllScopesAsync("ManualCheckpointTest", CancellationToken.None);

        // Assert - WAL, Document Checkpoints, DVV map, and ClusterState were all persisted
        (await innerStorage.GetJournalCountAsync("default", CancellationToken.None)).ShouldBeGreaterThan(0);

        var compositeKey = CrdtRegistryEntry.GetRegistryKey("shutdown-doc", "doc-ckpt");
        var savedDoc = await innerStorage.LoadDocumentAsync<ShutdownTestState>("default", compositeKey, CancellationToken.None);
        savedDoc.ShouldNotBeNull();
        savedDoc!.Value.Data.DataMap.ShouldContainKeyAndValue("CkptKey", "CkptVal");

        var savedDvv = await innerStorage.LoadShardVersionVectorMapAsync("ReplicaCheckpoints", CancellationToken.None);
        savedDvv.ShouldNotBeNull();
        savedDvv!.ShouldContainKey("default");
        savedDvv["default"].Versions["ReplicaCheckpoints"].ShouldBe(1);

        var savedCluster = await innerStorage.LoadClusterStateAsync("ReplicaCheckpoints", CancellationToken.None);
        savedCluster.ShouldNotBeNull();
        savedCluster!.TombstonedReplicas.ShouldContainKey("EvictedPeerX");

        // Anti-entropy state sync was NOT dispatched in JournalAndCheckpoints mode
        mockSender.Verify(s => s.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [IntegrationFact]
    public async Task CrdtShutdownCoordinator_FlushModeFullSyncAndCheckpoint_DispatchesFinalAntiEntropyAndSavesAllState()
    {
        // Arrange
        var innerStorage = new MemoryCrdtStorage();
        var capturedSends = new List<ReadOnlyMemory<byte>>();
        var mockSender = new Mock<IDirectMessageSender>();
        mockSender.Setup(s => s.SendDirectAsync(It.IsAny<PeerId>(), Capture.In(capturedSends), It.IsAny<CancellationToken>()))
                  .Returns(Task.CompletedTask);

        var sp = BuildNode("ReplicaFullSync", opt =>
        {
            opt.FlushModeOnShutdown = CrdtShutdownFlushMode.FullSyncAndCheckpoint;
        }, services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
            services.Replace(ServiceDescriptor.Scoped<IDistributedCrdtStorage>(_ => innerStorage));
        });

        var peerRegistry = sp.GetRequiredService<IPeerRegistry>();
        var peerId = new PeerId(Guid.NewGuid());
        await peerRegistry.AddOrUpdatePeerAsync(TestMeshId, new PeerNode(peerId, new TcpPeerEndpoint("127.0.0.1", 5051)), PeerStatus.Active, CancellationToken.None);

        var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaFullSync");
        await scope.Orchestrator.InitializeAsync(CancellationToken.None);
        await scope.Orchestrator.CreateDocumentAsync("doc-full-sync", "shutdown-doc", CancellationToken.None);
        await scope.Orchestrator.ApplyIntentAsync<ShutdownTestState>("doc-full-sync", new MapSetIntent("FullKey", "FullVal"), nameof(ShutdownTestState.DataMap), CancellationToken.None);

        capturedSends.Clear();
        var coordinator = sp.GetRequiredService<ICrdtShutdownCoordinator>();

        // Act
        await coordinator.FlushAllScopesAsync("ManualFullSyncTest", CancellationToken.None);

        // Assert - Final anti-entropy CrdtSync was sent AND checkpoints/DVV/cluster state were saved
        capturedSends.Count.ShouldBe(1);
        var serializer = sp.GetRequiredService<ICrdtSerializer>();
        var wrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(capturedSends[0].ToArray());
        wrapper.MessageType.ShouldBe("CrdtSync");

        var compositeKey = CrdtRegistryEntry.GetRegistryKey("shutdown-doc", "doc-full-sync");
        var savedDoc = await innerStorage.LoadDocumentAsync<ShutdownTestState>("default", compositeKey, CancellationToken.None);
        savedDoc.ShouldNotBeNull();
        savedDoc!.Value.Data.DataMap.ShouldContainKeyAndValue("FullKey", "FullVal");
    }

    [IntegrationFact]
    public async Task CrdtShutdownCoordinator_ConcurrentCallsAndRequestShutdown_ExecutesFlushPipelineExactlyOnce_AndStopsHostApplication()
    {
        // Arrange
        var saveDvvCount = 0;
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        mockStorage.SetReturnsDefault(Task.CompletedTask);
        mockStorage.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Callback(() => Interlocked.Increment(ref saveDvvCount))
                   .Returns(Task.CompletedTask);

        var mockLifetime = new Mock<IHostApplicationLifetime>();

        var sp = BuildNode("ReplicaIdempotent", opt =>
        {
            opt.FlushModeOnShutdown = CrdtShutdownFlushMode.JournalAndCheckpoints;
        }, services =>
        {
            services.Replace(ServiceDescriptor.Scoped(_ => mockStorage.Object));
            services.AddSingleton(mockLifetime.Object);
        });

        var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaIdempotent");
        await scope.Orchestrator.InitializeAsync(CancellationToken.None);

        var coordinator = sp.GetRequiredService<ICrdtShutdownCoordinator>();

        // Act - Fire 10 concurrent FlushAllScopesAsync and RequestShutdownAsync calls
        var tasks = Enumerable.Range(0, 10).Select(i =>
            i % 2 == 0
                ? coordinator.FlushAllScopesAsync($"ConcurrentReason_{i}", CancellationToken.None)
                : coordinator.RequestShutdownAsync($"ConcurrentShutdown_{i}", CancellationToken.None)).ToArray();

        await Task.WhenAll(tasks);

        // Assert - Underlying flush pipeline ran only once, and StopApplication was invoked for RequestShutdownAsync
        coordinator.IsShuttingDown.ShouldBeTrue();
        saveDvvCount.ShouldBe(1);
        mockLifetime.Verify(l => l.StopApplication(), Times.Exactly(5));
    }

    [IntegrationFact]
    public async Task CrdtShutdownService_LifecycleStoppingAndStandardStop_FlushesIdempotently()
    {
        // Arrange
        var saveDvvCount = 0;
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        mockStorage.SetReturnsDefault(Task.CompletedTask);
        mockStorage.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Callback(() => Interlocked.Increment(ref saveDvvCount))
                   .Returns(Task.CompletedTask);

        var sp = BuildNode("ReplicaLifecycle", opt =>
        {
            opt.FlushModeOnShutdown = CrdtShutdownFlushMode.JournalAndCheckpoints;
            opt.WatchAzureWebJobsShutdownFile = false;
            opt.ShutdownPosixSignals = [];
        }, services =>
        {
            services.Replace(ServiceDescriptor.Scoped(_ => mockStorage.Object));
        });

        var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaLifecycle");
        await scope.Orchestrator.InitializeAsync(CancellationToken.None);

        var shutdownService = sp.GetServices<IHostedService>().OfType<CrdtShutdownService>().Single();

        // Act - Simulate full IHostedLifecycleService lifecycle (StartingAsync -> StartAsync -> StartedAsync -> StoppingAsync -> StopAsync -> StoppedAsync)
        await shutdownService.StartingAsync(CancellationToken.None);
        await shutdownService.StartAsync(CancellationToken.None);
        await shutdownService.StartedAsync(CancellationToken.None);

        await shutdownService.StoppingAsync(CancellationToken.None);
        await shutdownService.StopAsync(CancellationToken.None);
        await shutdownService.StoppedAsync(CancellationToken.None);
        shutdownService.Dispose();

        // Assert - StoppingAsync triggered the flush and StopAsync was a no-op because CrdtShutdownCoordinator is idempotent
        saveDvvCount.ShouldBe(1);
    }

    [IntegrationFact]
    public async Task CrdtShutdownService_WhenEnableGracefulShutdownIsFalse_SkipsAllFlushOperations()
    {
        // Arrange
        var saveDvvCount = 0;
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        mockStorage.SetReturnsDefault(Task.CompletedTask);
        mockStorage.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Callback(() => Interlocked.Increment(ref saveDvvCount))
                   .Returns(Task.CompletedTask);

        var sp = BuildNode("ReplicaDisabled", opt =>
        {
            opt.EnableGracefulShutdown = false;
        }, services =>
        {
            services.Replace(ServiceDescriptor.Scoped(_ => mockStorage.Object));
        });

        var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaDisabled");
        await scope.Orchestrator.InitializeAsync(CancellationToken.None);

        var shutdownService = sp.GetServices<IHostedService>().OfType<CrdtShutdownService>().Single();
        var coordinator = sp.GetRequiredService<ICrdtShutdownCoordinator>();

        // Act
        await shutdownService.StartAsync(CancellationToken.None);
        await shutdownService.StoppingAsync(CancellationToken.None);
        await shutdownService.StopAsync(CancellationToken.None);
        shutdownService.Dispose();

        // Assert
        coordinator.IsShuttingDown.ShouldBeFalse();
        saveDvvCount.ShouldBe(0);
    }

    [IntegrationFact]
    public async Task CrdtShutdownService_WhenApplicationStoppingTokenFires_TriggersBackgroundFlush()
    {
        // Arrange
        var tcsFlushed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        mockStorage.SetReturnsDefault(Task.CompletedTask);
        mockStorage.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Callback(() => tcsFlushed.TrySetResult())
                   .Returns(Task.CompletedTask);

        using var stoppingCts = new CancellationTokenSource();
        var mockLifetime = new Mock<IHostApplicationLifetime>();
        mockLifetime.SetupGet(l => l.ApplicationStopping).Returns(stoppingCts.Token);

        var sp = BuildNode("ReplicaAppStopping", opt =>
        {
            opt.FlushModeOnShutdown = CrdtShutdownFlushMode.JournalAndCheckpoints;
            opt.WatchAzureWebJobsShutdownFile = false;
            opt.ShutdownPosixSignals = [];
        }, services =>
        {
            services.Replace(ServiceDescriptor.Scoped(_ => mockStorage.Object));
            services.AddSingleton(mockLifetime.Object);
        });

        var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaAppStopping");
        await scope.Orchestrator.InitializeAsync(CancellationToken.None);

        var shutdownService = sp.GetServices<IHostedService>().OfType<CrdtShutdownService>().Single();
        await shutdownService.StartAsync(CancellationToken.None);

        // Act - Cancel ApplicationStopping token
        await stoppingCts.CancelAsync();
        var completed = await Task.WhenAny(tcsFlushed.Task, Task.Delay(5000));
        shutdownService.Dispose();

        // Assert
        completed.ShouldBe(tcsFlushed.Task);
    }

    [IntegrationFact]
    public async Task CrdtShutdownService_WhenAzureWebJobsShutdownFileIsCreated_FlushesScopesAndRequestsApplicationStop()
    {
        // Arrange
        var tempDir = Path.Combine(Path.GetTempPath(), $"ama-webjobs-shutdown-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);
        var sentinelFilePath = Path.Combine(tempDir, "shutdown.sentinel");
        var previousEnvValue = Environment.GetEnvironmentVariable(AzureWebJobsShutdownFileEnvVar);

        try
        {
            Environment.SetEnvironmentVariable(AzureWebJobsShutdownFileEnvVar, sentinelFilePath);

            var tcsStopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var mockLifetime = new Mock<IHostApplicationLifetime>();
            mockLifetime.Setup(l => l.StopApplication()).Callback(() => tcsStopped.TrySetResult());

            var innerStorage = new MemoryCrdtStorage();
            var sp = BuildNode("ReplicaAzureSentinel", opt =>
            {
                opt.FlushModeOnShutdown = CrdtShutdownFlushMode.JournalAndCheckpoints;
                opt.WatchAzureWebJobsShutdownFile = true;
                opt.ShutdownPosixSignals = [];
            }, services =>
            {
                services.Replace(ServiceDescriptor.Scoped<IDistributedCrdtStorage>(_ => innerStorage));
                services.AddSingleton(mockLifetime.Object);
            });

            var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaAzureSentinel");
            await scope.Orchestrator.InitializeAsync(CancellationToken.None);
            await scope.Orchestrator.CreateDocumentAsync("azure-doc", "shutdown-doc", CancellationToken.None);
            await scope.Orchestrator.ApplyIntentAsync<ShutdownTestState>("azure-doc", new MapSetIntent("CloudKey", "CloudVal"), nameof(ShutdownTestState.DataMap), CancellationToken.None);

            var shutdownService = sp.GetServices<IHostedService>().OfType<CrdtShutdownService>().Single();
            await shutdownService.StartAsync(CancellationToken.None);

            // Act - Create the sentinel file watched by FileSystemWatcher
            await File.WriteAllTextAsync(sentinelFilePath, "shutting down");

            var completed = await Task.WhenAny(tcsStopped.Task, Task.Delay(5000));
            shutdownService.Dispose();

            // Assert
            completed.ShouldBe(tcsStopped.Task, "Creating the Azure WebJobs shutdown sentinel file should flush scopes and call StopApplication.");
            var savedDvv = await innerStorage.LoadShardVersionVectorMapAsync("ReplicaAzureSentinel", CancellationToken.None);
            savedDvv.ShouldNotBeNull();
        }
        finally
        {
            Environment.SetEnvironmentVariable(AzureWebJobsShutdownFileEnvVar, previousEnvValue);
            if (Directory.Exists(tempDir))
            {
                try { Directory.Delete(tempDir, recursive: true); } catch { }
            }
        }
    }

    [IntegrationFact]
    public async Task CrdtShutdownCoordinator_WhenStorageHangsPastShutdownTimeout_AbortsGracefullyWithoutDeadlocking()
    {
        // Arrange
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        mockStorage.SetReturnsDefault(Task.CompletedTask);
        mockStorage.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Returns(async (string _, ShardVersionVectorMap _, CancellationToken ct) =>
                   {
                       await Task.Delay(TimeSpan.FromSeconds(30), ct);
                   });

        var sp = BuildNode("ReplicaTimeout", opt =>
        {
            opt.ShutdownTimeoutMilliseconds = 150;
            opt.FlushModeOnShutdown = CrdtShutdownFlushMode.JournalAndCheckpoints;
        }, services =>
        {
            services.Replace(ServiceDescriptor.Scoped(_ => mockStorage.Object));
        });

        var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaTimeout");
        await scope.Orchestrator.InitializeAsync(CancellationToken.None);

        var coordinator = sp.GetRequiredService<ICrdtShutdownCoordinator>();

        // Act - Flush should time out after ~150ms and swallow the OperationCanceledException cleanly
        var flushTask = coordinator.FlushAllScopesAsync("TimeoutTest", CancellationToken.None);
        var completed = await Task.WhenAny(flushTask, Task.Delay(3000));

        // Assert
        completed.ShouldBe(flushTask, "FlushAllScopesAsync should respect ShutdownTimeoutMilliseconds and complete without throwing.");
        await flushTask;
        coordinator.IsShuttingDown.ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task CrdtShutdownCoordinator_WhenMultipleReplicaScopesExist_FlushesAllInitializedScopesAndSkipsUninitializedScopes()
    {
        // Arrange
        var savedReplicaIds = new System.Collections.Concurrent.ConcurrentBag<string>();
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        mockStorage.SetReturnsDefault(Task.CompletedTask);
        mockStorage.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Callback<string, ShardVersionVectorMap, CancellationToken>((replicaId, _, _) => savedReplicaIds.Add(replicaId))
                   .Returns(Task.CompletedTask);

        var sp = BuildNode("ReplicaMulti1", opt =>
        {
            opt.FlushModeOnShutdown = CrdtShutdownFlushMode.JournalAndCheckpoints;
        }, services =>
        {
            services.AddDistributedCrdtReplica("ReplicaMulti2");
            services.AddDistributedCrdtReplica("ReplicaUninitialized");
            services.Replace(ServiceDescriptor.Scoped(_ => mockStorage.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope1 = scopeManager.GetOrCreateScope("ReplicaMulti1");
        var scope2 = scopeManager.GetOrCreateScope("ReplicaMulti2");
        _ = scopeManager.GetOrCreateScope("ReplicaUninitialized"); // Created but NOT initialized

        await scope1.Orchestrator.InitializeAsync(CancellationToken.None);
        await scope2.Orchestrator.InitializeAsync(CancellationToken.None);

        var coordinator = sp.GetRequiredService<ICrdtShutdownCoordinator>();

        // Act
        await coordinator.FlushAllScopesAsync("MultiScopeFlushTest", CancellationToken.None);

        // Assert - Both initialized scopes saved their state, while the uninitialized scope was safely skipped
        savedReplicaIds.ShouldContain("ReplicaMulti1");
        savedReplicaIds.ShouldContain("ReplicaMulti2");
        savedReplicaIds.ShouldNotContain("ReplicaUninitialized");
    }

    [IntegrationFact]
    public async Task CrdtShutdownCoordinator_WhenOneScopeThrowsDuringCheckpoint_ContinuesFlushingRemainingHealthyScopes()
    {
        // Arrange
        var savedDvvReplicas = new System.Collections.Concurrent.ConcurrentBag<string>();
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        mockStorage.SetReturnsDefault(Task.CompletedTask);

        mockStorage.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Returns<string, ShardVersionVectorMap, CancellationToken>((replicaId, _, _) =>
                   {
                       if (string.Equals(replicaId, "ReplicaFaulty", StringComparison.Ordinal))
                       {
                           throw new InvalidOperationException("Simulated storage failure on ReplicaFaulty during shutdown.");
                       }

                       savedDvvReplicas.Add(replicaId);
                       return Task.CompletedTask;
                   });

        var sp = BuildNode("ReplicaFaulty", opt =>
        {
            opt.FlushModeOnShutdown = CrdtShutdownFlushMode.JournalAndCheckpoints;
        }, services =>
        {
            services.AddDistributedCrdtReplica("ReplicaHealthy");
            services.Replace(ServiceDescriptor.Scoped(_ => mockStorage.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var faultyScope = scopeManager.GetOrCreateScope("ReplicaFaulty");
        var healthyScope = scopeManager.GetOrCreateScope("ReplicaHealthy");

        await faultyScope.Orchestrator.InitializeAsync(CancellationToken.None);
        await healthyScope.Orchestrator.InitializeAsync(CancellationToken.None);

        var coordinator = sp.GetRequiredService<ICrdtShutdownCoordinator>();

        // Act - Should not throw despite ReplicaFaulty failing its checkpoint
        await coordinator.FlushAllScopesAsync("FaultIsolationShutdownTest", CancellationToken.None);

        // Assert - ReplicaHealthy still completed its shutdown checkpoint
        savedDvvReplicas.ShouldContain("ReplicaHealthy");
    }

    private static IServiceProvider BuildNode(
        string replicaId,
        Action<DistributedCrdtOptions>? configureOptions = null,
        Action<IServiceCollection>? configureExtra = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            opt.CheckpointIntervalSeconds = 3600;
            opt.MaintenanceIntervalSeconds = 3600;
            opt.AntiEntropyIntervalSeconds = 3600;
            opt.AntiEntropyInitialDelaySeconds = 3600;
            opt.CompactionIntervalSeconds = 3600;
            opt.WatchAzureWebJobsShutdownFile = false;

            configureOptions?.Invoke(opt);
        });

        services.AddDistributedCrdtReplica(replicaId);

        services.AddCrdt()
                .AddCrdtAotContext(new GracefulShutdownTestAotContext())
                .AddCrdtJsonTypeInfoResolver(GracefulShutdownTestJsonContext.Default);

        services.AddDistributedDocumentType<ShutdownTestState>("shutdown-doc");
        services.AddDistributedCrdtP2p(TestMeshId, replicaId);

        services.AddSingleton(Mock.Of<IP2pAlgorithm>());
        services.AddSingleton(Mock.Of<IDirectMessageSender>());
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        services.AddSingleton(new P2pMeshMetadata(TestMeshId));

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
    }
}