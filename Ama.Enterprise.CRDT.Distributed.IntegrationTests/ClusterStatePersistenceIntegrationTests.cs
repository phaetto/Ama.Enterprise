namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

public sealed class ClusterStatePersistenceIntegrationTests
{
    private IServiceProvider BuildNode(string replicaId, Action<IServiceCollection>? configureExtra = null)
    {
        return BuildNodeWithStorage(replicaId, null, configureExtra);
    }

    private IServiceProvider BuildNodeWithStorage(string replicaId, IDistributedCrdtStorage? customStorage, Action<IServiceCollection>? configureExtra = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            opt.CheckpointIntervalSeconds = 1;
            opt.ActiveSyncEnabled = true;
        });

        services.AddDistributedCrdtReplica(replicaId);

        services.AddCrdt()
                .AddCrdtAotContext(new DistributedCrdtSystemAotContext())
                .AddCrdtJsonTypeInfoResolver(DistributedCrdtSystemJsonContext.Default);

        if (customStorage != null)
        {
            services.Replace(ServiceDescriptor.Singleton(customStorage));
        }

        services.AddSingleton(Mock.Of<IP2pAlgorithm>());

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
    }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
    private async IAsyncEnumerable<JournaledOperation> EmptyJournalStream()
    {
        yield break;
    }
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

    [IntegrationFact]
    public void ClusterStateTracker_UpdateAndTombstone_StateTransitions()
    {
        // Arrange
        var sp = BuildNode("Replica1");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();
        var remoteDvv = new DottedVersionVector();
        remoteDvv.Versions["RemoteReplica1"] = 10;
        
        var remoteMap = new ShardVersionVectorMap { { "", remoteDvv } };

        // Act - Track a new peer
        tracker.UpdatePeerState("RemoteReplica1", "NetworkId1", remoteMap);

        // Assert
        var states = tracker.GetClusterStates();
        states.Count.ShouldBe(1);
        states[0][""].Versions["RemoteReplica1"].ShouldBe(10);
        tracker.IsReplicaTombstoned("RemoteReplica1").ShouldBeFalse();

        // Act - Unmap network
        tracker.RemovePeerByNetworkId("NetworkId1");

        // Assert - State preserved offline
        tracker.GetClusterStates().Count.ShouldBe(1);

        // Act - Force tombstone
        tracker.TombstoneReplica("RemoteReplica1");

        // Assert
        tracker.GetClusterStates().Count.ShouldBe(0);
        tracker.IsReplicaTombstoned("RemoteReplica1").ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task ClusterStateTracker_ShouldIsolateShards_InShardVersionVectorMap_WhenPersisting()
    {
        // Arrange
        var storageMock = new Mock<IDistributedCrdtStorage>();
        storageMock.SetReturnsDefault(Task.CompletedTask);
        var tcs = new TaskCompletionSource<ShardVersionVectorMap>();

        storageMock.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Callback<string, ShardVersionVectorMap, CancellationToken>((_, map, _) => tcs.TrySetResult(map))
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.SaveClusterStateAsync(It.IsAny<string>(), It.IsAny<ClusterStateSnapshotDto>(), It.IsAny<CancellationToken>()))
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.GetJournalCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(0);

        storageMock.Setup(s => s.LoadClusterStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync((ClusterStateSnapshotDto?)null);

        storageMock.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());

        storageMock.Setup(s => s.LoadShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new ShardVersionVectorMap());

        var sp = BuildNodeWithStorage("Replica1", storageMock.Object);

        // Initialize orchestrator natively prior to interacting with documents to avoid null reference exceptions.
        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

        lock (context.ShardVersionVectors)
        {
            var dvvA = new DottedVersionVector(); dvvA.Versions["Replica1"] = 5;
            var dvvB = new DottedVersionVector(); dvvB.Versions["Replica1"] = 10;
            context.ShardVersionVectors["shard-A"] = dvvA;
            context.ShardVersionVectors["shard-B"] = dvvB;
        }

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();

        // Act
        await checkpointService.StartAsync(CancellationToken.None);
        var capturedMap = await tcs.Task;
        await checkpointService.StopAsync(CancellationToken.None);

        // Assert - Ensures persistence naturally isolates multi-shard matrices gracefully
        capturedMap.ShouldNotBeNull();
        capturedMap.ShouldContainKey("shard-A");
        capturedMap.ShouldContainKey("shard-B");
        capturedMap["shard-A"].Versions["Replica1"].ShouldBe(5);
        capturedMap["shard-B"].Versions["Replica1"].ShouldBe(10);
    }

    [IntegrationFact]
    public async Task ClusterStateTracker_TombstoneCooldown_PurgesExpiredReplicas()
    {
        // Arrange
        var sp = BuildNode("Replica1");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();

        // Act
        tracker.TombstoneReplica("RemoteReplica1");

        // Assert
        tracker.IsReplicaTombstoned("RemoteReplica1").ShouldBeTrue();

        // Act - Attempt clean up with large cooldown
        tracker.CleanupExpiredTombstones(TimeSpan.FromMinutes(5));

        // Assert
        tracker.IsReplicaTombstoned("RemoteReplica1").ShouldBeTrue();

        // Act - Trigger short cooldown
        await Task.Delay(50);
        tracker.CleanupExpiredTombstones(TimeSpan.FromMilliseconds(10));

        // Assert
        tracker.IsReplicaTombstoned("RemoteReplica1").ShouldBeFalse();
    }

    [IntegrationFact]
    public async Task CrdtMaintenanceService_TombstoneCooldown_PurgesTombstones()
    {
        // Arrange
        var sp = BuildNode("Replica1", services =>
        {
            services.Configure<DistributedCrdtOptions>(opt =>
            {
                opt.MaintenanceIntervalSeconds = 1;
                opt.PeerTombstoneCooldownSeconds = 1;
            });
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();

        tracker.TombstoneReplica("RemoteReplica1");
        tracker.IsReplicaTombstoned("RemoteReplica1").ShouldBeTrue();

        var maintenanceService = sp.GetServices<IHostedService>().OfType<CrdtMaintenanceService>().First();

        // Act
        await maintenanceService.StartAsync(CancellationToken.None);
        await Task.Delay(2500);
        await maintenanceService.StopAsync(CancellationToken.None);

        // Assert
        tracker.IsReplicaTombstoned("RemoteReplica1").ShouldBeFalse();
    }

    [IntegrationFact]
    public async Task CrdtInitializationService_Startup_RestoresPersistedState()
    {
        // Arrange
        var mockStorage = new Mock<IDistributedCrdtStorage>();
        mockStorage.SetReturnsDefault(Task.CompletedTask);
        var sp = BuildNodeWithStorage("Replica1", mockStorage.Object);

        var savedDvv = new DottedVersionVector();
        savedDvv.Versions["Replica1"] = 100;
        
        var savedMap = new ShardVersionVectorMap { { "", savedDvv } };
        
        mockStorage.Setup(s => s.LoadShardVersionVectorMapAsync("Replica1", It.IsAny<CancellationToken>()))
                   .ReturnsAsync(savedMap);
        
        mockStorage.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());

        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();

        // Act
        await initService.StartAsync(CancellationToken.None);

        // Assert
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

        context.ShardVersionVectors[""].Versions["Replica1"].ShouldBe(100);
    }

    [IntegrationFact]
    public async Task CrdtCheckpointService_ShouldExportAndPersistClusterState_OnInterval()
    {
        // Arrange
        var storageMock = new Mock<IDistributedCrdtStorage>();
        storageMock.SetReturnsDefault(Task.CompletedTask);
        var tcs = new TaskCompletionSource<ClusterStateSnapshotDto>();

        storageMock.Setup(s => s.SaveClusterStateAsync(It.IsAny<string>(), It.IsAny<ClusterStateSnapshotDto>(), It.IsAny<CancellationToken>()))
                   .Callback<string, ClusterStateSnapshotDto, CancellationToken>((_, state, _) => tcs.TrySetResult(state))
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Returns(Task.CompletedTask);
                   
        storageMock.Setup(s => s.GetJournalCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(0);

        storageMock.Setup(s => s.LoadClusterStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync((ClusterStateSnapshotDto?)null);

        storageMock.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());

        storageMock.Setup(s => s.LoadShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new ShardVersionVectorMap());

        var sp = BuildNodeWithStorage("Replica1", storageMock.Object);

        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var tracker = scope.ClusterTracker;

        var dvv = new DottedVersionVector();
        dvv.Versions["ReplicaB"] = 15;
        tracker.UpdatePeerState("ReplicaB", "NetworkB", new ShardVersionVectorMap { { "", dvv } });
        tracker.TombstoneReplica("ReplicaC");

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();

        // Act
        await checkpointService.StartAsync(CancellationToken.None);
        var completedTask = await Task.WhenAny(tcs.Task, Task.Delay(5000));
        await checkpointService.StopAsync(CancellationToken.None);

        // Assert
        completedTask.ShouldBe(tcs.Task, "The background checkpoint service did not invoke SaveClusterStateAsync within the allotted time.");
        
        var capturedState = await tcs.Task;
        capturedState.ShouldNotBeNull();
        
        capturedState.TombstonedReplicas.ShouldContainKey("ReplicaC");
        capturedState.PeerStates.ShouldContainKey("ReplicaB");
        capturedState.PeerStates["ReplicaB"].State[""].Versions["ReplicaB"].ShouldBe(15);
    }

    [IntegrationFact]
    public async Task CrdtInitializationService_ShouldImportClusterState_OnStartup_PreventingAmnesia()
    {
        // Arrange
        var savedDto = new ClusterStateSnapshotDto();
        savedDto.TombstonedReplicas.Add("ReplicaZOMBIE", DateTime.UtcNow);
        savedDto.NetworkIdToReplicaId["NetworkD"] = "ReplicaD";

        var dvv = new DottedVersionVector();
        dvv.Versions["ReplicaD"] = 42;
        
        savedDto.PeerStates["ReplicaD"] = new ClusterPeerStateDto 
        { 
            State = new ShardVersionVectorMap { { "", dvv } }, 
            LastSeen = DateTime.UtcNow 
        };

        var storageMock = new Mock<IDistributedCrdtStorage>();
        storageMock.SetReturnsDefault(Task.CompletedTask);
        
        storageMock.Setup(s => s.LoadClusterStateAsync("Replica1", It.IsAny<CancellationToken>()))
                   .ReturnsAsync(savedDto);

        storageMock.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());
                   
        storageMock.Setup(s => s.LoadShardVersionVectorMapAsync("Replica1", It.IsAny<CancellationToken>()))
                   .ReturnsAsync((ShardVersionVectorMap?)null);

        var sp = BuildNodeWithStorage("Replica1", storageMock.Object);
        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();

        // Act
        await initService.StartAsync(CancellationToken.None);

        // Assert
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var tracker = scope.ClusterTracker;

        tracker.IsReplicaTombstoned("ReplicaZOMBIE").ShouldBeTrue();
        
        var states = tracker.GetClusterStates();
        states.Count.ShouldBe(1);
        states[0][""].Versions.ShouldContainKeyAndValue("ReplicaD", 42);
        
        var tombstonedPeer = tracker.TombstonePeerByNetworkId("NetworkD");
        tombstonedPeer.ShouldNotBe("ReplicaD"); // We don't load the network volatile info anymore
        tracker.TombstoneReplica("ReplicaD");
        tracker.IsReplicaTombstoned("ReplicaD").ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task CrdtCheckpointService_ShouldSkipBlindWrites_WhenConfiguredAndUnchanged()
    {
        // Arrange
        var saveClusterCount = 0;
        var saveDvvCount = 0;
        var tcsFirstWrite = new TaskCompletionSource();

        var storageMock = new Mock<IDistributedCrdtStorage>();
        storageMock.SetReturnsDefault(Task.CompletedTask);

        storageMock.Setup(s => s.SaveClusterStateAsync(It.IsAny<string>(), It.IsAny<ClusterStateSnapshotDto>(), It.IsAny<CancellationToken>()))
                   .Callback(() => Interlocked.Increment(ref saveClusterCount))
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Callback(() => 
                   {
                       Interlocked.Increment(ref saveDvvCount);
                       tcsFirstWrite.TrySetResult();
                   })
                   .Returns(Task.CompletedTask);
                   
        storageMock.Setup(s => s.GetJournalCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(0);

        storageMock.Setup(s => s.LoadClusterStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync((ClusterStateSnapshotDto?)null);

        storageMock.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());

        storageMock.Setup(s => s.LoadShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new ShardVersionVectorMap());

        var sp = BuildNodeWithStorage("Replica1", storageMock.Object, services => 
        {
            services.Configure<DistributedCrdtOptions>(opt => 
            {
                opt.CheckpointIntervalSeconds = 1;
                opt.AvoidBlindCheckpointWrites = true;
            });
        });

        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();

        // Act
        await checkpointService.StartAsync(CancellationToken.None);
        await Task.WhenAny(tcsFirstWrite.Task, Task.Delay(5000));
        await Task.Delay(2500); // 2 more cycles naturally
        await checkpointService.StopAsync(CancellationToken.None);

        // Assert
        saveClusterCount.ShouldBe(1, "The background checkpoint service should have skipped subsequent unchanged cluster state writes.");
        saveDvvCount.ShouldBe(1, "The background checkpoint service should have skipped subsequent unchanged DVV writes.");
    }

    [IntegrationFact]
    public async Task CrdtCheckpointService_ShouldExecuteWrite_WhenConfiguredButStateMutates()
    {
        // Arrange
        var saveClusterCount = 0;
        var saveDvvCount = 0;
        var tcsFirstWrite = new TaskCompletionSource();
        var tcsSecondWrite = new TaskCompletionSource();

        var storageMock = new Mock<IDistributedCrdtStorage>();
        storageMock.SetReturnsDefault(Task.CompletedTask);

        storageMock.Setup(s => s.SaveClusterStateAsync(It.IsAny<string>(), It.IsAny<ClusterStateSnapshotDto>(), It.IsAny<CancellationToken>()))
                   .Callback(() => 
                   {
                       var count = Interlocked.Increment(ref saveClusterCount);
                       if (count == 1) tcsFirstWrite.TrySetResult();
                       if (count == 2) tcsSecondWrite.TrySetResult();
                   })
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Callback(() => Interlocked.Increment(ref saveDvvCount))
                   .Returns(Task.CompletedTask);
                   
        storageMock.Setup(s => s.GetJournalCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(0);

        storageMock.Setup(s => s.LoadClusterStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync((ClusterStateSnapshotDto?)null);

        storageMock.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());

        storageMock.Setup(s => s.LoadShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new ShardVersionVectorMap());

        var sp = BuildNodeWithStorage("Replica1", storageMock.Object, services => 
        {
            services.Configure<DistributedCrdtOptions>(opt => 
            {
                opt.CheckpointIntervalSeconds = 1;
                opt.AvoidBlindCheckpointWrites = true;
            });
        });

        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();

        // Act
        await checkpointService.StartAsync(CancellationToken.None);
        await Task.WhenAny(tcsFirstWrite.Task, Task.Delay(5000));

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        
        scope.ClusterTracker.TombstoneReplica("ReplicaMutated");
        
        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        lock (context.ShardVersionVectors)
        {
            if (!context.ShardVersionVectors.TryGetValue("", out var dvv))
            {
                dvv = new DottedVersionVector();
                context.ShardVersionVectors[""] = dvv;
            }
            dvv.Versions["ReplicaMutated"] = 10;
        }

        await Task.WhenAny(tcsSecondWrite.Task, Task.Delay(5000));
        await checkpointService.StopAsync(CancellationToken.None);

        // Assert
        saveClusterCount.ShouldBeGreaterThanOrEqualTo(2, "The mutation should have bypassed the blind write rule triggering a persist.");
        saveDvvCount.ShouldBeGreaterThanOrEqualTo(2, "The DVV mutation should have bypassed the blind write rule triggering a persist.");
    }

    [IntegrationFact]
    public void ClusterStateTracker_UpdatePeerState_WhenPeerReconnectsWithNewNetworkId_ReplacesStaleSessionMapping()
    {
        // Arrange
        var sp = BuildNode("Replica1");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();

        var dvv = new DottedVersionVector();
        dvv.Versions["RemoteReplica1"] = 5;
        var map = new ShardVersionVectorMap { { "default", dvv } };

        tracker.UpdatePeerState("RemoteReplica1", "OldNetworkId", map);
        tracker.GetPeerIdByReplicaId("RemoteReplica1").ShouldBe("OldNetworkId");
        tracker.GetReplicaIdByPeerId("OldNetworkId").ShouldBe("RemoteReplica1");

        // Act - Peer reconnects with a new network session ID
        dvv.Versions["RemoteReplica1"] = 6;
        tracker.UpdatePeerState("RemoteReplica1", "NewNetworkId", map);

        // Assert - 1:1 bidirectional mapping is enforced and old network ID is evicted
        tracker.GetPeerIdByReplicaId("RemoteReplica1").ShouldBe("NewNetworkId");
        tracker.GetReplicaIdByPeerId("NewNetworkId").ShouldBe("RemoteReplica1");
        tracker.GetReplicaIdByPeerId("OldNetworkId").ShouldBeNull();
    }

    [IntegrationFact]
    public async Task CrdtCheckpointService_ShouldDetectDotMutationsInDvv_WhenAvoidBlindWritesIsEnabled()
    {
        // Arrange
        var saveDvvCount = 0;
        var tcsFirstWrite = new TaskCompletionSource();
        var tcsSecondWrite = new TaskCompletionSource();

        var storageMock = new Mock<IDistributedCrdtStorage>();
        storageMock.SetReturnsDefault(Task.CompletedTask);

        storageMock.Setup(s => s.SaveClusterStateAsync(It.IsAny<string>(), It.IsAny<ClusterStateSnapshotDto>(), It.IsAny<CancellationToken>()))
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Callback(() =>
                   {
                       var count = Interlocked.Increment(ref saveDvvCount);
                       if (count == 1) tcsFirstWrite.TrySetResult();
                       if (count == 2) tcsSecondWrite.TrySetResult();
                   })
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.GetJournalCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(0);

        storageMock.Setup(s => s.LoadClusterStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync((ClusterStateSnapshotDto?)null);

        storageMock.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());

        storageMock.Setup(s => s.LoadShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new ShardVersionVectorMap());

        var sp = BuildNodeWithStorage("Replica1", storageMock.Object, services =>
        {
            services.Configure<DistributedCrdtOptions>(opt =>
            {
                opt.CheckpointIntervalSeconds = 1;
                opt.AvoidBlindCheckpointWrites = true;
            });
        });

        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();

        // Act
        await checkpointService.StartAsync(CancellationToken.None);
        await Task.WhenAny(tcsFirstWrite.Task, Task.Delay(5000));

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

        // Mutate ONLY the Dots collection (out-of-order causal dot) without changing Versions
        lock (context.ShardVersionVectors)
        {
            if (!context.ShardVersionVectors.TryGetValue("system-document-registry", out var dvv))
            {
                dvv = new DottedVersionVector();
                context.ShardVersionVectors["system-document-registry"] = dvv;
            }
            dvv.Add("ReplicaOutOfOrder", 5);
        }

        var completedSecond = await Task.WhenAny(tcsSecondWrite.Task, Task.Delay(5000));
        await checkpointService.StopAsync(CancellationToken.None);

        // Assert
        completedSecond.ShouldBe(tcsSecondWrite.Task, "A mutation in DVV Dots should be detected and persisted even when contiguous Versions are unchanged.");
        saveDvvCount.ShouldBeGreaterThanOrEqualTo(2);
    }

    [IntegrationFact]
    public async Task CrdtCheckpointService_WhenStorageThrowsTransientException_RecoversOnSubsequentCycleAndRetainsDirtyState()
    {
        // Arrange
        var dvvAttemptCount = 0;
        var tcsRecoveredWrite = new TaskCompletionSource();

        var storageMock = new Mock<IDistributedCrdtStorage>();
        storageMock.SetReturnsDefault(Task.CompletedTask);

        storageMock.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Returns<string, ShardVersionVectorMap, CancellationToken>((_, _, _) =>
                   {
                       var attempt = Interlocked.Increment(ref dvvAttemptCount);
                       if (attempt == 1)
                       {
                           throw new InvalidOperationException("Simulated transient storage outage.");
                       }

                       tcsRecoveredWrite.TrySetResult();
                       return Task.CompletedTask;
                   });

        storageMock.Setup(s => s.SaveClusterStateAsync(It.IsAny<string>(), It.IsAny<ClusterStateSnapshotDto>(), It.IsAny<CancellationToken>()))
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.GetJournalCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(0);

        storageMock.Setup(s => s.LoadClusterStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync((ClusterStateSnapshotDto?)null);

        storageMock.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());

        storageMock.Setup(s => s.LoadShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new ShardVersionVectorMap());

        var sp = BuildNodeWithStorage("Replica1", storageMock.Object, services =>
        {
            services.Configure<DistributedCrdtOptions>(opt =>
            {
                opt.CheckpointIntervalSeconds = 1;
                opt.AvoidBlindCheckpointWrites = true;
            });
        });

        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();

        // Act
        await checkpointService.StartAsync(CancellationToken.None);
        var completed = await Task.WhenAny(tcsRecoveredWrite.Task, Task.Delay(5000));
        await checkpointService.StopAsync(CancellationToken.None);

        // Assert - First cycle threw, cache was not prematurely updated, and second cycle succeeded
        completed.ShouldBe(tcsRecoveredWrite.Task, "Checkpoint service should survive a transient storage exception and retry on the next cycle.");
        dvvAttemptCount.ShouldBeGreaterThanOrEqualTo(2);
    }

    [IntegrationFact]
    public async Task CrdtCheckpointService_WhenShardAddedOrRemoved_DetectsMapKeyChangeWithAvoidBlindWrites()
    {
        // Arrange
        var saveDvvCount = 0;
        var tcsFirstWrite = new TaskCompletionSource();
        var tcsSecondWrite = new TaskCompletionSource();

        var storageMock = new Mock<IDistributedCrdtStorage>();
        storageMock.SetReturnsDefault(Task.CompletedTask);

        storageMock.Setup(s => s.SaveClusterStateAsync(It.IsAny<string>(), It.IsAny<ClusterStateSnapshotDto>(), It.IsAny<CancellationToken>()))
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.SaveShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<ShardVersionVectorMap>(), It.IsAny<CancellationToken>()))
                   .Callback(() =>
                   {
                       var count = Interlocked.Increment(ref saveDvvCount);
                       if (count == 1) tcsFirstWrite.TrySetResult();
                       if (count == 2) tcsSecondWrite.TrySetResult();
                   })
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.GetJournalCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(0);

        storageMock.Setup(s => s.LoadClusterStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync((ClusterStateSnapshotDto?)null);

        storageMock.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());

        storageMock.Setup(s => s.LoadShardVersionVectorMapAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new ShardVersionVectorMap());

        var sp = BuildNodeWithStorage("Replica1", storageMock.Object, services =>
        {
            services.Configure<DistributedCrdtOptions>(opt =>
            {
                opt.CheckpointIntervalSeconds = 1;
                opt.AvoidBlindCheckpointWrites = true;
            });
        });

        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();

        // Act
        await checkpointService.StartAsync(CancellationToken.None);
        await Task.WhenAny(tcsFirstWrite.Task, Task.Delay(5000));

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var versionVectorManager = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();

        // Register a brand new empty shard in the map
        versionVectorManager.EnsureShardRegistered("brand-new-shard");

        var completedSecond = await Task.WhenAny(tcsSecondWrite.Task, Task.Delay(5000));
        await checkpointService.StopAsync(CancellationToken.None);

        // Assert
        completedSecond.ShouldBe(tcsSecondWrite.Task, "Adding a new shard key to ShardVersionVectorMap should trigger a checkpoint save.");
        saveDvvCount.ShouldBeGreaterThanOrEqualTo(2);
    }

    [IntegrationFact]
    public void ClusterStateTracker_InitializeState_DoesNotNotifyObservers()
    {
        // Arrange
        var observerMock = new Mock<IClusterStateObserver>();
        var sp = BuildNode("Replica1", services =>
        {
            services.AddScoped<IClusterStateObserver>(_ => observerMock.Object);
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();

        var savedDto = new ClusterStateSnapshotDto();
        var dvv = new DottedVersionVector();
        dvv.Versions["ReplicaD"] = 42;
        savedDto.PeerStates["ReplicaD"] = new ClusterPeerStateDto
        {
            State = new ShardVersionVectorMap { { "", dvv } },
            LastSeen = DateTime.UtcNow
        };

        // Act - InitializeState during startup should not trigger topology observers before the system is ready
        tracker.InitializeState(savedDto, TimeSpan.FromMinutes(5));

        // Assert
        observerMock.Verify(o => o.OnClusterTopologyChanged(), Times.Never);
        tracker.GetClusterStates().Count.ShouldBe(1);

        // Act - Subsequent runtime updates should notify observers
        tracker.UpdatePeerState("ReplicaE", "NetworkE", new ShardVersionVectorMap { { "", dvv } });

        // Assert
        observerMock.Verify(o => o.OnClusterTopologyChanged(), Times.Once);
    }

    [IntegrationFact]
    public void ClusterStateTracker_UpdatePeerState_WhenPeerIsAlreadyTombstoned_IgnoresUpdateAndDoesNotNotifyObservers()
    {
        // Arrange
        var observerMock = new Mock<IClusterStateObserver>();
        var sp = BuildNode("Replica1", services =>
        {
            services.AddScoped<IClusterStateObserver>(_ => observerMock.Object);
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();

        tracker.TombstoneReplica("TombstonedPeer");
        observerMock.Invocations.Clear();

        var dvv = new DottedVersionVector();
        dvv.Versions["TombstonedPeer"] = 99;
        var map = new ShardVersionVectorMap { { "default", dvv } };

        // Act - Attempt to update state for a replica that is currently tombstoned
        tracker.UpdatePeerState("TombstonedPeer", "ZombieNetId", map);

        // Assert - State and network session mappings are rejected and observers are not notified
        tracker.GetClusterStates().ShouldBeEmpty();
        tracker.GetPeerIdByReplicaId("TombstonedPeer").ShouldBeNull();
        tracker.GetReplicaIdByPeerId("ZombieNetId").ShouldBeNull();
        observerMock.Verify(o => o.OnClusterTopologyChanged(), Times.Never);
    }

    [IntegrationFact]
    public void ClusterStateTracker_EvictStalePeers_WhenTtlIsZeroOrNegative_ReturnsEmptyWithoutEvicting()
    {
        // Arrange
        var sp = BuildNode("Replica1");
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();

        var dvv = new DottedVersionVector();
        dvv.Versions["RemotePeer"] = 1;
        tracker.UpdatePeerState("RemotePeer", "Net1", new ShardVersionVectorMap { { "default", dvv } });

        // Act
        var evictedZero = tracker.GetAndTombstoneExpiredPeers(TimeSpan.Zero);
        var evictedNegative = tracker.GetAndTombstoneExpiredPeers(TimeSpan.FromSeconds(-10));

        // Assert
        evictedZero.ShouldBeEmpty();
        evictedNegative.ShouldBeEmpty();
        tracker.IsReplicaTombstoned("RemotePeer").ShouldBeFalse();
        tracker.GetClusterStates().Count.ShouldBe(1);
    }
}