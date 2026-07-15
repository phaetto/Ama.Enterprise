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

        // Act - Track a new peer
        tracker.UpdatePeerState("RemoteReplica1", "NetworkId1", remoteDvv);

        // Assert
        var states = tracker.GetClusterStates();
        states.Count.ShouldBe(1);
        states[0].Versions["RemoteReplica1"].ShouldBe(10);
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
        var sp = BuildNodeWithStorage("Replica1", mockStorage.Object);

        var savedDvv = new DottedVersionVector();
        savedDvv.Versions["Replica1"] = 100;
        
        mockStorage.Setup(s => s.LoadGlobalVersionVectorAsync("Replica1", It.IsAny<CancellationToken>()))
                   .ReturnsAsync(savedDvv);
        
        mockStorage.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());

        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();

        // Act
        await initService.StartAsync(CancellationToken.None);

        // Assert
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();

        context.GlobalVersionVector.Versions["Replica1"].ShouldBe(100);
    }

    [IntegrationFact]
    public async Task CrdtCheckpointService_ShouldExportAndPersistClusterState_OnInterval()
    {
        // Arrange
        var storageMock = new Mock<IDistributedCrdtStorage>();
        var tcs = new TaskCompletionSource<ClusterStateSnapshotDto>();

        storageMock.Setup(s => s.SaveClusterStateAsync(It.IsAny<string>(), It.IsAny<ClusterStateSnapshotDto>(), It.IsAny<CancellationToken>()))
                   .Callback<string, ClusterStateSnapshotDto, CancellationToken>((_, state, _) => tcs.TrySetResult(state))
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.SaveGlobalVersionVectorAsync(It.IsAny<string>(), It.IsAny<DottedVersionVector>(), It.IsAny<CancellationToken>()))
                   .Returns(Task.CompletedTask);
                   
        storageMock.Setup(s => s.GetJournalCountAsync(It.IsAny<CancellationToken>()))
                   .ReturnsAsync(0);

        storageMock.Setup(s => s.LoadClusterStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync((ClusterStateSnapshotDto?)null);

        storageMock.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());

        storageMock.Setup(s => s.LoadGlobalVersionVectorAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new DottedVersionVector());

        var sp = BuildNodeWithStorage("Replica1", storageMock.Object);

        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var tracker = scope.ClusterTracker;

        var dvv = new DottedVersionVector();
        dvv.Versions["ReplicaB"] = 15;
        tracker.UpdatePeerState("ReplicaB", "NetworkB", dvv);
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
        capturedState.NetworkIdToReplicaId.ShouldContainKeyAndValue("NetworkB", "ReplicaB");
        capturedState.PeerStates.ShouldContainKey("ReplicaB");
        capturedState.PeerStates["ReplicaB"].State.Versions["ReplicaB"].ShouldBe(15);
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
            State = dvv, 
            LastSeen = DateTime.UtcNow 
        };

        var storageMock = new Mock<IDistributedCrdtStorage>();
        
        storageMock.Setup(s => s.LoadClusterStateAsync("Replica1", It.IsAny<CancellationToken>()))
                   .ReturnsAsync(savedDto);

        storageMock.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());
                   
        storageMock.Setup(s => s.LoadGlobalVersionVectorAsync("Replica1", It.IsAny<CancellationToken>()))
                   .ReturnsAsync((DottedVersionVector?)null);

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
        states[0].Versions.ShouldContainKeyAndValue("ReplicaD", 42);
        
        var tombstonedPeer = tracker.TombstonePeerByNetworkId("NetworkD");
        tombstonedPeer.ShouldBe("ReplicaD");
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

        storageMock.Setup(s => s.SaveClusterStateAsync(It.IsAny<string>(), It.IsAny<ClusterStateSnapshotDto>(), It.IsAny<CancellationToken>()))
                   .Callback(() => Interlocked.Increment(ref saveClusterCount))
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.SaveGlobalVersionVectorAsync(It.IsAny<string>(), It.IsAny<DottedVersionVector>(), It.IsAny<CancellationToken>()))
                   .Callback(() => 
                   {
                       Interlocked.Increment(ref saveDvvCount);
                       tcsFirstWrite.TrySetResult();
                   })
                   .Returns(Task.CompletedTask);
                   
        storageMock.Setup(s => s.GetJournalCountAsync(It.IsAny<CancellationToken>()))
                   .ReturnsAsync(0);

        storageMock.Setup(s => s.LoadClusterStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync((ClusterStateSnapshotDto?)null);

        storageMock.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());

        storageMock.Setup(s => s.LoadGlobalVersionVectorAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new DottedVersionVector());

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

        storageMock.Setup(s => s.SaveClusterStateAsync(It.IsAny<string>(), It.IsAny<ClusterStateSnapshotDto>(), It.IsAny<CancellationToken>()))
                   .Callback(() => 
                   {
                       var count = Interlocked.Increment(ref saveClusterCount);
                       if (count == 1) tcsFirstWrite.TrySetResult();
                       if (count == 2) tcsSecondWrite.TrySetResult();
                   })
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.SaveGlobalVersionVectorAsync(It.IsAny<string>(), It.IsAny<DottedVersionVector>(), It.IsAny<CancellationToken>()))
                   .Callback(() => Interlocked.Increment(ref saveDvvCount))
                   .Returns(Task.CompletedTask);
                   
        storageMock.Setup(s => s.GetJournalCountAsync(It.IsAny<CancellationToken>()))
                   .ReturnsAsync(0);

        storageMock.Setup(s => s.LoadClusterStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync((ClusterStateSnapshotDto?)null);

        storageMock.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());

        storageMock.Setup(s => s.LoadGlobalVersionVectorAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new DottedVersionVector());

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
        lock (context.GlobalVersionVector)
        {
            context.GlobalVersionVector.Versions["ReplicaMutated"] = 10;
        }

        await Task.WhenAny(tcsSecondWrite.Task, Task.Delay(5000));
        await checkpointService.StopAsync(CancellationToken.None);

        // Assert
        saveClusterCount.ShouldBeGreaterThanOrEqualTo(2, "The mutation should have bypassed the blind write rule triggering a persist.");
        saveDvvCount.ShouldBeGreaterThanOrEqualTo(2, "The DVV mutation should have bypassed the blind write rule triggering a persist.");
    }
}