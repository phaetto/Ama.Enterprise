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
    private IServiceProvider BuildNode(string replicaId, IDistributedCrdtStorage customStorage, Action<DistributedCrdtOptions>? configureOptions = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            // Accelerated for testing boundaries
            opt.CheckpointIntervalSeconds = 1;
            opt.ActiveSyncEnabled = true;
            
            configureOptions?.Invoke(opt);
        });

        services.AddDistributedCrdtReplica(replicaId);

        services.AddCrdt()
                .AddCrdtAotContext(new DistributedCrdtSystemAotContext())
                .AddCrdtJsonTypeInfoResolver(DistributedCrdtSystemJsonContext.Default);

        // Map the mocked tracking storage explicitly intercepting bounds
        services.Replace(ServiceDescriptor.Singleton<IDistributedCrdtStorage>(customStorage));
        services.AddSingleton(Mock.Of<IP2pAlgorithm>());

        return services.BuildServiceProvider();
    }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
    private async IAsyncEnumerable<JournaledOperation> EmptyJournalStream()
    {
        yield break;
    }
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

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

        // Mocks required by CrdtInitializationService
        storageMock.Setup(s => s.LoadClusterStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync((ClusterStateSnapshotDto?)null);

        storageMock.Setup(s => s.GetAllJournaledOperationsAsync(It.IsAny<CancellationToken>()))
                   .Returns(EmptyJournalStream());

        storageMock.Setup(s => s.LoadGlobalVersionVectorAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new DottedVersionVector());

        var sp = BuildNode("Replica1", storageMock.Object);

        // Start initialization service to ensure ReplicaContext bounds are established preventing ArgumentNullExceptions natively
        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var tracker = scope.ClusterTracker;

        // Populate the in-memory tracker with mocked connected topologies simulating network activities natively
        var dvv = new DottedVersionVector();
        dvv.Versions["ReplicaB"] = 15;
        
        tracker.UpdatePeerState("ReplicaB", "NetworkB", dvv);
        tracker.TombstoneReplica("ReplicaC");

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();

        // Act
        await checkpointService.StartAsync(CancellationToken.None);

        // Wait for the periodic background loop to execute explicitly exporting tracking limits cleanly.
        // TaskCompletionSource avoids flaky Thread.Sleep or Task.Delay constraints natively safely resolving when matched.
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

        var sp = BuildNode("Replica1", storageMock.Object);
        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();

        // Act
        await initService.StartAsync(CancellationToken.None);

        // Assert
        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var tracker = scope.ClusterTracker;

        // The tracker must now securely recognize the resurrected state preventing split-brain topologies inherently
        tracker.IsReplicaTombstoned("ReplicaZOMBIE").ShouldBeTrue();
        
        var states = tracker.GetClusterStates();
        states.Count.ShouldBe(1);
        states[0].Versions.ShouldContainKeyAndValue("ReplicaD", 42);
        
        // Ensure that querying the tracker natively reflects the loaded bounds mapped across
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
                   .Callback(() => 
                   {
                       Interlocked.Increment(ref saveClusterCount);
                   })
                   .Returns(Task.CompletedTask);

        storageMock.Setup(s => s.SaveGlobalVersionVectorAsync(It.IsAny<string>(), It.IsAny<DottedVersionVector>(), It.IsAny<CancellationToken>()))
                   .Callback(() => 
                   {
                       Interlocked.Increment(ref saveDvvCount);
                       tcsFirstWrite.TrySetResult(); // Trigger signal explicitly on first cache execution natively
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

        var sp = BuildNode("Replica1", storageMock.Object, opt => 
        {
            opt.CheckpointIntervalSeconds = 1;
            opt.AvoidBlindCheckpointWrites = true; // Key configuration strictly tracked
        });

        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();

        // Act
        await checkpointService.StartAsync(CancellationToken.None);

        // Wait for the first cycle to definitely execute and populate the isolated cache natively
        await Task.WhenAny(tcsFirstWrite.Task, Task.Delay(5000));
        
        // Now delay enough for at least 2 more background cycles to trigger naturally ensuring bounds don't leak
        await Task.Delay(2500);

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
                   .Callback(() => 
                   {
                       Interlocked.Increment(ref saveDvvCount);
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

        var sp = BuildNode("Replica1", storageMock.Object, opt => 
        {
            opt.CheckpointIntervalSeconds = 1;
            opt.AvoidBlindCheckpointWrites = true;
        });

        var initService = sp.GetServices<IHostedService>().OfType<CrdtInitializationService>().First();
        await initService.StartAsync(CancellationToken.None);

        var checkpointService = sp.GetServices<IHostedService>().OfType<CrdtCheckpointService>().First();

        // Act - Start background loops safely
        await checkpointService.StartAsync(CancellationToken.None);

        // Wait for the first cycle explicit extraction
        await Task.WhenAny(tcsFirstWrite.Task, Task.Delay(5000));

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        
        // Mutate the local explicit bound dynamically
        scope.ClusterTracker.TombstoneReplica("ReplicaMutated");
        
        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        lock (context.GlobalVersionVector)
        {
            context.GlobalVersionVector.Versions["ReplicaMutated"] = 10;
        }

        // Wait for the mutation to inherently trigger a subsequent bounded storage write explicitly overriding skip rules
        await Task.WhenAny(tcsSecondWrite.Task, Task.Delay(5000));

        await checkpointService.StopAsync(CancellationToken.None);

        // Assert
        saveClusterCount.ShouldBeGreaterThanOrEqualTo(2, "The mutation should have bypassed the blind write rule triggering a persist.");
        saveDvvCount.ShouldBeGreaterThanOrEqualTo(2, "The DVV mutation should have bypassed the blind write rule triggering a persist.");
    }
}