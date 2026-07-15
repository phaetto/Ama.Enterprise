namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
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
    private IServiceProvider BuildNode(string replicaId, IDistributedCrdtStorage customStorage)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            // Accelerated for testing boundaries
            opt.CheckpointIntervalSeconds = 1;
            opt.ActiveSyncEnabled = true;
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
}