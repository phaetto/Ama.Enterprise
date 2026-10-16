namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using Shouldly;
using Xunit;

/// <summary>
/// Integration tests verifying the distribution profile, balancing accuracy, and topology adaptability
/// of the <see cref="ConsistentHashShardDistributionManager"/>.
/// </summary>
public sealed class ConsistentHashShardDistributionManagerIntegrationTests
{
    [Fact]
    public async Task IsShardLocalAsync_WhenShardingDisabled_ReturnsTrue()
    {
        // Arrange
        var context = SetupManager(enableSharding: false, replicationFactor: 3, nodeCount: 5);

        // Act
        var result = await context.Manager.IsShardLocalAsync("test-shard-123");

        // Assert
        result.ShouldBeTrue();
    }

    [Fact]
    public async Task GetTargetNodesForShardAsync_WhenShardingDisabled_ReturnsAllActiveReplicas()
    {
        // Arrange
        var context = SetupManager(enableSharding: false, replicationFactor: 3, nodeCount: 5);

        // Act
        var targets = await context.Manager.GetTargetNodesForShardAsync("test-shard-123");

        // Assert
        targets.Count.ShouldBe(5);
        targets.ShouldBe(context.ReplicaIds.OrderBy(x => x, StringComparer.Ordinal), ignoreOrder: true);
    }

    [Fact]
    public async Task GetTargetNodesForShardAsync_RespectsReplicationFactor()
    {
        // Arrange
        int replicationFactor = 2;
        var context = SetupManager(enableSharding: true, replicationFactor: replicationFactor, nodeCount: 5);

        // Act
        var targets = await context.Manager.GetTargetNodesForShardAsync("test-shard-456");

        // Assert
        targets.Count.ShouldBe(replicationFactor);
        targets.Distinct().Count().ShouldBe(replicationFactor);
    }

    [Fact]
    public async Task GetTargetNodesForShardAsync_WithEnableSharding_GeneratesBalancedDistribution()
    {
        // Arrange
        int replicationFactor = 3;
        int totalShards = 10000;
        int nodeCount = 5;
        
        var context = SetupManager(
            enableSharding: true,
            replicationFactor: replicationFactor,
            nodeCount: nodeCount);

        var allocationCounts = new Dictionary<string, int>(StringComparer.Ordinal);

        // Act
        for (int i = 0; i < totalShards; i++)
        {
            var targets = await context.Manager.GetTargetNodesForShardAsync($"shard-{i}");
            foreach (var target in targets)
            {
                if (!allocationCounts.ContainsKey(target))
                {
                    allocationCounts[target] = 0;
                }
                allocationCounts[target]++;
            }
        }

        // Assert
        // Total assignments should be 30,000. For 5 nodes, perfect balance is 6,000 per node.
        // We'll allow a generous 25% deviation (4,500 to 7,500) to account for natural hashing distribution variance.
        int expectedPerNode = (totalShards * replicationFactor) / nodeCount;
        int lowerBound = (int)(expectedPerNode * 0.75);
        int upperBound = (int)(expectedPerNode * 1.25);

        allocationCounts.Count.ShouldBe(nodeCount);
        foreach (var kvp in allocationCounts)
        {
            kvp.Value.ShouldBeInRange(lowerBound, upperBound);
        }
    }

    [Fact]
    public async Task GetTargetNodesForShardAsync_WhenNodeRemoved_CausesMinimalReassignments()
    {
        // Arrange
        int replicationFactor = 3;
        int totalShards = 1000;
        var context = SetupManager(enableSharding: true, replicationFactor: replicationFactor, nodeCount: 5);
        
        var initialAssignments = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        for (int i = 0; i < totalShards; i++)
        {
            var shardId = $"shard-{i}";
            initialAssignments[shardId] = await context.Manager.GetTargetNodesForShardAsync(shardId);
        }

        // Act - Simulate a node crash/eviction by removing the last remote peer
        var removedReplicaId = context.ReplicaIds.Last();
        context.ActivePeers.RemoveAt(context.ActivePeers.Count - 1);
        
        int changedAssignments = 0;

        for (int i = 0; i < totalShards; i++)
        {
            var shardId = $"shard-{i}";
            var oldTargets = initialAssignments[shardId];
            var newTargets = await context.Manager.GetTargetNodesForShardAsync(shardId);
            
            bool wasOnRemovedNode = oldTargets.Contains(removedReplicaId);
            
            // For assignments that did NOT include the removed node natively, 
            // the consistent hash ring should guarantee their mapping remains 100% identical.
            if (!wasOnRemovedNode)
            {
                newTargets.ShouldBe(oldTargets);
            }
            else
            {
                changedAssignments++;
                newTargets.ShouldNotContain(removedReplicaId);
                newTargets.Count.ShouldBe(replicationFactor); // Ring should resolve to the next available replica securely
            }
        }
        
        // Assert - Ensure only a proportional fraction of shards required remapping
        changedAssignments.ShouldBeGreaterThan(0);
        changedAssignments.ShouldBeLessThan(totalShards);
    }
    
    [Fact]
    public async Task GetTargetNodesForShardAsync_WithNullOrEmptyShard_ReturnsEmpty()
    {
        // Arrange
        var context = SetupManager(enableSharding: true, replicationFactor: 3, nodeCount: 5);

        // Act
        var targetsNull = await context.Manager.GetTargetNodesForShardAsync(null!);
        var targetsEmpty = await context.Manager.GetTargetNodesForShardAsync(string.Empty);
        var targetsWhitespace = await context.Manager.GetTargetNodesForShardAsync("   ");

        // Assert
        targetsNull.ShouldBeEmpty();
        targetsEmpty.ShouldBeEmpty();
        targetsWhitespace.ShouldBeEmpty();
    }
    
    [Fact]
    public async Task IsShardLocalAsync_WithNullOrEmptyShard_ReturnsFalse()
    {
        // Arrange
        var context = SetupManager(enableSharding: true, replicationFactor: 3, nodeCount: 5);

        // Act
        var targetsNull = await context.Manager.IsShardLocalAsync(null!);
        var targetsEmpty = await context.Manager.IsShardLocalAsync(string.Empty);
        var targetsWhitespace = await context.Manager.IsShardLocalAsync("   ");

        // Assert
        targetsNull.ShouldBeFalse();
        targetsEmpty.ShouldBeFalse();
        targetsWhitespace.ShouldBeFalse();
    }

    [Fact]
    public async Task SystemRegistryShard_WhenShardingEnabled_AlwaysReturnsLocalAndTargetsAllReplicas()
    {
        // Arrange - Use replicationFactor = 1 so normal shards only map to 1 node out of 5
        var context = SetupManager(enableSharding: true, replicationFactor: 1, nodeCount: 5);

        // Act
        var isLocal = await context.Manager.IsShardLocalAsync("system-document-registry");
        var targets = await context.Manager.GetTargetNodesForShardAsync("system-document-registry");

        // Assert - System registry shard must bypass the consistent hash ring and replicate globally
        isLocal.ShouldBeTrue();
        targets.Count.ShouldBe(5);
        targets.ShouldBe(context.ReplicaIds, ignoreOrder: true);
    }

    [Fact]
    public async Task GetTargetNodesForShardAsync_WhenReplicationFactorExceedsClusterSize_ReturnsAllUniqueReplicasWithoutDuplicates()
    {
        // Arrange - ReplicationFactor (10) > NodeCount (3)
        var context = SetupManager(enableSharding: true, replicationFactor: 10, nodeCount: 3);

        // Act
        var targets = await context.Manager.GetTargetNodesForShardAsync("shard-oversubscribed");

        // Assert
        targets.Count.ShouldBe(3);
        targets.Distinct(StringComparer.Ordinal).Count().ShouldBe(3);
        targets.ShouldBe(context.ReplicaIds, ignoreOrder: true);
    }

    [Fact]
    public async Task GetTargetNodesForShardAsync_WhenActivePeerHasNoReplicaIdMappingYet_ExcludesUnmappedPeerUntilMapped()
    {
        // Arrange
        var context = SetupManager(enableSharding: true, replicationFactor: 5, nodeCount: 2);

        // Add a newly connected network peer whose CRDT ReplicaId has not yet been mapped in ClusterTracker
        var unmappedPeerId = new PeerId(Guid.NewGuid());
        context.ActivePeers.Add(new PeerNode { Id = unmappedPeerId });

        // Act 1 - Query before ClusterTracker knows the peer's ReplicaId
        var targetsBeforeMapping = await context.Manager.GetTargetNodesForShardAsync("shard-unmapped-test");

        // Assert 1 - Only the 2 mapped replicas are returned
        targetsBeforeMapping.Count.ShouldBe(2);
        targetsBeforeMapping.ShouldBe(context.ReplicaIds, ignoreOrder: true);

        // Act 2 - Map the peer in ClusterTracker and query again
        context.PeerToReplicaMap[unmappedPeerId.Value.ToString()] = "replica-newly-mapped";
        var targetsAfterMapping = await context.Manager.GetTargetNodesForShardAsync("shard-unmapped-test");

        // Assert 2 - The ring rebuilds and now includes the newly mapped replica
        targetsAfterMapping.Count.ShouldBe(3);
        targetsAfterMapping.ShouldContain("replica-newly-mapped");
    }

    [Fact]
    public async Task IsShardLocalAsync_WhenClusterScalesFromSingleNodeToMultiNode_DemotesReassignedShardsWhileKeepingRegistryLocal()
    {
        // Arrange - Start as a single-node cluster with replicationFactor = 1
        var context = SetupManager(enableSharding: true, replicationFactor: 1, nodeCount: 1);

        const int TotalShards = 100;
        for (int i = 0; i < TotalShards; i++)
        {
            (await context.Manager.IsShardLocalAsync($"scale-shard-{i}")).ShouldBeTrue();
        }

        // Act - 4 new replicas join the cluster
        for (int i = 1; i <= 4; i++)
        {
            var newPeerId = new PeerId(Guid.NewGuid());
            context.ActivePeers.Add(new PeerNode { Id = newPeerId });
            context.PeerToReplicaMap[newPeerId.Value.ToString()] = $"replica-{i}";
        }

        int localCountAfterScaleOut = 0;
        for (int i = 0; i < TotalShards; i++)
        {
            if (await context.Manager.IsShardLocalAsync($"scale-shard-{i}"))
            {
                localCountAfterScaleOut++;
            }
        }

        // Assert - Only a subset of data shards remain local on replica-0, while system-document-registry stays local
        localCountAfterScaleOut.ShouldBeGreaterThan(0);
        localCountAfterScaleOut.ShouldBeLessThan(TotalShards);
        (await context.Manager.IsShardLocalAsync("system-document-registry")).ShouldBeTrue();
    }

    private static ManagerTestContext SetupManager(bool enableSharding, int replicationFactor, int nodeCount)
    {
        var localReplicaId = "replica-0";
        var meshId = "default-mesh";
        
        var activePeers = new List<PeerNode>();
        var peerToReplicaMap = new Dictionary<string, string>(StringComparer.Ordinal);
        var replicaIds = new List<string> { localReplicaId };

        for (int i = 1; i < nodeCount; i++)
        {
            var peerId = new PeerId(Guid.NewGuid());
            var replicaId = $"replica-{i}";
            
            // Bypass strict struct constructor to leave Endpoint null; the Manager exclusively evaluates Id string mappings safely.
            activePeers.Add(new PeerNode { Id = peerId }); 
            peerToReplicaMap[peerId.Value.ToString()] = replicaId;
            replicaIds.Add(replicaId);
        }

        var options = Options.Create(new DistributedCrdtOptions
        {
            EnableSharding = enableSharding,
            ShardReplicationFactor = replicationFactor,
            VirtualNodesPerReplica = 100
        });

        var clusterTrackerMock = new Mock<IClusterStateTracker>();
        clusterTrackerMock.Setup(x => x.GetReplicaIdByPeerId(It.IsAny<string>()))
            .Returns((string id) => peerToReplicaMap.TryGetValue(id, out var rId) ? rId : null!);

        var peerRegistryMock = new Mock<IPeerRegistry>();
        peerRegistryMock.Setup(x => x.GetPeersByStatusAsync(meshId, PeerStatus.Active, It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => activePeers.ToList()); // Return dynamic copy simulating active discovery changes

        var serviceCollection = new ServiceCollection();
        serviceCollection.AddSingleton(peerRegistryMock.Object);
        var serviceProvider = serviceCollection.BuildServiceProvider();

        var replicaRegistration = new DistributedCrdtReplicaRegistration(localReplicaId);
        var p2pRegistrations = new[] { new DistributedCrdtP2pRegistration(meshId, localReplicaId) };

        var manager = new ConsistentHashShardDistributionManager(
            clusterTrackerMock.Object,
            replicaRegistration,
            serviceProvider,
            p2pRegistrations,
            options);

        return new ManagerTestContext(manager, activePeers, replicaIds, peerToReplicaMap, peerRegistryMock);
    }
    
    private sealed record ManagerTestContext(
        ConsistentHashShardDistributionManager Manager,
        List<PeerNode> ActivePeers,
        List<string> ReplicaIds,
        Dictionary<string, string> PeerToReplicaMap,
        Mock<IPeerRegistry> PeerRegistryMock);
}