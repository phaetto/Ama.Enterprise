namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using Ama.CRDT.Attributes;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Extensions;
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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

[CrdtAotType(typeof(ScopeTopologyProviderIntegrationTests.TopologyTestState))]
[CrdtAotType(typeof(Dictionary<string, string>))]
public sealed partial class TopologyTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(ScopeTopologyProviderIntegrationTests.TopologyTestState))]
[JsonSerializable(typeof(CrdtDocument<ScopeTopologyProviderIntegrationTests.TopologyTestState>))]
public sealed partial class TopologyTestJsonContext : JsonSerializerContext
{
}

public sealed class ScopeTopologyProviderIntegrationTests
{
    private const string TestMeshId = "TestMesh";

    public sealed class TopologyTestState
    {
        public string Id { get; set; } = "topo-doc";
        public Dictionary<string, string> DataMap { get; set; } = new(StringComparer.Ordinal);
    }

    public sealed class TestTopologyProvider : IScopeTopologyProvider
    {
        public HashSet<string> ExpectedPeers { get; } = new(StringComparer.OrdinalIgnoreCase);

        public ValueTask<bool> IsPeerExpectedAsync(string peerNetworkId, CancellationToken cancellationToken = default)
        {
            return new ValueTask<bool>(ExpectedPeers.Contains(peerNetworkId));
        }
    }

    private IServiceProvider BuildNode(string replicaId, TestTopologyProvider topologyProvider, Action<IServiceCollection>? configureExtra = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            opt.CheckpointIntervalSeconds = 1; // Fast for testing
            opt.MaintenanceIntervalSeconds = 1; // Fast for testing
            opt.AntiEntropyIntervalSeconds = 15;
            opt.ActiveSyncEnabled = true;
        });

        services.AddDistributedCrdtReplica(replicaId);

        services.AddCrdt()
                .AddCrdtAotContext(new TopologyTestAotContext())
                .AddCrdtJsonTypeInfoResolver(TopologyTestJsonContext.Default);

        services.AddDistributedDocumentType<TopologyTestState>("topo-doc");
        services.AddDistributedCrdtP2p(TestMeshId, replicaId);
        
        services.Replace(ServiceDescriptor.Scoped<IScopeTopologyProvider>(sp => topologyProvider));

        // Mock P2P Outbound
        services.AddSingleton(Mock.Of<IP2pAlgorithm>());
        services.AddSingleton(Mock.Of<IDirectMessageSender>());
        
        // Mock and construct explicit internal registries resolving localized targeted multi-mesh orchestrations
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        services.AddSingleton(new P2pMeshMetadata(TestMeshId));

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
    }

    [IntegrationFact]
    public async Task Orchestrator_ProvideSnapshotAsync_ShouldRejectUnexpectedPeer()
    {
        // Arrange
        var topologyProvider = new TestTopologyProvider();
        var unexpectedPeerGuid = Guid.NewGuid();
        var expectedPeerGuid = Guid.NewGuid();
        
        topologyProvider.ExpectedPeers.Add(expectedPeerGuid.ToString()); // Only expected peer is allowed

        var mockSender = new Mock<IDirectMessageSender>();
        var sp = BuildNode("Replica1", topologyProvider, services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("topo-doc", "topo-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        // Act - Request snapshot for unexpected peer
        await orchestrator.ProvideSnapshotAsync("topo-doc", "RemoteReplica", new PeerId(unexpectedPeerGuid), CancellationToken.None);

        // Assert - The orchestrator must drop the request preventing state leaks
        mockSender.Verify(p => p.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Never());

        // Act - Request snapshot for expected peer
        await orchestrator.ProvideSnapshotAsync("topo-doc", "RemoteReplica", new PeerId(expectedPeerGuid), CancellationToken.None);

        // Assert - Expected peer receives the payload
        mockSender.Verify(p => p.SendDirectAsync(It.Is<PeerId>(id => id.Value == expectedPeerGuid), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once());
    }

    [IntegrationFact]
    public async Task Orchestrator_DispatchAntiEntropyStateAsync_OnlyTargetsExpectedPeers()
    {
        // Arrange
        var topologyProvider = new TestTopologyProvider();
        var unexpectedPeerGuid = Guid.NewGuid();
        var expectedPeerGuid = Guid.NewGuid();
        
        topologyProvider.ExpectedPeers.Add(expectedPeerGuid.ToString());

        var mockSender = new Mock<IDirectMessageSender>();
        var sp = BuildNode("Replica1", topologyProvider, services =>
        {
            services.Replace(ServiceDescriptor.Singleton(mockSender.Object));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Populate the P2P registry with both expected and unexpected peers
        var peerRegistry = sp.GetRequiredService<IPeerRegistry>();
        
        // Use default PeerNode mappings
        var expectedNode = new PeerNode(new PeerId(expectedPeerGuid), new TcpPeerEndpoint("nice!", 69));
        var unexpectedNode = new PeerNode(new PeerId(unexpectedPeerGuid), new TcpPeerEndpoint("nice!", 69));

        await peerRegistry.AddOrUpdatePeerAsync(TestMeshId, expectedNode, PeerStatus.Active, CancellationToken.None);
        await peerRegistry.AddOrUpdatePeerAsync(TestMeshId, unexpectedNode, PeerStatus.Active, CancellationToken.None);

        // Act
        await orchestrator.DispatchAntiEntropyStateAsync(CancellationToken.None);

        // Assert - Ensure only the expected peer was selected as the target for the point-to-point generic state sync
        mockSender.Verify(p => p.SendDirectAsync(It.Is<PeerId>(id => id.Value == expectedPeerGuid), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once());
        mockSender.Verify(p => p.SendDirectAsync(It.Is<PeerId>(id => id.Value == unexpectedPeerGuid), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Never());
    }

    [IntegrationFact]
    public async Task CrdtMaintenanceService_ShouldCalculateGMVV_ExcludingUnexpectedPeers()
    {
        // Arrange
        var topologyProvider = new TestTopologyProvider();
        var expectedPeerGuid = Guid.NewGuid();
        var unexpectedPeerGuid = Guid.NewGuid();
        
        topologyProvider.ExpectedPeers.Add(expectedPeerGuid.ToString());

        var sharedStorage = new MemoryCrdtStorage();

        var sp = BuildNode("Replica1", topologyProvider, services =>
        {
            services.Replace(ServiceDescriptor.Scoped<IDistributedCrdtStorage>(s => sharedStorage));
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope("Replica1");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher = scope.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        var clusterTracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();

        await orchestrator.InitializeAsync(CancellationToken.None);
        await orchestrator.CreateDocumentAsync("topo-doc", "topo-doc", CancellationToken.None);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None);

        var doc = orchestrator.GetDocument<TopologyTestState>("topo-doc")!;

        // Generate 5 operations locally, bumping the local DVV for Replica1 appropriately
        for (int i = 0; i < 5; i++)
        {
            var intent = new MapSetIntent($"key{i}", $"value{i}");
            var op = await patcher.GenerateOperationAsync(doc.Document, x => x.DataMap, intent, CancellationToken.None);
            await doc.ApplyPatchAsync(new CrdtPatch(new[] { op }), CancellationToken.None);
        }

        var opsBeforeCheckpoint = await sharedStorage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        opsBeforeCheckpoint.Count.ShouldBeGreaterThanOrEqualTo(5); // Including registry ops

        // Extract the overarching global clock identifying absolute boundaries.
        var context = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
        long currentClock = 0;
        lock (context.GlobalVersionVector)
        {
            currentClock = context.GlobalVersionVector.Versions["Replica1"];
        }

        // The expected peer has fully synced and matches our local state preventing amnesia limits.
        var expectedDvv = new DottedVersionVector();
        expectedDvv.Versions["Replica1"] = currentClock;
        clusterTracker.UpdatePeerState("ReplicaB", expectedPeerGuid.ToString(), expectedDvv);

        // The unexpected peer has NOT synced and is entirely lagging (Clock 0)
        var unexpectedDvv = new DottedVersionVector();
        clusterTracker.UpdatePeerState("ReplicaC", unexpectedPeerGuid.ToString(), unexpectedDvv);

        var maintenanceService = sp.GetServices<IHostedService>().OfType<CrdtMaintenanceService>().First();

        // Act - Trigger maintenance cycle evaluating GMVV math bounds
        await maintenanceService.StartAsync(CancellationToken.None);
        await Task.Delay(1500); // Wait for the 1-second interval
        await maintenanceService.StopAsync(CancellationToken.None);

        // Assert - If GMVV included ReplicaC, GMVV would be 0 and no operations would trim.
        // Because the TopologyProvider explicitly limits the causal matrix to expected peers (ReplicaB and Local),
        // the GMVV evaluates trimming the journal matching limits.
        var opsAfterCheckpoint = await sharedStorage.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        
        opsAfterCheckpoint.Count.ShouldBeLessThan(opsBeforeCheckpoint.Count);
        
        // Ensure the data operations were collected
        opsAfterCheckpoint.Any(o => o.DocumentId == "topo-doc").ShouldBeFalse();
    }
}