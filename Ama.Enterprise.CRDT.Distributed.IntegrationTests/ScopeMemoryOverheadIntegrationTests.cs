namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Attributes;
using Ama.CRDT.Attributes.Strategies;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services.Providers;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.P2p;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Shouldly;
using Xunit;

[CrdtAotType(typeof(ScopeMemoryOverheadIntegrationTests.DistributedMemoryTestModel))]
[CrdtAotType(typeof(List<string>))]
[CrdtAotType(typeof(Dictionary<string, int>))]
public sealed partial class ScopeMemoryOverheadAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(ScopeMemoryOverheadIntegrationTests.DistributedMemoryTestModel))]
[JsonSerializable(typeof(CrdtDocument<ScopeMemoryOverheadIntegrationTests.DistributedMemoryTestModel>))]
public sealed partial class ScopeMemoryOverheadJsonContext : JsonSerializerContext
{
}

[Collection("ScopeMemoryOverheadSequential")]
public sealed class ScopeMemoryOverheadIntegrationTests
{
    public sealed class DistributedMemoryTestModel
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = "Sample Distributed Document";

        [CrdtCounterStrategy]
        public int Count { get; set; }

        [CrdtOrSetStrategy]
        public List<string> Tags { get; set; } = new();

        [CrdtLwwMapStrategy]
        public Dictionary<string, int> Scores { get; set; } = new(StringComparer.Ordinal);
    }

    private readonly ITestOutputHelper output;

    public ScopeMemoryOverheadIntegrationTests(ITestOutputHelper output)
    {
        this.output = output ?? throw new ArgumentNullException(nameof(output));
    }

    [IntegrationFact]
    public async Task MeasureDistributedScopeOverhead_AcrossLifecycleAndPipelineStages()
    {
        using var sp = BuildNode();
        var scopeFactory = sp.GetRequiredService<IDistributedCrdtScopeFactory>();

        // Warmup JIT, string pools, and MS.DI callsite expression compilation for every service resolved in all stages
        for (int i = 0; i < 5; i++)
        {
            using var warmupScope = scopeFactory.CreateScope("warmup-scope", "HostReplica");
            var warmupProvider = warmupScope.ServiceProvider;
            _ = warmupProvider.GetRequiredService<IDocumentCommandPipeline>();
            _ = warmupProvider.GetRequiredService<IShardVersionVectorManager>();
            _ = warmupProvider.GetRequiredService<IShardDistributionManager>();
            _ = warmupProvider.GetRequiredService<ICrdtRemoteProxyClient>();
            _ = warmupProvider.GetRequiredService<ICrdtMapReduceCoordinator>();
            _ = warmupProvider.GetRequiredService<ICrdtAntiEntropyCoordinator>();
            _ = warmupProvider.GetRequiredService<ICrdtRpcPayloadProcessor>();
            _ = warmupProvider.GetRequiredService<ICrdtStateSyncPayloadProcessor>();
            await warmupScope.Orchestrator.InitializeAsync(CancellationToken.None);
            await warmupScope.Orchestrator.CreateDocumentAsync("warmup-doc", "memory-test-doc", CancellationToken.None);
        }

        // Wait for background MS.DI ExpressionResolverBuilder compilation to finish
        await Task.Delay(50);

        // 1. Bare uninitialized scope (constructor resolves Orchestrator, ClusterTracker, Storage, EvictionService)
        long bareUninitializedBytes = MeasureAllocatedBytes(() =>
        {
            using var scope = scopeFactory.CreateScope("bench-bare-scope", "HostReplica");
            return scope.ReplicaId;
        }, iterations: 25);

        // 2. Scope with full DI graph resolved (CommandPipeline, DvvManager, DistributionManager, Coordinators, Processors)
        long fullDiResolvedBytes = MeasureAllocatedBytes(() =>
        {
            using var scope = scopeFactory.CreateScope("bench-di-scope", "HostReplica");
            var provider = scope.ServiceProvider;
            _ = scope.Orchestrator;
            _ = scope.ClusterTracker;
            _ = scope.Storage;
            _ = scope.EvictionService;
            _ = provider.GetRequiredService<IDocumentCommandPipeline>();
            _ = provider.GetRequiredService<IShardVersionVectorManager>();
            _ = provider.GetRequiredService<IShardDistributionManager>();
            _ = provider.GetRequiredService<ICrdtRemoteProxyClient>();
            _ = provider.GetRequiredService<ICrdtMapReduceCoordinator>();
            _ = provider.GetRequiredService<ICrdtAntiEntropyCoordinator>();
            _ = provider.GetRequiredService<ICrdtRpcPayloadProcessor>();
            _ = provider.GetRequiredService<ICrdtStateSyncPayloadProcessor>();
            return scope.ReplicaId;
        }, iterations: 25);

        // 3. Initialized scope (includes system-document-registry CRDT document initialization)
        long initializedScopeBytes = await MeasureAllocatedBytesAsync(async () =>
        {
            using var scope = scopeFactory.CreateScope("bench-init-scope", "HostReplica");
            await scope.Orchestrator.InitializeAsync(CancellationToken.None);
            return scope.Orchestrator.InitializationStage;
        }, iterations: 20);

        // 4. Initialized scope + 1 active user document created
        long singleDocScopeBytes = await MeasureAllocatedBytesAsync(async () =>
        {
            using var scope = scopeFactory.CreateScope("bench-1doc-scope", "HostReplica");
            await scope.Orchestrator.InitializeAsync(CancellationToken.None);
            await scope.Orchestrator.CreateDocumentAsync("doc-1", "memory-test-doc", CancellationToken.None);
            return scope.Orchestrator.GetActiveDocuments().Count;
        }, iterations: 20);

        output.WriteLine("=== Distributed CRDT Scope Lifecycle & DI Resolution Overhead ===");
        output.WriteLine("| Scope Stage | Allocated Bytes | Delta From Previous |");
        output.WriteLine("| :--- | :---: | :---: |");
        output.WriteLine($"| 1. Bare Uninitialized Scope (`CreateScope` only) | {bareUninitializedBytes:N0} B | - |");
        output.WriteLine($"| 2. Full Scoped Services Resolved (Pipeline, Coordinators, Processors) | {fullDiResolvedBytes:N0} B | +{fullDiResolvedBytes - bareUninitializedBytes:N0} B |");
        output.WriteLine($"| 3. Initialized Scope (with `system-document-registry`) | {initializedScopeBytes:N0} B | +{initializedScopeBytes - fullDiResolvedBytes:N0} B |");
        output.WriteLine($"| 4. Initialized Scope + 1 Empty User Document | {singleDocScopeBytes:N0} B | +{singleDocScopeBytes - initializedScopeBytes:N0} B |");

        bareUninitializedBytes.ShouldBeGreaterThan(0);
        fullDiResolvedBytes.ShouldBeGreaterThanOrEqualTo(bareUninitializedBytes);
        initializedScopeBytes.ShouldBeGreaterThan(bareUninitializedBytes);
        singleDocScopeBytes.ShouldBeGreaterThan(initializedScopeBytes);
    }

    [IntegrationFact]
    public async Task MeasureScopeMemoryFootprint_AcrossDocumentAndOperationScales()
    {
        using var sp = BuildNode();
        var scopeFactory = sp.GetRequiredService<IDistributedCrdtScopeFactory>();

        // Warmup
        using (var warmup = scopeFactory.CreateScope("scale-warmup", "HostReplica"))
        {
            await PopulateScopeWithDocumentsAsync(warmup, docCount: 1, initialItems: 5, opsPerDoc: 5);
        }

        var scenarios = new[]
        {
            new ScopeWorkloadScenario("Minimal (1 doc, 0 items, 0 ops)", 1, 0, 0),
            new ScopeWorkloadScenario("Light (1 doc, 5 items, 10 ops)", 1, 5, 10),
            new ScopeWorkloadScenario("Small Multi-Doc (5 docs, 5 items, 10 ops)", 5, 5, 10),
            new ScopeWorkloadScenario("Medium Multi-Doc (10 docs, 20 items, 25 ops)", 10, 20, 25),
            new ScopeWorkloadScenario("Heavy Multi-Doc (25 docs, 50 items, 50 ops)", 25, 50, 50)
        };

        output.WriteLine("=== Active Scope Memory Footprint Across Document & Operation Scales ===");
        output.WriteLine("| Scenario | User Docs | Estimated Data | Estimated Meta | User Docs Total | System Registry | Total Est. Footprint | Heap Retained Bytes |");
        output.WriteLine("| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |");

        long previousTotalFootprint = 0;

        foreach (var scenario in scenarios)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long heapBefore = GC.GetTotalMemory(forceFullCollection: true);

            using var scope = scopeFactory.CreateScope($"scale-{scenario.DocCount}-{scenario.OpsPerDoc}", "HostReplica");
            await PopulateScopeWithDocumentsAsync(scope, scenario.DocCount, scenario.InitialItems, scenario.OpsPerDoc);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long heapAfter = GC.GetTotalMemory(forceFullCollection: true);
            long retainedHeap = Math.Max(0, heapAfter - heapBefore);

            var registryFootprint = await scope.Orchestrator.Registry.EstimateMemoryFootprintAsync(CancellationToken.None);

            long totalUserDataBytes = 0;
            long totalUserMetaBytes = 0;
            long totalUserWholeBytes = 0;

            var activeDocs = scope.Orchestrator.GetActiveDocuments();
            foreach (var activeDoc in activeDocs)
            {
                if (string.Equals(activeDoc.DocumentId, Constants.SystemRegistryDocumentId, StringComparison.Ordinal))
                {
                    continue;
                }

                var docFootprint = await activeDoc.EstimateMemoryFootprintAsync(CancellationToken.None);
                totalUserDataBytes += docFootprint.DataBytes;
                totalUserMetaBytes += docFootprint.MetadataBytes;
                totalUserWholeBytes += docFootprint.WholeDocumentBytes;
            }

            long totalEstimatedFootprint = totalUserWholeBytes + registryFootprint.WholeDocumentBytes + registryFootprint.ShardVersionVectorBytes;

            output.WriteLine(
                $"| {scenario.Name} | {scenario.DocCount} | {totalUserDataBytes:N0} B | {totalUserMetaBytes:N0} B | {totalUserWholeBytes:N0} B | {registryFootprint.WholeDocumentBytes:N0} B | {totalEstimatedFootprint:N0} B | {retainedHeap:N0} B |");

            totalUserWholeBytes.ShouldBeGreaterThan(0);
            registryFootprint.WholeDocumentBytes.ShouldBeGreaterThan(0);
            totalEstimatedFootprint.ShouldBeGreaterThan(previousTotalFootprint);
            previousTotalFootprint = totalEstimatedFootprint;
        }
    }

    [IntegrationFact]
    public async Task MeasureScopeMemoryFootprint_AcrossStorageAndJournalingModes()
    {
        var modes = new[]
        {
            new StorageModeScenario("1. Direct Mode (`CrdtJournalMode.Direct` + `MemoryCrdtStorage`)", CrdtJournalMode.Direct, UseExternalStorage: false),
            new StorageModeScenario("2. InMemoryBuffer Mode (with `MemoryCrdtStorage` bypass)", CrdtJournalMode.InMemoryBuffer, UseExternalStorage: false),
            new StorageModeScenario("3. InMemoryBuffer Mode (with `BufferedDistributedCrdtStorageDecorator`)", CrdtJournalMode.InMemoryBuffer, UseExternalStorage: true),
            new StorageModeScenario("4. DiskWal Mode (with `DiskWriteAheadBuffer` decorator)", CrdtJournalMode.DiskWal, UseExternalStorage: true)
        };

        output.WriteLine("=== Distributed Scope Overhead Across Storage & Journaling Modes ===");
        output.WriteLine("| Journaling Configuration | Bare Scope Allocation | Initialized Scope Allocation | Active Scope (5 Docs, 15 Ops/Doc) Allocation |");
        output.WriteLine("| :--- | :---: | :---: | :---: |");

        foreach (var mode in modes)
        {
            var walDir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"crdt-mem-wal-{Guid.NewGuid():N}");
            try
            {
                using var sp = BuildNode(services =>
                {
                    services.Configure<DistributedCrdtOptions>(o =>
                    {
                        o.JournalMode = mode.JournalMode;
                        o.WalDirectoryPath = walDir;
                    });

                    if (mode.UseExternalStorage)
                    {
                        services.AddDistributedCrdtStorage<SimulatedExternalCrdtStorage>();
                    }
                });

                var scopeFactory = sp.GetRequiredService<IDistributedCrdtScopeFactory>();

                // Warmup
                using (var warmup = scopeFactory.CreateScope("mode-warmup", "HostReplica"))
                {
                    await PopulateScopeWithDocumentsAsync(warmup, docCount: 1, initialItems: 2, opsPerDoc: 2);
                }

                long bareBytes = MeasureAllocatedBytes(() =>
                {
                    using var scope = scopeFactory.CreateScope("mode-bare", "HostReplica");
                    return scope.Storage;
                }, iterations: 15);

                long initBytes = await MeasureAllocatedBytesAsync(async () =>
                {
                    using var scope = scopeFactory.CreateScope("mode-init", "HostReplica");
                    await scope.Orchestrator.InitializeAsync(CancellationToken.None);
                    return scope.Orchestrator.InitializationStage;
                }, iterations: 10);

                long activeBytes = await MeasureAllocatedBytesAsync(async () =>
                {
                    using var scope = scopeFactory.CreateScope("mode-active", "HostReplica");
                    await PopulateScopeWithDocumentsAsync(scope, docCount: 5, initialItems: 5, opsPerDoc: 15);
                    return scope.Orchestrator.GetActiveDocuments().Count;
                }, iterations: 5);

                output.WriteLine($"| {mode.Name} | {bareBytes:N0} B | {initBytes:N0} B | {activeBytes:N0} B |");

                bareBytes.ShouldBeGreaterThan(0);
                initBytes.ShouldBeGreaterThan(bareBytes);
                activeBytes.ShouldBeGreaterThan(initBytes);
            }
            finally
            {
                if (System.IO.Directory.Exists(walDir))
                {
                    try { System.IO.Directory.Delete(walDir, recursive: true); } catch { }
                }
            }
        }
    }

    [IntegrationFact]
    public async Task MeasureHighCardinalityDynamicScopes_ScalingAndEvictionMemoryReclamation()
    {
        var sharedBackingStorage = new MemoryCrdtStorage();
        using var sp = BuildNode(services =>
        {
            services.AddSingleton<IDistributedCrdtStorage>(sharedBackingStorage);
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();

        // Ensure host scope is initialized first
        var hostScope = await scopeManager.GetOrCreateScopeAsync("HostReplica", cancellationToken: CancellationToken.None);
        hostScope.IsPinnedHostScope.ShouldBeTrue();

        // Warmup dynamic scope creation & eviction path
        var warmupScope = await scopeManager.GetOrCreateScopeAsync("warmup-user", "HostReplica", CancellationToken.None);
        await PopulateScopeWithDocumentsAsync(warmupScope, docCount: 1, initialItems: 2, opsPerDoc: 2);
        await Task.Delay(25);
        await scopeManager.EvictIdleDynamicScopesAsync(TimeSpan.FromMilliseconds(10), CancellationToken.None);

        var scopeCounts = new[] { 10, 50, 150 };

        output.WriteLine("=== High-Cardinality Dynamic Scopes Memory Scaling & Eviction Reclamation ===");
        output.WriteLine("| Dynamic Scopes | Total Retained Bytes | Bytes / Scope | Total Est. Docs Footprint | Evicted Scopes | Post-Eviction Retained | Reclaimed % |");
        output.WriteLine("| :---: | :---: | :---: | :---: | :---: | :---: | :---: |");

        foreach (var scopeCount in scopeCounts)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long baselineHeap = GC.GetTotalMemory(forceFullCollection: true);

            long totalEstimatedDocsFootprint = 0;

            for (int i = 0; i < scopeCount; i++)
            {
                var scopeId = $"tenant-{scopeCount}-user-{i:D4}";
                var dynamicScope = await scopeManager.GetOrCreateScopeAsync(scopeId, "HostReplica", CancellationToken.None);

                await PopulateScopeWithDocumentsAsync(dynamicScope, docCount: 2, initialItems: 5, opsPerDoc: 5);

                foreach (var doc in dynamicScope.Orchestrator.GetActiveDocuments())
                {
                    totalEstimatedDocsFootprint += await doc.EstimateObjectSizeAsync(CancellationToken.None);
                }
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long peakHeap = GC.GetTotalMemory(forceFullCollection: true);

            long retainedAtPeak = Math.Max(1, peakHeap - baselineHeap);
            double bytesPerScope = (double)retainedAtPeak / scopeCount;

            // Wait for idle TTL and evict all dynamic scopes
            await Task.Delay(35);
            int evicted = await scopeManager.EvictIdleDynamicScopesAsync(TimeSpan.FromMilliseconds(15), CancellationToken.None);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long postEvictionHeap = GC.GetTotalMemory(forceFullCollection: true);

            // Note: sharedBackingStorage retains checkpoints in-memory for re-hydration, so we clear its in-memory dictionary
            // to measure purely the runtime scope / DI / orchestrator / document memory reclamation.
            long retainedAfterEviction = Math.Max(0, postEvictionHeap - baselineHeap);
            double reclaimedPct = Math.Max(0.0, (1.0 - ((double)retainedAfterEviction / retainedAtPeak)) * 100.0);

            output.WriteLine(
                $"| {scopeCount} | {retainedAtPeak:N0} B | {bytesPerScope:N0} B | {totalEstimatedDocsFootprint:N0} B | {evicted} | {retainedAfterEviction:N0} B | {reclaimedPct:F1}% |");

            evicted.ShouldBe(scopeCount);
            scopeManager.GetActiveScopes().Count.ShouldBe(1); // Only pinned HostReplica remains
            totalEstimatedDocsFootprint.ShouldBeGreaterThan(0);
        }
    }

    [IntegrationFact]
    public async Task MeasureScopeClusterTopologyAndShardDvvMemory_AcrossClusterDimensions()
    {
        var clusterDimensions = new[]
        {
            new ClusterTopologyScenario("Small Cluster (2 shards, 3 peers, 0 dots)", Shards: 2, Peers: 3, DotsPerReplica: 0),
            new ClusterTopologyScenario("Medium Cluster (10 shards, 10 peers, 5 dots)", Shards: 10, Peers: 10, DotsPerReplica: 5),
            new ClusterTopologyScenario("Large Cluster (25 shards, 25 peers, 10 dots)", Shards: 25, Peers: 25, DotsPerReplica: 10),
            new ClusterTopologyScenario("Enterprise Mesh (50 shards, 50 peers, 15 dots)", Shards: 50, Peers: 50, DotsPerReplica: 15)
        };

        output.WriteLine("=== Scope ClusterStateTracker & ShardVersionVectorManager Memory Overhead ===");
        output.WriteLine("| Topology Configuration | Local DVV Manager Allocation | Cluster Tracker Allocation | Est. Shard DVV Bytes | Combined Topology Bytes | Bytes / Peer |");
        output.WriteLine("| :--- | :---: | :---: | :---: | :---: | :---: |");

        long previousCombinedBytes = 0;

        foreach (var cfg in clusterDimensions)
        {
            using var sp = BuildNode();
            var scopeFactory = sp.GetRequiredService<IDistributedCrdtScopeFactory>();
            using var scope = scopeFactory.CreateScope($"topo-{cfg.Shards}-{cfg.Peers}", "HostReplica");
            await scope.Orchestrator.InitializeAsync(CancellationToken.None);

            var dvvManager = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
            var shardIds = CreateIds("shard", cfg.Shards);
            var peerIds = CreateIds("peer-replica", cfg.Peers);

            long localDvvBytes = MeasureAllocatedBytes(() =>
            {
                var map = new ShardVersionVectorMap();
                for (int s = 0; s < shardIds.Length; s++)
                {
                    map[shardIds[s]] = CreateDvv(peerIds, cfg.DotsPerReplica);
                }
                return map;
            }, iterations: 20);

            // Load into scope's ShardVersionVectorManager
            var localMap = new ShardVersionVectorMap();
            for (int s = 0; s < shardIds.Length; s++)
            {
                localMap[shardIds[s]] = CreateDvv(peerIds, cfg.DotsPerReplica);
            }
            dvvManager.RestoreFullState(localMap);

            // Measure ClusterStateTracker allocation when tracking all peers' ShardVersionVectorMaps
            long clusterTrackerBytes = MeasureAllocatedBytes(() =>
            {
                using var tempScope = scopeFactory.CreateScope("tracker-bench", "HostReplica");
                var tracker = tempScope.ClusterTracker;
                for (int p = 0; p < peerIds.Length; p++)
                {
                    var peerMap = new ShardVersionVectorMap();
                    for (int s = 0; s < shardIds.Length; s++)
                    {
                        peerMap[shardIds[s]] = CreateDvv(peerIds, cfg.DotsPerReplica);
                    }
                    tracker.UpdatePeerState(peerIds[p], $"net-{p:D3}", peerMap);
                }
                return tracker.GetActiveReplicaIds().Count;
            }, iterations: 10);

            var registryFootprint = await scope.Orchestrator.Registry.EstimateMemoryFootprintAsync(CancellationToken.None);
            long combinedBytes = localDvvBytes + clusterTrackerBytes;
            double bytesPerPeer = (double)combinedBytes / cfg.Peers;

            output.WriteLine(
                $"| {cfg.Name} | {localDvvBytes:N0} B | {clusterTrackerBytes:N0} B | {registryFootprint.ShardVersionVectorBytes:N0} B | {combinedBytes:N0} B | {bytesPerPeer:F1} B |");

            localDvvBytes.ShouldBeGreaterThan(0);
            clusterTrackerBytes.ShouldBeGreaterThan(localDvvBytes);
            registryFootprint.ShardVersionVectorBytes.ShouldBeGreaterThan(0);
            combinedBytes.ShouldBeGreaterThan(previousCombinedBytes);
            previousCombinedBytes = combinedBytes;
        }
    }

    [IntegrationFact]
    public async Task MeasureScopeShardDemotionAndCompaction_MemoryReduction()
    {
        bool keepEvenShardsOnly = false;
        var mockDistribution = new Mock<IShardDistributionManager>();
        mockDistribution
            .Setup(d => d.IsShardLocalAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string shardId, CancellationToken _) =>
            {
                if (string.Equals(shardId, Constants.SystemRegistryDocumentId, StringComparison.Ordinal))
                {
                    return new ValueTask<bool>(true);
                }

                if (!keepEvenShardsOnly)
                {
                    return new ValueTask<bool>(true);
                }

                // Only even numbered shards remain local after topology rebalance
                int suffix = int.TryParse(shardId.AsSpan(shardId.LastIndexOf('-') + 1), out var n) ? n : 0;
                return new ValueTask<bool>(suffix % 2 == 0);
            });
        mockDistribution
            .Setup(d => d.GetTargetNodesForShardAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<string>());

        using var sp = BuildNode(services =>
        {
            services.AddSingleton<IShardIdProvider, MultiShardTestProvider>();
            services.AddScoped(_ => mockDistribution.Object);
        });

        var scopeManager = sp.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = await scopeManager.GetOrCreateScopeAsync("HostReplica", cancellationToken: CancellationToken.None);

        // Populate 20 documents across 10 shards with 25 operations each
        await PopulateScopeWithDocumentsAsync(scope, docCount: 20, initialItems: 10, opsPerDoc: 25);

        long footprintBeforeDemotion = await CalculateScopeTotalEstimatedFootprintAsync(scope);
        int activeDocsBeforeDemotion = scope.Orchestrator.GetActiveDocuments().Count;

        // Rebalance topology so 50% of shards (10 user documents) are demoted from local memory
        keepEvenShardsOnly = true;
        await scope.Orchestrator.ReevaluateTopologyAsync(CancellationToken.None);

        long footprintAfterDemotion = await CalculateScopeTotalEstimatedFootprintAsync(scope);
        int activeDocsAfterDemotion = scope.Orchestrator.GetActiveDocuments().Count;

        double reductionPct = (1.0 - ((double)footprintAfterDemotion / footprintBeforeDemotion)) * 100.0;

        output.WriteLine("=== Shard Rebalance Demotion Memory Reduction ===");
        output.WriteLine("| Phase | Active Documents (incl. Registry) | Total Estimated Footprint | Memory Reduction % |");
        output.WriteLine("| :--- | :---: | :---: | :---: |");
        output.WriteLine($"| Before Topology Rebalance (All 10 Shards Local) | {activeDocsBeforeDemotion} | {footprintBeforeDemotion:N0} B | - |");
        output.WriteLine($"| After Topology Rebalance (5 Even Shards Local) | {activeDocsAfterDemotion} | {footprintAfterDemotion:N0} B | {reductionPct:F1}% |");

        activeDocsBeforeDemotion.ShouldBe(21); // 20 user docs + 1 system registry
        activeDocsAfterDemotion.ShouldBe(11);  // 10 user docs + 1 system registry
        footprintAfterDemotion.ShouldBeLessThan(footprintBeforeDemotion);
        reductionPct.ShouldBeGreaterThan(35.0);
    }

    private static ServiceProvider BuildNode(Action<IServiceCollection>? configureExtra = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(options =>
        {
            options.CompactionTtlSeconds = 60;
        });

        services.AddDistributedCrdtReplica("HostReplica");

        services.AddCrdt()
                .AddCrdtAotContext(new ScopeMemoryOverheadAotContext())
                .AddCrdtJsonTypeInfoResolver(ScopeMemoryOverheadJsonContext.Default);

        services.AddDistributedDocumentType<DistributedMemoryTestModel>("memory-test-doc");

        var mockP2p = new Mock<IP2pAlgorithm>();
        mockP2p.SetReturnsDefault(Task.CompletedTask);
        services.AddSingleton(mockP2p.Object);

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
    }

    private static async Task PopulateScopeWithDocumentsAsync(
        IDistributedCrdtScope scope,
        int docCount,
        int initialItems,
        int opsPerDoc)
    {
        var orchestrator = scope.Orchestrator;
        await orchestrator.InitializeAsync(CancellationToken.None);

        for (int d = 0; d < docCount; d++)
        {
            var docId = $"doc-{d:D3}";
            await orchestrator.CreateDocumentAsync(docId, "memory-test-doc", CancellationToken.None);

            for (int item = 0; item < initialItems; item++)
            {
                await orchestrator.ApplyIntentAsync<DistributedMemoryTestModel>(
                    docId,
                    new AddIntent($"init-tag-{item}"),
                    nameof(DistributedMemoryTestModel.Tags),
                    CancellationToken.None);

                await orchestrator.ApplyIntentAsync<DistributedMemoryTestModel>(
                    docId,
                    new MapSetIntent($"init-k{item}", item),
                    nameof(DistributedMemoryTestModel.Scores),
                    CancellationToken.None);
            }

            for (int op = 0; op < opsPerDoc; op++)
            {
                switch (op % 4)
                {
                    case 0:
                        await orchestrator.ApplyIntentAsync<DistributedMemoryTestModel>(
                            docId,
                            new IncrementIntent(1),
                            nameof(DistributedMemoryTestModel.Count),
                            CancellationToken.None);
                        break;
                    case 1:
                        await orchestrator.ApplyIntentAsync<DistributedMemoryTestModel>(
                            docId,
                            new AddIntent($"op-tag-{op}"),
                            nameof(DistributedMemoryTestModel.Tags),
                            CancellationToken.None);
                        break;
                    case 2:
                        await orchestrator.ApplyIntentAsync<DistributedMemoryTestModel>(
                            docId,
                            new MapSetIntent($"op-k{op}", op * 10),
                            nameof(DistributedMemoryTestModel.Scores),
                            CancellationToken.None);
                        break;
                    default:
                        await orchestrator.ApplyIntentAsync<DistributedMemoryTestModel>(
                            docId,
                            new MapRemoveIntent(op >= 4 ? $"op-k{op - 2}" : "init-k0"),
                            nameof(DistributedMemoryTestModel.Scores),
                            CancellationToken.None);
                        break;
                }
            }
        }

        await orchestrator.FlushAsync(CancellationToken.None);
    }

    private static async Task<long> CalculateScopeTotalEstimatedFootprintAsync(IDistributedCrdtScope scope)
    {
        long total = 0;
        var docs = scope.Orchestrator.GetActiveDocuments();
        for (int i = 0; i < docs.Count; i++)
        {
            var fp = await docs[i].EstimateMemoryFootprintAsync(CancellationToken.None);
            total += fp.WholeDocumentBytes;
            if (i == 0)
            {
                total += fp.ShardVersionVectorBytes;
            }
        }

        return total;
    }

    private static DottedVersionVector CreateDvv(string[] replicaIds, int dotsPerReplica)
    {
        var dvv = new DottedVersionVector();
        for (int r = 0; r < replicaIds.Length; r++)
        {
            var repId = replicaIds[r];
            dvv.Versions[repId] = 50;
            for (int d = 0; d < dotsPerReplica; d++)
            {
                dvv.Add(repId, 52 + (d * 2));
            }
        }

        return dvv;
    }

    private static string[] CreateIds(string prefix, int count)
    {
        var arr = new string[count];
        for (int i = 0; i < count; i++)
        {
            arr[i] = $"{prefix}-{i:D3}";
        }

        return arr;
    }

    private static long MeasureAllocatedBytes<T>(Func<T> action, int iterations = 25)
    {
        for (int w = 0; w < 10; w++)
        {
            _ = action(); // Warmup MS.DI DynamicMethod callsite compilation (triggers after 2+ calls)
        }

        // MS.DI schedules ExpressionResolverBuilder compilation on ThreadPool.QueueUserWorkItem after 2 calls.
        // Wait for background compilation to complete and verify allocation stability before sampling.
        Thread.Sleep(50);
        for (int s = 0; s < 10; s++)
        {
            long b1 = GC.GetAllocatedBytesForCurrentThread();
            _ = action();
            long a1 = GC.GetAllocatedBytesForCurrentThread() - b1;

            long b2 = GC.GetAllocatedBytesForCurrentThread();
            _ = action();
            long a2 = GC.GetAllocatedBytesForCurrentThread() - b2;

            if (a1 == a2)
            {
                break;
            }

            Thread.Sleep(15);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long[] samples = new long[iterations];
        for (int i = 0; i < iterations; i++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            _ = action();
            long after = GC.GetAllocatedBytesForCurrentThread();
            samples[i] = Math.Max(1, after - before);
        }

        Array.Sort(samples);
        return samples[0];
    }

    private static async Task<long> MeasureAllocatedBytesAsync<T>(Func<Task<T>> action, int iterations = 15)
    {
        for (int w = 0; w < 10; w++)
        {
            _ = await action(); // Warmup
        }

        await Task.Delay(50);
        _ = await action();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long[] samples = new long[iterations];
        for (int i = 0; i < iterations; i++)
        {
            long before = GC.GetTotalAllocatedBytes(precise: true);
            _ = await action();
            long after = GC.GetTotalAllocatedBytes(precise: true);
            samples[i] = Math.Max(1, after - before);
        }

        Array.Sort(samples);
        return samples[0];
    }

    private readonly record struct ScopeWorkloadScenario(
        string Name,
        int DocCount,
        int InitialItems,
        int OpsPerDoc);

    private readonly record struct StorageModeScenario(
        string Name,
        CrdtJournalMode JournalMode,
        bool UseExternalStorage);

    private readonly record struct ClusterTopologyScenario(
        string Name,
        int Shards,
        int Peers,
        int DotsPerReplica);

    private sealed class SimulatedExternalCrdtStorage : IDistributedCrdtStorage
    {
        private readonly MemoryCrdtStorage inner = new();

        public void Append(IComparable documentId, IReadOnlyList<CrdtOperation> operations)
            => inner.Append(documentId, operations);

        public Task AppendAsync(IComparable documentId, IReadOnlyList<CrdtOperation> operations, CancellationToken cancellationToken = default)
            => inner.AppendAsync(documentId, operations, cancellationToken);

        public IAsyncEnumerable<JournaledOperation> GetOperationsByRangeAsync(string shardId, string originReplicaId, long minShardClock, long maxShardClock, CancellationToken cancellationToken = default)
            => inner.GetOperationsByRangeAsync(shardId, originReplicaId, minShardClock, maxShardClock, cancellationToken);

        public IAsyncEnumerable<JournaledOperation> GetOperationsByDotsAsync(string shardId, string originReplicaId, IEnumerable<long> shardClocks, CancellationToken cancellationToken = default)
            => inner.GetOperationsByDotsAsync(shardId, originReplicaId, shardClocks, cancellationToken);

        public Task<ShardVersionVectorMap?> LoadShardVersionVectorMapAsync(string replicaId, CancellationToken cancellationToken = default)
            => inner.LoadShardVersionVectorMapAsync(replicaId, cancellationToken);

        public Task SaveShardVersionVectorMapAsync(string replicaId, ShardVersionVectorMap shardVersionVectorMap, CancellationToken cancellationToken = default)
            => inner.SaveShardVersionVectorMapAsync(replicaId, shardVersionVectorMap, cancellationToken);

        public Task<ClusterStateSnapshotDto?> LoadClusterStateAsync(string replicaId, CancellationToken cancellationToken = default)
            => inner.LoadClusterStateAsync(replicaId, cancellationToken);

        public Task SaveClusterStateAsync(string replicaId, ClusterStateSnapshotDto state, CancellationToken cancellationToken = default)
            => inner.SaveClusterStateAsync(replicaId, state, cancellationToken);

        public Task<CrdtDocument<TState>?> LoadDocumentAsync<TState>(string shardId, string documentId, CancellationToken cancellationToken = default) where TState : class, new()
            => inner.LoadDocumentAsync<TState>(shardId, documentId, cancellationToken);

        public Task<CrdtDocument<TState>?> LoadDocumentFromAnyShardAsync<TState>(string documentId, CancellationToken cancellationToken = default) where TState : class, new()
            => inner.LoadDocumentFromAnyShardAsync<TState>(documentId, cancellationToken);

        public Task<CrdtDocument<TState>?> LoadOrphanedDocumentAsync<TState>(string shardId, string documentId, CancellationToken cancellationToken = default) where TState : class, new()
            => inner.LoadOrphanedDocumentAsync<TState>(shardId, documentId, cancellationToken);

        public Task SaveDocumentAsync<TState>(string shardId, string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default) where TState : class, new()
            => inner.SaveDocumentAsync(shardId, documentId, document, cancellationToken);

        public Task SaveSerializedDocumentAsync<TState>(string shardId, string documentId, byte[] serializedDocument, CancellationToken cancellationToken = default) where TState : class, new()
            => inner.SaveSerializedDocumentAsync<TState>(shardId, documentId, serializedDocument, cancellationToken);

        public Task DeleteDocumentAsync(string shardId, string documentId, CancellationToken cancellationToken = default)
            => inner.DeleteDocumentAsync(shardId, documentId, cancellationToken);

        public Task<long> GetJournalCountAsync(string shardId, CancellationToken cancellationToken = default)
            => inner.GetJournalCountAsync(shardId, cancellationToken);

        public Task TrimAsync(string shardId, IReadOnlyDictionary<string, long> globalMinimumVersionVector, CancellationToken cancellationToken = default)
            => inner.TrimAsync(shardId, globalMinimumVersionVector, cancellationToken);

        public IAsyncEnumerable<JournaledOperation> GetAllJournaledOperationsAsync(CancellationToken cancellationToken = default)
            => inner.GetAllJournaledOperationsAsync(cancellationToken);

        public Task AppendBatchAsync(IReadOnlyList<JournaledOperation> operations, CancellationToken cancellationToken = default)
            => inner.AppendBatchAsync(operations, cancellationToken);
    }

    private sealed class MultiShardTestProvider : IShardIdProvider
    {
        public string GetShardId<T>(CrdtDocument<T> document) where T : class
        {
            if (document.Data is DistributedMemoryTestModel model && !string.IsNullOrEmpty(model.Id))
            {
                return ResolveShard(model.Id);
            }

            return "shard-0";
        }

        public string GetShardId<T>(IComparable documentId) where T : class
            => ResolveShard(documentId?.ToString() ?? string.Empty);

        public string GetShardId(IComparable documentId, Type documentType)
            => ResolveShard(documentId?.ToString() ?? string.Empty);

        private static string ResolveShard(string docId)
        {
            int dashIdx = docId.LastIndexOf('-');
            if (dashIdx >= 0 && int.TryParse(docId.AsSpan(dashIdx + 1), out int docNum))
            {
                return $"shard-{docNum % 10}";
            }

            return "shard-0";
        }
    }
}