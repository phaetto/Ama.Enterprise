namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Concurrent;
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
using Ama.CRDT.Services;
using Ama.CRDT.Services.Providers;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

[CrdtAotType(typeof(DvvExchangeAndOscillationIntegrationTests.DvvTestState))]
[CrdtAotType(typeof(Dictionary<string, string>))]
public sealed partial class DvvExchangeTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(DvvExchangeAndOscillationIntegrationTests.DvvTestState))]
[JsonSerializable(typeof(CrdtDocument<DvvExchangeAndOscillationIntegrationTests.DvvTestState>))]
public sealed partial class DvvExchangeTestJsonContext : JsonSerializerContext
{
}

public sealed class DvvExchangeAndOscillationIntegrationTests : IAsyncDisposable
{
    public sealed class DvvTestState : IEquatable<DvvTestState>
    {
        public string Id { get; set; } = "dvv-doc";
        public Dictionary<string, string> DataMap { get; set; } = new(StringComparer.Ordinal);

        public bool Equals(DvvTestState? other)
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
                if (!other.DataMap.TryGetValue(kvp.Key, out var otherValue) || !string.Equals(kvp.Value, otherValue, StringComparison.Ordinal))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj) => Equals(obj as DvvTestState);

        public override int GetHashCode() => HashCode.Combine(Id, DataMap.Count);
    }

    private readonly List<TestNode> nodes = new();

    [IntegrationFact]
    public async Task TwoReplicas_DvvExchange_ShouldConvergeMonotonically_WithoutOscillations()
    {
        // Arrange
        var nodeA = BuildNode("ReplicaA", opt => opt.ActiveSyncEnabled = false);
        var nodeB = BuildNode("ReplicaB", opt => opt.ActiveSyncEnabled = false);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManagerA = scopeA.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManagerB = scopeB.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await orchB.InitializeAsync(CancellationToken.None);

        await orchA.CreateDocumentAsync("doc-dvv-1", "dvv-doc", CancellationToken.None);
        await orchA.CreateDocumentAsync("doc-dvv-2", "dvv-doc", CancellationToken.None);

        await orchB.CreateDocumentAsync("doc-dvv-1", "dvv-doc", CancellationToken.None);
        await orchB.CreateDocumentAsync("doc-dvv-2", "dvv-doc", CancellationToken.None);

        // Apply distinct mutations on both nodes while relying solely on DVV anti-entropy
        await orchA.ApplyIntentAsync<DvvTestState>("doc-dvv-1", new MapSetIntent("A1", "ValA1"), "DataMap", CancellationToken.None);
        await orchA.ApplyIntentAsync<DvvTestState>("doc-dvv-2", new MapSetIntent("A2", "ValA2"), "DataMap", CancellationToken.None);

        await orchB.ApplyIntentAsync<DvvTestState>("doc-dvv-1", new MapSetIntent("B1", "ValB1"), "DataMap", CancellationToken.None);
        await orchB.ApplyIntentAsync<DvvTestState>("doc-dvv-2", new MapSetIntent("B2", "ValB2"), "DataMap", CancellationToken.None);

        var recordedMessages = new ConcurrentQueue<CapturedMessageDto>();
        using var network = WireMeshWithInspection(recordedMessages, nodeA, nodeB);

        // Act - Round 1: Exchange DVVs in both directions and allow patches to be applied
        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        await WaitForConvergenceAsync(async () =>
        {
            var docA1 = await orchA.GetDocumentStateAsync<DvvTestState>("doc-dvv-1", CancellationToken.None);
            var docA2 = await orchA.GetDocumentStateAsync<DvvTestState>("doc-dvv-2", CancellationToken.None);
            var docB1 = await orchB.GetDocumentStateAsync<DvvTestState>("doc-dvv-1", CancellationToken.None);
            var docB2 = await orchB.GetDocumentStateAsync<DvvTestState>("doc-dvv-2", CancellationToken.None);

            return docA1.Data.DataMap.ContainsKey("B1") &&
                   docA2.Data.DataMap.ContainsKey("B2") &&
                   docB1.Data.DataMap.ContainsKey("A1") &&
                   docB2.Data.DataMap.ContainsKey("A2");
        }, TimeSpan.FromSeconds(5));

        var snapshotA = await dvvManagerA.GetFullSnapshotAsync(CancellationToken.None);
        var snapshotB = await dvvManagerB.GetFullSnapshotAsync(CancellationToken.None);

        snapshotA["default"].Versions["ReplicaA"].ShouldBe(2);
        snapshotA["default"].Versions["ReplicaB"].ShouldBe(2);
        snapshotB["default"].Versions["ReplicaA"].ShouldBe(2);
        snapshotB["default"].Versions["ReplicaB"].ShouldBe(2);

        // Ensure registry shards are also converged before testing steady-state exchanges
        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        // Clear recorded messages after initial convergence to verify zero oscillations in subsequent exchanges
        recordedMessages.Clear();

        // Act - Execute 5 consecutive bidirectional DVV exchange rounds
        for (int round = 0; round < 5; round++)
        {
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await network.DrainAsync();
        }

        // Assert - Only CrdtSync messages were sent; zero CrdtPatch or CrdtSnapshot messages were triggered
        var postConvergenceMessages = recordedMessages.ToList();
        postConvergenceMessages.Count.ShouldBe(10);
        postConvergenceMessages.ShouldAllBe(m => m.MessageType == "CrdtSync");

        var finalSnapshotA = await dvvManagerA.GetFullSnapshotAsync(CancellationToken.None);
        var finalSnapshotB = await dvvManagerB.GetFullSnapshotAsync(CancellationToken.None);
        finalSnapshotA["default"].Versions["ReplicaA"].ShouldBe(2);
        finalSnapshotA["default"].Versions["ReplicaB"].ShouldBe(2);
        finalSnapshotB["default"].Versions["ReplicaA"].ShouldBe(2);
        finalSnapshotB["default"].Versions["ReplicaB"].ShouldBe(2);
    }

    [IntegrationFact]
    public async Task TwoReplicas_MultiDocumentShardSnapshotFallback_ShouldNotAdvanceDvvPrematurely_OrOscillate()
    {
        // Arrange
        var nodeA = BuildNode("ReplicaA", opt => opt.ActiveSyncEnabled = false);
        var nodeB = BuildNode("ReplicaB", opt => opt.ActiveSyncEnabled = false);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var storageA = scopeA.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManagerB = scopeB.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await orchB.InitializeAsync(CancellationToken.None);

        // Both nodes know about the two documents in the "default" shard
        await orchA.CreateDocumentAsync("doc-snap-1", "dvv-doc", CancellationToken.None);
        await orchA.CreateDocumentAsync("doc-snap-2", "dvv-doc", CancellationToken.None);

        await orchB.CreateDocumentAsync("doc-snap-1", "dvv-doc", CancellationToken.None);
        await orchB.CreateDocumentAsync("doc-snap-2", "dvv-doc", CancellationToken.None);

        // Node A applies mutations to both documents on shard "default"
        await orchA.ApplyIntentAsync<DvvTestState>("doc-snap-1", new MapSetIntent("K1", "SnapVal1"), "DataMap", CancellationToken.None);
        await orchA.ApplyIntentAsync<DvvTestState>("doc-snap-2", new MapSetIntent("K2", "SnapVal2"), "DataMap", CancellationToken.None);

        // Trim Node A's journal for shard "default" so anti-entropy must fall back to full document snapshots
        await storageA.TrimAsync("default", new Dictionary<string, long> { { "ReplicaA", 10 } }, CancellationToken.None);

        // Step 1: Verify that merging only the first document's snapshot on Node B does NOT prematurely advance Node B's shard DVV
        var docA1 = orchA.TryGetLocalDocument<DvvTestState>("doc-snap-1")!;
        var docA2 = orchA.TryGetLocalDocument<DvvTestState>("doc-snap-2")!;
        var snap1 = await docA1.GetSnapshotDataAsync(CancellationToken.None);
        var snap2 = await docA2.GetSnapshotDataAsync(CancellationToken.None);

        var docB1 = orchB.TryGetLocalDocument<DvvTestState>("doc-snap-1")!;
        var docB2 = orchB.TryGetLocalDocument<DvvTestState>("doc-snap-2")!;

        await docB1.MergeSnapshotAsync(snap1.SnapshotData, snap1.GlobalState, CancellationToken.None);

        var partialDvvB = dvvManagerB.GetShardSnapshot("default");
        partialDvvB["default"].Versions.ContainsKey("ReplicaA").ShouldBeFalse();

        // Step 2: Merge the second document's snapshot on Node B; now all active documents in the shard have merged snapshots
        await docB2.MergeSnapshotAsync(snap2.SnapshotData, snap2.GlobalState, CancellationToken.None);

        var completeDvvB = dvvManagerB.GetShardSnapshot("default");
        completeDvvB["default"].Versions.ShouldContainKeyAndValue("ReplicaA", 2);

        // Step 3: Wire mesh and run multiple bidirectional DVV exchanges to prove zero snapshot or patch oscillation
        var recordedMessages = new ConcurrentQueue<CapturedMessageDto>();
        using var network = WireMeshWithInspection(recordedMessages, nodeA, nodeB);

        // Let registry sync first if needed
        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        recordedMessages.Clear();

        for (int round = 0; round < 5; round++)
        {
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await network.DrainAsync();
        }

        // Assert - Neither node requests or dispatches any further snapshots or patches for the shard
        var messagesAfterSync = recordedMessages.ToList();
        messagesAfterSync.ShouldNotBeEmpty();
        messagesAfterSync.ShouldAllBe(m => m.MessageType == "CrdtSync");
    }

    [IntegrationFact]
    public async Task TwoReplicas_ConcurrentIntentsAndDvvExchanges_ShouldNeverTriggerFalseSnapshots_OrOscillate()
    {
        // Arrange
        var nodeA = BuildNode("ReplicaA", opt => opt.ActiveSyncEnabled = false);
        var nodeB = BuildNode("ReplicaB", opt => opt.ActiveSyncEnabled = false);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchB.InitializeAsync(CancellationToken.None);

        await orchA.CreateDocumentAsync("doc-conc-1", "dvv-doc", CancellationToken.None);
        await orchA.CreateDocumentAsync("doc-conc-2", "dvv-doc", CancellationToken.None);

        await orchB.CreateDocumentAsync("doc-conc-1", "dvv-doc", CancellationToken.None);
        await orchB.CreateDocumentAsync("doc-conc-2", "dvv-doc", CancellationToken.None);

        var recordedMessages = new ConcurrentQueue<CapturedMessageDto>();
        using var network = WireMeshWithInspection(recordedMessages, nodeA, nodeB);

        // Synchronize initial registry state before starting concurrent writers
        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        using var syncLoopCts = new CancellationTokenSource();

        // Continuously exchange DVVs while operations are in-flight
        var continuousSyncTask = Task.Run(async () =>
        {
            while (!syncLoopCts.Token.IsCancellationRequested)
            {
                await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
                await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
                await Task.Delay(20);
            }
        });

        // Act - Concurrently apply intents on both nodes across both documents in the same shard
        var writerATask = Task.Run(async () =>
        {
            for (int i = 0; i < 20; i++)
            {
                var docId = i % 2 == 0 ? "doc-conc-1" : "doc-conc-2";
                await orchA.ApplyIntentAsync<DvvTestState>(docId, new MapSetIntent($"KeyA_{i}", $"ValA_{i}"), "DataMap", CancellationToken.None);
            }
        });

        var writerBTask = Task.Run(async () =>
        {
            for (int i = 0; i < 20; i++)
            {
                var docId = i % 2 == 0 ? "doc-conc-1" : "doc-conc-2";
                await orchB.ApplyIntentAsync<DvvTestState>(docId, new MapSetIntent($"KeyB_{i}", $"ValB_{i}"), "DataMap", CancellationToken.None);
            }
        });

        await Task.WhenAll(writerATask, writerBTask);
        await syncLoopCts.CancelAsync();
        await continuousSyncTask;
        await network.DrainAsync();

        // Final settling anti-entropy exchange
        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        await WaitForConvergenceAsync(async () =>
        {
            var docA1 = await orchA.GetDocumentStateAsync<DvvTestState>("doc-conc-1", CancellationToken.None);
            var docA2 = await orchA.GetDocumentStateAsync<DvvTestState>("doc-conc-2", CancellationToken.None);
            var docB1 = await orchB.GetDocumentStateAsync<DvvTestState>("doc-conc-1", CancellationToken.None);
            var docB2 = await orchB.GetDocumentStateAsync<DvvTestState>("doc-conc-2", CancellationToken.None);

            return docA1.Data.DataMap.Count == 20 &&
                   docA2.Data.DataMap.Count == 20 &&
                   docB1.Data.DataMap.Count == 20 &&
                   docB2.Data.DataMap.Count == 20;
        }, TimeSpan.FromSeconds(5));

        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        // Assert 1 - No false-positive CrdtSnapshot was ever triggered by in-flight DVV reads
        recordedMessages.ShouldNotContain(m => m.MessageType == "CrdtSnapshot");

        // Assert 2 - Subsequent DVV exchanges produce zero CrdtPatch or CrdtSnapshot oscillations
        recordedMessages.Clear();

        for (int round = 0; round < 5; round++)
        {
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await network.DrainAsync();
        }

        var steadyStateMessages = recordedMessages.ToList();
        steadyStateMessages.Count.ShouldBe(10);
        steadyStateMessages.ShouldAllBe(m => m.MessageType == "CrdtSync");
    }

    [IntegrationFact]
    public async Task ThreeReplicas_TransitiveRingAndMeshDvvExchange_ShouldConvergeMonotonically_WithoutOscillations()
    {
        // Arrange
        var nodeA = BuildNode("ReplicaA", opt => opt.ActiveSyncEnabled = false);
        var nodeB = BuildNode("ReplicaB", opt => opt.ActiveSyncEnabled = false);
        var nodeC = BuildNode("ReplicaC", opt => opt.ActiveSyncEnabled = false);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManagerA = scopeA.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManagerB = scopeB.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await orchB.InitializeAsync(CancellationToken.None);

        var scopeC = nodeC.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeC.ReplicaId);
        var orchC = scopeC.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManagerC = scopeC.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await orchC.InitializeAsync(CancellationToken.None);

        foreach (var orch in new[] { orchA, orchB, orchC })
        {
            await orch.CreateDocumentAsync("doc-ring-1", "dvv-doc", CancellationToken.None);
            await orch.CreateDocumentAsync("doc-ring-2", "dvv-doc", CancellationToken.None);
        }

        // Each node applies local mutations across both documents
        await orchA.ApplyIntentAsync<DvvTestState>("doc-ring-1", new MapSetIntent("KA1", "VA1"), "DataMap", CancellationToken.None);
        await orchA.ApplyIntentAsync<DvvTestState>("doc-ring-2", new MapSetIntent("KA2", "VA2"), "DataMap", CancellationToken.None);

        await orchB.ApplyIntentAsync<DvvTestState>("doc-ring-1", new MapSetIntent("KB1", "VB1"), "DataMap", CancellationToken.None);
        await orchB.ApplyIntentAsync<DvvTestState>("doc-ring-2", new MapSetIntent("KB2", "VB2"), "DataMap", CancellationToken.None);

        await orchC.ApplyIntentAsync<DvvTestState>("doc-ring-1", new MapSetIntent("KC1", "VC1"), "DataMap", CancellationToken.None);
        await orchC.ApplyIntentAsync<DvvTestState>("doc-ring-2", new MapSetIntent("KC2", "VC2"), "DataMap", CancellationToken.None);

        var recordedMessages = new ConcurrentQueue<CapturedMessageDto>();
        using var network = WireMeshWithInspection(recordedMessages, nodeA, nodeB, nodeC);

        // Act - Perform directed ring DVV exchanges (A -> B -> C -> A -> B)
        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        await orchB.DispatchAntiEntropyStateAsync(nodeC.PeerId, CancellationToken.None);
        await orchC.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await network.DrainAsync();

        await orchC.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await orchA.DispatchAntiEntropyStateAsync(nodeC.PeerId, CancellationToken.None);
        await network.DrainAsync();

        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        await WaitForConvergenceAsync(async () =>
        {
            foreach (var orch in new[] { orchA, orchB, orchC })
            {
                var d1 = await orch.GetDocumentStateAsync<DvvTestState>("doc-ring-1", CancellationToken.None);
                var d2 = await orch.GetDocumentStateAsync<DvvTestState>("doc-ring-2", CancellationToken.None);
                if (d1.Data.DataMap.Count < 3 || d2.Data.DataMap.Count < 3)
                {
                    return false;
                }
            }

            return true;
        }, TimeSpan.FromSeconds(5));

        foreach (var dvvManager in new[] { dvvManagerA, dvvManagerB, dvvManagerC })
        {
            var snap = await dvvManager.GetFullSnapshotAsync(CancellationToken.None);
            snap["default"].Versions["ReplicaA"].ShouldBe(2);
            snap["default"].Versions["ReplicaB"].ShouldBe(2);
            snap["default"].Versions["ReplicaC"].ShouldBe(2);
            snap["default"].Dots.Values.ShouldAllBe(dots => dots.Count == 0);
        }

        // Clear recorded messages and verify 5 full-mesh rounds of DVV exchanges produce zero patches/snapshots
        recordedMessages.Clear();

        for (int round = 0; round < 5; round++)
        {
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchA.DispatchAntiEntropyStateAsync(nodeC.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeC.PeerId, CancellationToken.None);
            await orchC.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await orchC.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await network.DrainAsync();
        }

        // Assert - 5 rounds * 6 directed pairs = 30 CrdtSync messages, 0 CrdtPatch, 0 CrdtSnapshot
        var postConvergenceMessages = recordedMessages.ToList();
        postConvergenceMessages.Count.ShouldBe(30);
        postConvergenceMessages.ShouldAllBe(m => m.MessageType == "CrdtSync");
    }

    [IntegrationFact]
    public async Task TwoReplicas_OutOfOrderDotsInDvv_ShouldFillGapCollapseDots_AndNeverOscillate()
    {
        // Arrange
        var nodeA = BuildNode("ReplicaA", opt => opt.ActiveSyncEnabled = false);
        var nodeB = BuildNode("ReplicaB", opt => opt.ActiveSyncEnabled = false);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var storageA = scopeA.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManagerB = scopeB.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await orchB.InitializeAsync(CancellationToken.None);

        await orchA.CreateDocumentAsync("doc-dots", "dvv-doc", CancellationToken.None);

        await orchB.CreateDocumentAsync("doc-dots", "dvv-doc", CancellationToken.None);

        // Node A generates 3 sequential operations on shard "default" (clocks 1, 2, 3)
        await orchA.ApplyIntentAsync<DvvTestState>("doc-dots", new MapSetIntent("Op1", "V1"), "DataMap", CancellationToken.None);
        await orchA.ApplyIntentAsync<DvvTestState>("doc-dots", new MapSetIntent("Op2", "V2"), "DataMap", CancellationToken.None);
        await orchA.ApplyIntentAsync<DvvTestState>("doc-dots", new MapSetIntent("Op3", "V3"), "DataMap", CancellationToken.None);

        var allJournalOpsA = await storageA.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync();
        var docDotsOps = allJournalOpsA
            .Where(o => o.DocumentId == "doc-dots")
            .OrderBy(o => o.Operation.ShardClock)
            .Select(o => o.Operation)
            .ToList();

        docDotsOps.Count.ShouldBe(3);

        // Deliver Op 1 (clock 1) and Op 3 (clock 3) out-of-order to Node B, skipping Op 2 (clock 2)
        var docB = orchB.TryGetLocalDocument<DvvTestState>("doc-dots")!;
        await docB.ApplyRemotePatchAsync(new CrdtPatch([docDotsOps[0], docDotsOps[2]]), CancellationToken.None);

        // Verify Node B has contiguous version 1 and an isolated dot at 3
        var gappedDvvB = dvvManagerB.GetShardSnapshot("default")["default"];
        gappedDvvB.Versions["ReplicaA"].ShouldBe(1);
        gappedDvvB.Dots["ReplicaA"].ShouldContain(3L);

        var recordedMessages = new ConcurrentQueue<CapturedMessageDto>();
        using var network = WireMeshWithInspection(recordedMessages, nodeA, nodeB);

        // Act - Node B sends its gapped DVV to Node A
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        await WaitForConvergenceAsync(async () =>
        {
            var stateB = await orchB.GetDocumentStateAsync<DvvTestState>("doc-dots", CancellationToken.None);
            return stateB.Data.DataMap.ContainsKey("Op1") &&
                   stateB.Data.DataMap.ContainsKey("Op2") &&
                   stateB.Data.DataMap.ContainsKey("Op3");
        }, TimeSpan.FromSeconds(5));

        // Assert - Dot 3 collapsed into contiguous version 3 after Op 2 arrived
        var healedDvvB = dvvManagerB.GetShardSnapshot("default")["default"];
        healedDvvB.Versions["ReplicaA"].ShouldBe(3);
        (healedDvvB.Dots.TryGetValue("ReplicaA", out var remainingDots) ? remainingDots.Count : 0).ShouldBe(0);

        // Sync A -> B as well so both sides have identical registry & shard states
        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        recordedMessages.Clear();

        // Execute 5 more bidirectional DVV exchanges to prove zero oscillations
        for (int round = 0; round < 5; round++)
        {
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await network.DrainAsync();
        }

        var steadyMessages = recordedMessages.ToList();
        steadyMessages.Count.ShouldBe(10);
        steadyMessages.ShouldAllBe(m => m.MessageType == "CrdtSync");
    }

    [IntegrationFact]
    public async Task TwoReplicas_DocumentDeletionWithinSharedShard_ShouldKeepRemainingDocumentsDvvIntact_WithoutOscillations()
    {
        // Arrange - Both alpha-1 and alpha-2 share "shard-alpha" via PrefixShardIdProvider
        var nodeA = BuildNode("ReplicaA", opt => opt.ActiveSyncEnabled = false, usePrefixShardProvider: true);
        var nodeB = BuildNode("ReplicaB", opt => opt.ActiveSyncEnabled = false, usePrefixShardProvider: true);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManagerA = scopeA.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManagerB = scopeB.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await orchB.InitializeAsync(CancellationToken.None);

        foreach (var orch in new[] { orchA, orchB })
        {
            await orch.CreateDocumentAsync("alpha-1", "dvv-doc", CancellationToken.None);
            await orch.CreateDocumentAsync("alpha-2", "dvv-doc", CancellationToken.None);
        }

        await orchA.ApplyIntentAsync<DvvTestState>("alpha-1", new MapSetIntent("K1", "V1"), "DataMap", CancellationToken.None);
        await orchA.ApplyIntentAsync<DvvTestState>("alpha-2", new MapSetIntent("K2", "V2"), "DataMap", CancellationToken.None);

        var recordedMessages = new ConcurrentQueue<CapturedMessageDto>();
        using var network = WireMeshWithInspection(recordedMessages, nodeA, nodeB);

        // Initial convergence
        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        await WaitForConvergenceAsync(async () =>
        {
            var b1 = await orchB.GetDocumentStateAsync<DvvTestState>("alpha-1", CancellationToken.None);
            var b2 = await orchB.GetDocumentStateAsync<DvvTestState>("alpha-2", CancellationToken.None);
            return b1.Data.DataMap.ContainsKey("K1") && b2.Data.DataMap.ContainsKey("K2");
        }, TimeSpan.FromSeconds(5));

        // Act - Delete alpha-1 on Node A while alpha-2 remains active in "shard-alpha"
        await orchA.DeleteDocumentAsync("alpha-1", CancellationToken.None);

        // Mutate the remaining document alpha-2 on Node A
        await orchA.ApplyIntentAsync<DvvTestState>("alpha-2", new MapSetIntent("K2_Next", "V2_Next"), "DataMap", CancellationToken.None);

        // Verify "shard-alpha" DVV was NOT wiped out when alpha-1 was deleted
        var snapAfterDeleteA = await dvvManagerA.GetFullSnapshotAsync(CancellationToken.None);
        snapAfterDeleteA.ShouldContainKey("shard-alpha");
        snapAfterDeleteA["shard-alpha"].Versions["ReplicaA"].ShouldBe(3);

        // Sync deletion and new mutation to Node B
        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        await WaitForConvergenceAsync(async () =>
        {
            var deletedOnB = orchB.TryGetLocalDocument<DvvTestState>("alpha-1");
            var b2 = await orchB.GetDocumentStateAsync<DvvTestState>("alpha-2", CancellationToken.None);
            return deletedOnB == null && b2.Data.DataMap.ContainsKey("K2_Next");
        }, TimeSpan.FromSeconds(5));

        var snapAfterDeleteB = await dvvManagerB.GetFullSnapshotAsync(CancellationToken.None);
        snapAfterDeleteB.ShouldContainKey("shard-alpha");
        snapAfterDeleteB["shard-alpha"].Versions["ReplicaA"].ShouldBe(3);

        // Assert - No false-positive snapshots and zero oscillations on subsequent DVV exchanges
        recordedMessages.ShouldNotContain(m => m.MessageType == "CrdtSnapshot");
        recordedMessages.Clear();

        for (int round = 0; round < 5; round++)
        {
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await network.DrainAsync();
        }

        var steadyMessages = recordedMessages.ToList();
        steadyMessages.Count.ShouldBe(10);
        steadyMessages.ShouldAllBe(m => m.MessageType == "CrdtSync");
    }

    [IntegrationFact]
    public async Task TwoReplicas_LateJoinerEmptyNode_ShouldDiscoverRegistryFirst_ThenConvergeDataShards_WithoutOscillations()
    {
        // Arrange - Node A initializes, creates documents, mutates them, and trims one shard before Node B even knows the documents exist
        var nodeA = BuildNode("ReplicaA", opt => opt.ActiveSyncEnabled = false, usePrefixShardProvider: true);
        var nodeB = BuildNode("ReplicaB", opt => opt.ActiveSyncEnabled = false, usePrefixShardProvider: true);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var storageA = scopeA.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchB.InitializeAsync(CancellationToken.None);

        // Only Node A creates the documents initially
        await orchA.CreateDocumentAsync("alpha-late", "dvv-doc", CancellationToken.None);
        await orchA.CreateDocumentAsync("beta-late", "dvv-doc", CancellationToken.None);
        // This is here for sync wait
        await orchA.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);

        await orchA.ApplyIntentAsync<DvvTestState>("alpha-late", new MapSetIntent("LateA", "ValA"), "DataMap", CancellationToken.None);
        await orchA.ApplyIntentAsync<DvvTestState>("beta-late", new MapSetIntent("LateB", "ValB"), "DataMap", CancellationToken.None);

        // Trim shard-alpha on Node A so late joiner must recover alpha-late via snapshot and beta-late via journal patch
        await storageA.TrimAsync("shard-alpha", new Dictionary<string, long> { { "ReplicaA", 10 } }, CancellationToken.None);

        var recordedMessages = new ConcurrentQueue<CapturedMessageDto>();
        using var network = WireMeshWithInspection(recordedMessages, nodeA, nodeB);

        // Act - Bidirectional DVV exchange allows Node B to learn registry first, instantiate documents, and pull data on subsequent sync
        for (int round = 0; round < 3; round++)
        {
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            // This is here for sync wait
            await orchB.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
            await network.DrainAsync();
        }

        await WaitForConvergenceAsync(async () =>
        {
            var localAlphaB = orchB.TryGetLocalDocument<DvvTestState>("alpha-late");
            var localBetaB = orchB.TryGetLocalDocument<DvvTestState>("beta-late");
            if (localAlphaB == null || localBetaB == null)
            {
                return false;
            }

            var docAlphaB = await localAlphaB.GetDocumentAsync(CancellationToken.None);
            var docBetaB = await localBetaB.GetDocumentAsync(CancellationToken.None);
            return docAlphaB.Data.DataMap.ContainsKey("LateA") && docBetaB.Data.DataMap.ContainsKey("LateB");
        }, TimeSpan.FromSeconds(5));

        // Clear recorded messages and verify steady-state DVV exchanges produce zero oscillations
        recordedMessages.Clear();

        for (int round = 0; round < 5; round++)
        {
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await network.DrainAsync();
        }

        var steadyMessages = recordedMessages.ToList();
        steadyMessages.Count.ShouldBe(10);
        steadyMessages.ShouldAllBe(m => m.MessageType == "CrdtSync");
    }

    [IntegrationFact]
    public async Task ThreeReplicas_PeerEvictionAndAmnesiaReboot_ShouldConvergeUnderNewIdentity_WithoutOscillations()
    {
        // Arrange
        var nodeA = BuildNode("ReplicaA", opt => opt.ActiveSyncEnabled = false);
        var nodeB = BuildNode("ReplicaB", opt => opt.ActiveSyncEnabled = false);
        var nodeC = BuildNode("ReplicaC", opt => opt.ActiveSyncEnabled = false);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var evictionA = scopeA.ServiceProvider.GetRequiredService<ICrdtEvictionService>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var evictionB = scopeB.ServiceProvider.GetRequiredService<ICrdtEvictionService>();
        await orchB.InitializeAsync(CancellationToken.None);

        var scopeC = nodeC.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeC.ReplicaId);
        var orchC = scopeC.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var contextC = scopeC.ServiceProvider.GetRequiredService<ReplicaContext>();
        await orchC.InitializeAsync(CancellationToken.None);

        foreach (var orch in new[] { orchA, orchB, orchC })
        {
            await orch.CreateDocumentAsync("doc-evict-dvv", "dvv-doc", CancellationToken.None);
        }

        await orchA.ApplyIntentAsync<DvvTestState>("doc-evict-dvv", new MapSetIntent("KA", "VA"), "DataMap", CancellationToken.None);
        await orchB.ApplyIntentAsync<DvvTestState>("doc-evict-dvv", new MapSetIntent("KB", "VB"), "DataMap", CancellationToken.None);
        await orchC.ApplyIntentAsync<DvvTestState>("doc-evict-dvv", new MapSetIntent("KC_Old", "VC_Old"), "DataMap", CancellationToken.None);

        var recordedMessages = new ConcurrentQueue<CapturedMessageDto>();
        using var network = WireMeshWithInspection(recordedMessages, nodeA, nodeB, nodeC);

        // Initial convergence across A, B, C
        for (int round = 0; round < 2; round++)
        {
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeC.PeerId, CancellationToken.None);
            await orchC.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await orchC.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchA.DispatchAntiEntropyStateAsync(nodeC.PeerId, CancellationToken.None);
            await network.DrainAsync();
        }

        // Act - Node A and Node B tombstone and evict ReplicaC
        scopeA.ClusterTracker.TombstoneReplica("ReplicaC");
        scopeB.ClusterTracker.TombstoneReplica("ReplicaC");
        await evictionA.EvictPeersAsync(["ReplicaC"], CancellationToken.None);
        await evictionB.EvictPeersAsync(["ReplicaC"], CancellationToken.None);

        // Node C (unaware it was evicted) sends a DVV sync to Node A -> Node A broadcasts CrdtEviction -> Node C reboots its identity
        await orchC.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        await WaitForConvergenceAsync(
            () => Task.FromResult(!string.Equals(contextC.ReplicaId, "ReplicaC", StringComparison.Ordinal)),
            TimeSpan.FromSeconds(5));

        var newReplicaIdC = contextC.ReplicaId;
        newReplicaIdC.ShouldStartWith("ReplicaC_");

        // Re-bootstrapped Node C applies a new operation under its new identity and syncs with the cluster
        await orchC.ApplyIntentAsync<DvvTestState>("doc-evict-dvv", new MapSetIntent("KC_New", "VC_New"), "DataMap", CancellationToken.None);

        for (int round = 0; round < 3; round++)
        {
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeC.PeerId, CancellationToken.None);
            await orchC.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await orchC.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchA.DispatchAntiEntropyStateAsync(nodeC.PeerId, CancellationToken.None);
            await network.DrainAsync();
        }

        await WaitForConvergenceAsync(async () =>
        {
            foreach (var orch in new[] { orchA, orchB, orchC })
            {
                var doc = await orch.GetDocumentStateAsync<DvvTestState>("doc-evict-dvv", CancellationToken.None);
                if (!doc.Data.DataMap.ContainsKey("KA") ||
                    !doc.Data.DataMap.ContainsKey("KB") ||
                    !doc.Data.DataMap.ContainsKey("KC_New"))
                {
                    return false;
                }
            }

            return true;
        }, TimeSpan.FromSeconds(5));

        // Assert - Clear recorded messages and verify 4 full-mesh DVV exchange rounds produce zero oscillations
        recordedMessages.Clear();

        for (int round = 0; round < 4; round++)
        {
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchA.DispatchAntiEntropyStateAsync(nodeC.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeC.PeerId, CancellationToken.None);
            await orchC.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await orchC.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await network.DrainAsync();
        }

        var steadyMessages = recordedMessages.ToList();
        steadyMessages.Count.ShouldBe(24);
        steadyMessages.ShouldAllBe(m => m.MessageType == "CrdtSync");
    }

    [IntegrationFact]
    public async Task TwoReplicas_DuplicateAndStalePatchDelivery_ShouldBeIdempotent_AndNeverRegressDvvOrOscillate()
    {
        // Arrange
        var nodeA = BuildNode("ReplicaA", opt => opt.ActiveSyncEnabled = false);
        var nodeB = BuildNode("ReplicaB", opt => opt.ActiveSyncEnabled = false);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var storageA = scopeA.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManagerB = scopeB.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        await orchB.InitializeAsync(CancellationToken.None);

        await orchA.CreateDocumentAsync("doc-dup", "dvv-doc", CancellationToken.None);
        await orchB.CreateDocumentAsync("doc-dup", "dvv-doc", CancellationToken.None);

        await orchA.ApplyIntentAsync<DvvTestState>("doc-dup", new MapSetIntent("Key1", "FirstValue"), "DataMap", CancellationToken.None);
        await orchA.ApplyIntentAsync<DvvTestState>("doc-dup", new MapSetIntent("Key1", "SecondWinningValue"), "DataMap", CancellationToken.None);

        var opsA = (await storageA.GetAllJournaledOperationsAsync(CancellationToken.None).ToListAsync())
            .Where(o => o.DocumentId == "doc-dup")
            .OrderBy(o => o.Operation.ShardClock)
            .Select(o => o.Operation)
            .ToArray();

        opsA.Length.ShouldBe(2);

        var docB = orchB.TryGetLocalDocument<DvvTestState>("doc-dup")!;

        // Act - Apply full patch, then re-apply the stale first op and duplicate full patch multiple times
        await docB.ApplyRemotePatchAsync(new CrdtPatch(opsA), CancellationToken.None);
        await docB.ApplyRemotePatchAsync(new CrdtPatch([opsA[0]]), CancellationToken.None);
        await docB.ApplyRemotePatchAsync(new CrdtPatch(opsA), CancellationToken.None);

        // Assert - State on B retains the winning value and DVV remains at clock 2 with zero dots
        var stateB = await orchB.GetDocumentStateAsync<DvvTestState>("doc-dup", CancellationToken.None);
        stateB.Data.DataMap["Key1"].ShouldBe("SecondWinningValue");

        var dvvB = dvvManagerB.GetShardSnapshot("default")["default"];
        dvvB.Versions["ReplicaA"].ShouldBe(2);
        (dvvB.Dots.TryGetValue("ReplicaA", out var dots) ? dots.Count : 0).ShouldBe(0);

        var recordedMessages = new ConcurrentQueue<CapturedMessageDto>();
        using var network = WireMeshWithInspection(recordedMessages, nodeA, nodeB);

        // Sync registry first
        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
        await network.DrainAsync();

        recordedMessages.Clear();

        for (int round = 0; round < 4; round++)
        {
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await network.DrainAsync();
        }

        var steadyMessages = recordedMessages.ToList();
        steadyMessages.Count.ShouldBe(8);
        steadyMessages.ShouldAllBe(m => m.MessageType == "CrdtSync");
    }

    [IntegrationFact]
    public async Task TwoReplicas_AmnesiaRestartWithTrimmedRemoteJournal_ShouldRestoreOwnDvvFromSnapshot_AndNeverOscillateSnapshots()
    {
        // Arrange - Node A and Node B exchange operations so Node B records ReplicaA's version in its DVV,
        // then Node B trims its journal and Node A suffers amnesia (wipes local state and restarts with same ReplicaA ID).
        var nodeA = BuildNode("ReplicaA", opt => opt.ActiveSyncEnabled = false);
        var nodeB = BuildNode("ReplicaB", opt => opt.ActiveSyncEnabled = false);

        var scopeA = nodeA.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeA.ReplicaId);
        var orchA = scopeA.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManagerA = scopeA.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        var storageA = scopeA.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        await orchA.InitializeAsync(CancellationToken.None);

        var scopeB = nodeB.Sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(nodeB.ReplicaId);
        var orchB = scopeB.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var dvvManagerB = scopeB.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
        var storageB = scopeB.ServiceProvider.GetRequiredService<IDistributedCrdtStorage>();
        await orchB.InitializeAsync(CancellationToken.None);

        await orchA.CreateDocumentAsync("doc-amnesia-snap", "dvv-doc", CancellationToken.None);
        await orchB.CreateDocumentAsync("doc-amnesia-snap", "dvv-doc", CancellationToken.None);

        // Node A generates 3 mutations (ReplicaA clock = 3) and Node B generates 2 mutations (ReplicaB clock = 2)
        await orchA.ApplyIntentAsync<DvvTestState>("doc-amnesia-snap", new MapSetIntent("KA1", "VA1"), "DataMap", CancellationToken.None);
        await orchA.ApplyIntentAsync<DvvTestState>("doc-amnesia-snap", new MapSetIntent("KA2", "VA2"), "DataMap", CancellationToken.None);
        await orchA.ApplyIntentAsync<DvvTestState>("doc-amnesia-snap", new MapSetIntent("KA3", "VA3"), "DataMap", CancellationToken.None);

        await orchB.ApplyIntentAsync<DvvTestState>("doc-amnesia-snap", new MapSetIntent("KB1", "VB1"), "DataMap", CancellationToken.None);
        await orchB.ApplyIntentAsync<DvvTestState>("doc-amnesia-snap", new MapSetIntent("KB2", "VB2"), "DataMap", CancellationToken.None);

        var recordedMessages = new ConcurrentQueue<CapturedMessageDto>();
        using var network = WireMeshWithInspection(recordedMessages, nodeA, nodeB);

        // Converge both nodes initially so Node B's DVV has { ReplicaA: 3, ReplicaB: 2 }
        for (int round = 0; round < 2; round++)
        {
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await network.DrainAsync();
        }

        await WaitForConvergenceAsync(async () =>
        {
            var docB = await orchB.GetDocumentStateAsync<DvvTestState>("doc-amnesia-snap", CancellationToken.None);
            return docB.Data.DataMap.Count == 5;
        }, TimeSpan.FromSeconds(5));

        var preCrashSnapB = await dvvManagerB.GetFullSnapshotAsync(CancellationToken.None);
        preCrashSnapB["default"].Versions["ReplicaA"].ShouldBe(3);
        preCrashSnapB["default"].Versions["ReplicaB"].ShouldBe(2);

        // Trim Node B's journal so any missing operations for "default" require a full CrdtSnapshot
        await storageB.TrimAsync("default", new Dictionary<string, long> { { "ReplicaA", 10 }, { "ReplicaB", 10 } }, CancellationToken.None);

        // Simulate Node A restarting with amnesia (its local DVV for "default" resets to empty/0 while keeping ReplicaA identity)
        var docA = orchA.TryGetLocalDocument<DvvTestState>("doc-amnesia-snap")!;
        await docA.ResetLocalStateAsync("ReplicaA", CancellationToken.None);
        dvvManagerA.RemoveShard("default");
        dvvManagerA.EnsureShardRegistered("default");
        await storageA.TrimAsync("default", new Dictionary<string, long> { { "ReplicaA", 10 }, { "ReplicaB", 10 } }, CancellationToken.None);

        dvvManagerA.GetShardSnapshot("default")["default"].Versions.ContainsKey("ReplicaA").ShouldBeFalse();

        // Act - Node A dispatches its empty DVV to Node B -> Node B sends CrdtSnapshot -> Node A merges snapshot and restores its own ReplicaA: 3 clock
        recordedMessages.Clear();
        await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
        await network.DrainAsync();

        await WaitForConvergenceAsync(async () =>
        {
            var stateA = await orchA.GetDocumentStateAsync<DvvTestState>("doc-amnesia-snap", CancellationToken.None);
            return stateA.Data.DataMap.Count == 5;
        }, TimeSpan.FromSeconds(5));

        // Verify Node A restored its own ReplicaA version (3) as well as ReplicaB (2) in its DVV
        var recoveredSnapA = await dvvManagerA.GetFullSnapshotAsync(CancellationToken.None);
        recoveredSnapA["default"].Versions.ShouldContainKeyAndValue("ReplicaA", 3);
        recoveredSnapA["default"].Versions.ShouldContainKeyAndValue("ReplicaB", 2);

        // Ensure Node A saw exactly 1 CrdtSnapshot during recovery
        recordedMessages.Count(m => m.MessageType == "CrdtSnapshot" && m.DocumentId == "doc-amnesia-snap").ShouldBe(1);

        // Clear recorded messages and run 5 subsequent bidirectional DVV exchange rounds
        recordedMessages.Clear();

        for (int round = 0; round < 5; round++)
        {
            await orchA.DispatchAntiEntropyStateAsync(nodeB.PeerId, CancellationToken.None);
            await orchB.DispatchAntiEntropyStateAsync(nodeA.PeerId, CancellationToken.None);
            await network.DrainAsync();
        }

        // Assert - Zero additional CrdtSnapshot or CrdtPatch messages are sent because Node A knows its own DVV is at 3
        var steadyMessages = recordedMessages.ToList();
        steadyMessages.Count.ShouldBe(10);
        steadyMessages.ShouldAllBe(m => m.MessageType == "CrdtSync");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var node in nodes)
        {
            foreach (var hs in node.HostedServices)
            {
                await hs.StopAsync(CancellationToken.None);
            }
        }
    }

    private sealed class PrefixShardIdProvider : IShardIdProvider
    {
        public string GetShardId<T>(CrdtDocument<T> document) where T : class
        {
            if (document.Data is DvvTestState state)
            {
                return ResolveByDocumentId(state.Id);
            }

            return "default";
        }

        public string GetShardId<T>(IComparable documentId) where T : class
        {
            ArgumentNullException.ThrowIfNull(documentId);

            return ResolveByDocumentId(documentId.ToString() ?? string.Empty);
        }

        public string GetShardId(IComparable documentId, Type documentType)
        {
            ArgumentNullException.ThrowIfNull(documentId);
            ArgumentNullException.ThrowIfNull(documentType);

            return ResolveByDocumentId(documentId.ToString() ?? string.Empty);
        }

        private static string ResolveByDocumentId(string docId)
        {
            if (docId.StartsWith("alpha-", StringComparison.Ordinal))
            {
                return "shard-alpha";
            }

            if (docId.StartsWith("beta-", StringComparison.Ordinal))
            {
                return "shard-beta";
            }

            return "default";
        }
    }

    private readonly record struct CapturedMessageDto(PeerId Sender, PeerId Target, string DocumentId, string MessageType);

    private readonly record struct TestNode(
        IServiceProvider Sp,
        Mock<IDirectMessageSender> Sender,
        Mock<IP2pAlgorithm> Gossip,
        PeerId PeerId,
        string ReplicaId,
        IReadOnlyList<IHostedService> HostedServices);

    private TestNode BuildNode(string replicaId, Action<DistributedCrdtOptions>? configureOptions = null, bool usePrefixShardProvider = false)
    {
        var senderMock = new Mock<IDirectMessageSender>();
        var gossipMock = new Mock<IP2pAlgorithm>();
        var peerId = new PeerId(Guid.NewGuid());

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDistributedCrdtCore(opt =>
        {
            opt.ActiveSyncEnabled = false;
            opt.AntiEntropyInitialDelaySeconds = 3600;
            opt.AntiEntropyIntervalSeconds = 3600;
            opt.CheckpointIntervalSeconds = 3600;
            opt.MaintenanceIntervalSeconds = 3600;
            opt.PeerEvictionTtlSeconds = 0;
            opt.EnableGracefulShutdown = false;

            configureOptions?.Invoke(opt);
        });
        services.AddDistributedCrdtReplica(replicaId);

        var crdtBuilder = services.AddCrdt()
                .AddCrdtAotContext(new DvvExchangeTestAotContext())
                .AddCrdtJsonTypeInfoResolver(DvvExchangeTestJsonContext.Default);

        if (usePrefixShardProvider)
        {
            crdtBuilder.AddCrdtShardIdProvider<PrefixShardIdProvider>();
        }

        services.AddDistributedDocumentType<DvvTestState>("dvv-doc");
        services.AddDistributedCrdtP2p("TestMesh", replicaId);

        services.AddSingleton(senderMock.Object);
        services.AddSingleton(gossipMock.Object);

        var mockP2pRpcCorrelator = new Mock<IP2pRpcCorrelator>();
        mockP2pRpcCorrelator.SetReturnsDefault(Task.CompletedTask);
        services.AddSingleton(mockP2pRpcCorrelator.Object);

        var sp = services.BuildServiceProvider();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            hs.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        var node = new TestNode(sp, senderMock, gossipMock, peerId, replicaId, hostedServices);
        nodes.Add(node);
        return node;
    }

    private sealed class NetworkSimulationTracker : IDisposable
    {
        private readonly ConcurrentBag<Task> inFlightTasks = new();
        private volatile bool isDisposed;

        public void Track(Func<Task> work)
        {
            if (isDisposed)
            {
                return;
            }

            var task = Task.Run(async () =>
            {
                if (isDisposed)
                {
                    return;
                }

                await work().ConfigureAwait(false);
            });

            inFlightTasks.Add(task);
        }

        public async Task DrainAsync()
        {
            while (inFlightTasks.TryTake(out var task))
            {
                await task.ConfigureAwait(false);
            }
        }

        public void Dispose()
        {
            isDisposed = true;
        }
    }

    private static NetworkSimulationTracker WireMeshWithInspection(ConcurrentQueue<CapturedMessageDto> recordedMessages, params TestNode[] meshNodes)
    {
        ArgumentNullException.ThrowIfNull(recordedMessages);
        ArgumentNullException.ThrowIfNull(meshNodes);

        var tracker = new NetworkSimulationTracker();
        var handlers = meshNodes.ToDictionary(
            n => n.PeerId,
            n => n.Sp.GetRequiredKeyedService<IApplicationPayloadHandler>("TestMesh")
        );

        foreach (var node in meshNodes)
        {
            var serializer = node.Sp.GetRequiredService<ICrdtSerializer>();
            var others = meshNodes.Where(n => n.PeerId != node.PeerId).ToList();

            node.Sender.Setup(s => s.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns((PeerId target, ReadOnlyMemory<byte> payload, CancellationToken ct) =>
                {
                    var payloadArray = payload.ToArray();
                    var wrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(payloadArray);
                    recordedMessages.Enqueue(new CapturedMessageDto(node.PeerId, target, wrapper.DocumentId, wrapper.MessageType));

                    tracker.Track(async () =>
                    {
                        if (handlers.TryGetValue(target, out var targetHandler))
                        {
                            await targetHandler.HandlePayloadAsync("TestMesh", node.PeerId, payloadArray, CancellationToken.None);
                        }
                    });
                    return Task.CompletedTask;
                });

            node.Gossip.Setup(g => g.BroadcastAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns((ReadOnlyMemory<byte> payload, CancellationToken ct) =>
                {
                    var payloadArray = payload.ToArray();
                    var wrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(payloadArray);
                    foreach (var other in others)
                    {
                        recordedMessages.Enqueue(new CapturedMessageDto(node.PeerId, other.PeerId, wrapper.DocumentId, wrapper.MessageType));
                    }

                    tracker.Track(async () =>
                    {
                        var tasks = others.Select(other => handlers[other.PeerId].HandlePayloadAsync("TestMesh", node.PeerId, payloadArray, CancellationToken.None));
                        await Task.WhenAll(tasks);
                    });
                    return Task.CompletedTask;
                });
        }

        return tracker;
    }

    private static async Task WaitForConvergenceAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(condition);

        using var cts = new CancellationTokenSource(timeout);
        while (!cts.Token.IsCancellationRequested)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(50);
        }

        (await condition()).ShouldBeTrue("Nodes failed to converge within the allotted timeout.");
    }
}