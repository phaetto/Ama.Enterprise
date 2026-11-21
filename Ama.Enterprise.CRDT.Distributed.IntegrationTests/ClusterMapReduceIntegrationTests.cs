namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Attributes;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services.Providers;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

[CrdtAotType(typeof(ClusterMapReduceIntegrationTests.FleetTestState))]
[CrdtAotType(typeof(ClusterMapReduceIntegrationTests.DeviceTestItem))]
[CrdtAotType(typeof(Dictionary<string, ClusterMapReduceIntegrationTests.DeviceTestItem>))]
public sealed partial class ClusterMapReduceTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(ClusterMapReduceIntegrationTests.FleetTestState))]
[JsonSerializable(typeof(ClusterMapReduceIntegrationTests.DeviceTestItem))]
[JsonSerializable(typeof(CrdtDocument<ClusterMapReduceIntegrationTests.FleetTestState>))]
[JsonSerializable(typeof(ClusterMapReduceIntegrationTests.LowBatteryQueryDto))]
[JsonSerializable(typeof(ClusterMapReduceIntegrationTests.LowBatteryResultDto))]
[JsonSerializable(typeof(ClusterMapReduceIntegrationTests.BatteryStatsInputDto))]
[JsonSerializable(typeof(ClusterMapReduceIntegrationTests.BatteryStatsAccumulatorDto))]
[JsonSerializable(typeof(ClusterMapReduceIntegrationTests.PurgeOfflineCommandDto))]
[JsonSerializable(typeof(ClusterMapReduceIntegrationTests.PurgeOfflineResultDto))]
[JsonSerializable(typeof(ClusterMapReduceIntegrationTests.ConvergenceAuditInputDto))]
[JsonSerializable(typeof(ClusterMapReduceIntegrationTests.ReplicaDigestDto))]
[JsonSerializable(typeof(ClusterMapReduceIntegrationTests.ConvergenceAuditAccumulatorDto))]
[JsonSerializable(typeof(ClusterMapReduceIntegrationTests.FindDeviceInputDto))]
[JsonSerializable(typeof(ClusterMapReduceIntegrationTests.FindDeviceResultDto))]
public sealed partial class ClusterMapReduceTestJsonContext : JsonSerializerContext
{
}

/// <summary>
/// End-to-end multi-node integration tests verifying distributed Map/Reduce and Cluster Query execution
/// across sharded in-memory CRDT documents.
/// </summary>
public sealed class ClusterMapReduceIntegrationTests : IAsyncDisposable
{
    private const string MeshId = "MapReduceTestMesh";
    private const string DocTypeAlias = "test-fleet";

    public readonly record struct DeviceTestItem(string Id, bool IsOnline, int BatteryLevel) : IEquatable<DeviceTestItem>;

    public sealed class FleetTestState : IEquatable<FleetTestState>
    {
        public string Id { get; set; } = "default-fleet";

        public Dictionary<string, DeviceTestItem> Devices { get; set; } = new(StringComparer.Ordinal);

        public bool Equals(FleetTestState? other)
        {
            if (other is null)
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            if (!string.Equals(Id, other.Id, StringComparison.Ordinal) || Devices.Count != other.Devices.Count)
            {
                return false;
            }

            foreach (var kvp in Devices)
            {
                if (!other.Devices.TryGetValue(kvp.Key, out var otherVal) || !kvp.Value.Equals(otherVal))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj) => Equals(obj as FleetTestState);

        public override int GetHashCode() => HashCode.Combine(Id, Devices.Count);
    }

    public readonly record struct LowBatteryQueryDto(int MaxBattery) : IEquatable<LowBatteryQueryDto>
    {
        public bool Equals(LowBatteryQueryDto other) => MaxBattery == other.MaxBattery;

        public override int GetHashCode() => MaxBattery;
    }

    public readonly record struct LowBatteryResultDto(IList<string> MatchedDeviceKeys) : IEquatable<LowBatteryResultDto>
    {
        public bool Equals(LowBatteryResultDto other)
        {
            if (MatchedDeviceKeys is null && other.MatchedDeviceKeys is null) return true;
            if (MatchedDeviceKeys is null || other.MatchedDeviceKeys is null) return false;
            return MatchedDeviceKeys.SequenceEqual(other.MatchedDeviceKeys, StringComparer.Ordinal);
        }

        public override int GetHashCode() => MatchedDeviceKeys?.Count ?? 0;
    }

    public sealed class LowBatterySearchQueryHandler : ICrdtClusterQuery<FleetTestState, LowBatteryQueryDto, LowBatteryResultDto>
    {
        public ValueTask<LowBatteryResultDto> MapDocumentAsync(
            CrdtMapContext<FleetTestState> context,
            LowBatteryQueryDto input,
            CancellationToken cancellationToken = default)
        {
            var list = new List<string>();
            if (context.Document.Data?.Devices is not null)
            {
                foreach (var kvp in context.Document.Data.Devices)
                {
                    if (kvp.Value.BatteryLevel <= input.MaxBattery)
                    {
                        list.Add($"{context.DocumentId}:{kvp.Key}");
                    }
                }
            }

            return new ValueTask<LowBatteryResultDto>(new LowBatteryResultDto(list));
        }

        public LowBatteryResultDto Combine(LowBatteryQueryDto input, IReadOnlyList<LowBatteryResultDto> accumulators)
        {
            var combined = new List<string>();
            for (var i = 0; i < accumulators.Count; i++)
            {
                if (accumulators[i].MatchedDeviceKeys is not null)
                {
                    combined.AddRange(accumulators[i].MatchedDeviceKeys);
                }
            }

            return new LowBatteryResultDto(combined);
        }
    }

    public readonly record struct BatteryStatsInputDto(bool OnlineOnly) : IEquatable<BatteryStatsInputDto>
    {
        public bool Equals(BatteryStatsInputDto other) => OnlineOnly == other.OnlineOnly;

        public override int GetHashCode() => OnlineOnly.GetHashCode();
    }

    public readonly record struct BatteryStatsAccumulatorDto(long Count, long Sum, int Min, int Max)
        : IEquatable<BatteryStatsAccumulatorDto>
    {
        public bool Equals(BatteryStatsAccumulatorDto other)
            => Count == other.Count && Sum == other.Sum && Min == other.Min && Max == other.Max;

        public override int GetHashCode() => HashCode.Combine(Count, Sum, Min, Max);
    }

    public readonly record struct BatteryStatsReportDto(long TotalCount, double AverageBattery, int MinBattery, int MaxBattery);

    public sealed class BatteryStatsMapReduceHandler
        : ICrdtMapReduce<FleetTestState, BatteryStatsInputDto, BatteryStatsAccumulatorDto, BatteryStatsReportDto>
    {
        public ValueTask<BatteryStatsAccumulatorDto> MapDocumentAsync(
            CrdtMapContext<FleetTestState> context,
            BatteryStatsInputDto input,
            CancellationToken cancellationToken = default)
        {
            var devices = context.Document.Data?.Devices;
            if (devices is null || devices.Count == 0)
            {
                return new ValueTask<BatteryStatsAccumulatorDto>(
                    new BatteryStatsAccumulatorDto(0, 0, int.MaxValue, int.MinValue));
            }

            long count = 0;
            long sum = 0;
            var min = int.MaxValue;
            var max = int.MinValue;

            foreach (var kvp in devices)
            {
                if (input.OnlineOnly && !kvp.Value.IsOnline)
                {
                    continue;
                }

                count++;
                sum += kvp.Value.BatteryLevel;
                if (kvp.Value.BatteryLevel < min) min = kvp.Value.BatteryLevel;
                if (kvp.Value.BatteryLevel > max) max = kvp.Value.BatteryLevel;
            }

            return new ValueTask<BatteryStatsAccumulatorDto>(new BatteryStatsAccumulatorDto(count, sum, min, max));
        }

        public BatteryStatsAccumulatorDto Combine(BatteryStatsInputDto input, IReadOnlyList<BatteryStatsAccumulatorDto> accumulators)
        {
            long count = 0;
            long sum = 0;
            var min = int.MaxValue;
            var max = int.MinValue;

            for (var i = 0; i < accumulators.Count; i++)
            {
                var acc = accumulators[i];
                if (acc.Count == 0) continue;
                count += acc.Count;
                sum += acc.Sum;
                if (acc.Min < min) min = acc.Min;
                if (acc.Max > max) max = acc.Max;
            }

            return new BatteryStatsAccumulatorDto(count, sum, min, max);
        }

        public BatteryStatsReportDto Reduce(BatteryStatsInputDto input, BatteryStatsAccumulatorDto finalAccumulator)
        {
            if (finalAccumulator.Count == 0)
            {
                return new BatteryStatsReportDto(0, 0d, 0, 0);
            }

            return new BatteryStatsReportDto(
                finalAccumulator.Count,
                (double)finalAccumulator.Sum / finalAccumulator.Count,
                finalAccumulator.Min,
                finalAccumulator.Max);
        }
    }

    public readonly record struct PurgeOfflineCommandDto(int MaxBattery) : IEquatable<PurgeOfflineCommandDto>
    {
        public bool Equals(PurgeOfflineCommandDto other) => MaxBattery == other.MaxBattery;

        public override int GetHashCode() => MaxBattery;
    }

    public readonly record struct PurgeOfflineResultDto(int PurgedCount, int ModifiedDocuments)
        : IEquatable<PurgeOfflineResultDto>
    {
        public bool Equals(PurgeOfflineResultDto other)
            => PurgedCount == other.PurgedCount && ModifiedDocuments == other.ModifiedDocuments;

        public override int GetHashCode() => HashCode.Combine(PurgedCount, ModifiedDocuments);
    }

    public sealed class PurgeOfflineMapReduceHandler : ICrdtClusterQuery<FleetTestState, PurgeOfflineCommandDto, PurgeOfflineResultDto>
    {
        public async ValueTask<PurgeOfflineResultDto> MapDocumentAsync(
            CrdtMapContext<FleetTestState> context,
            PurgeOfflineCommandDto input,
            CancellationToken cancellationToken = default)
        {
            if (!context.CanMutateInPlace || context.LiveDocumentHandle is null || context.Document.Data?.Devices is null)
            {
                return new PurgeOfflineResultDto(0, 0);
            }

            var toRemove = context.Document.Data.Devices
                .Where(kvp => !kvp.Value.IsOnline && kvp.Value.BatteryLevel <= input.MaxBattery)
                .Select(kvp => kvp.Key)
                .ToList();

            if (toRemove.Count == 0)
            {
                return new PurgeOfflineResultDto(0, 0);
            }

            foreach (var key in toRemove)
            {
                await context.LiveDocumentHandle.ApplyIntentAsync(
                    new MapRemoveIntent(key),
                    nameof(FleetTestState.Devices),
                    IntentDeliveryMode.Acknowledged,
                    cancellationToken).ConfigureAwait(false);
            }

            return new PurgeOfflineResultDto(toRemove.Count, 1);
        }

        public PurgeOfflineResultDto Combine(PurgeOfflineCommandDto input, IReadOnlyList<PurgeOfflineResultDto> accumulators)
        {
            var purged = 0;
            var docs = 0;
            for (var i = 0; i < accumulators.Count; i++)
            {
                purged += accumulators[i].PurgedCount;
                docs += accumulators[i].ModifiedDocuments;
            }

            return new PurgeOfflineResultDto(purged, docs);
        }
    }

    public readonly record struct ConvergenceAuditInputDto(bool IncludeAll) : IEquatable<ConvergenceAuditInputDto>
    {
        public bool Equals(ConvergenceAuditInputDto other) => IncludeAll == other.IncludeAll;

        public override int GetHashCode() => IncludeAll.GetHashCode();
    }

    public readonly record struct ReplicaDigestDto(string DocumentId, string ReplicaId, int DeviceCount)
        : IEquatable<ReplicaDigestDto>
    {
        public bool Equals(ReplicaDigestDto other)
            => string.Equals(DocumentId, other.DocumentId, StringComparison.Ordinal) &&
               string.Equals(ReplicaId, other.ReplicaId, StringComparison.Ordinal) &&
               DeviceCount == other.DeviceCount;

        public override int GetHashCode() => HashCode.Combine(DocumentId, ReplicaId, DeviceCount);
    }

    public readonly record struct ConvergenceAuditAccumulatorDto(IList<ReplicaDigestDto> Digests)
        : IEquatable<ConvergenceAuditAccumulatorDto>
    {
        public bool Equals(ConvergenceAuditAccumulatorDto other)
        {
            if (Digests is null && other.Digests is null) return true;
            if (Digests is null || other.Digests is null) return false;
            return Digests.SequenceEqual(other.Digests);
        }

        public override int GetHashCode() => Digests?.Count ?? 0;
    }

    public sealed class ConvergenceAuditMapReduceHandler
        : ICrdtMapReduce<FleetTestState, ConvergenceAuditInputDto, ConvergenceAuditAccumulatorDto, Dictionary<string, List<ReplicaDigestDto>>>
    {
        public ValueTask<ConvergenceAuditAccumulatorDto> MapDocumentAsync(
            CrdtMapContext<FleetTestState> context,
            ConvergenceAuditInputDto input,
            CancellationToken cancellationToken = default)
        {
            var count = context.Document.Data?.Devices?.Count ?? 0;
            return new ValueTask<ConvergenceAuditAccumulatorDto>(
                new ConvergenceAuditAccumulatorDto(new[] { new ReplicaDigestDto(context.DocumentId, context.ReplicaId, count) }));
        }

        public ConvergenceAuditAccumulatorDto Combine(
            ConvergenceAuditInputDto input,
            IReadOnlyList<ConvergenceAuditAccumulatorDto> accumulators)
        {
            var list = new List<ReplicaDigestDto>();
            for (var i = 0; i < accumulators.Count; i++)
            {
                if (accumulators[i].Digests is not null)
                {
                    list.AddRange(accumulators[i].Digests);
                }
            }

            return new ConvergenceAuditAccumulatorDto(list);
        }

        public Dictionary<string, List<ReplicaDigestDto>> Reduce(
            ConvergenceAuditInputDto input,
            ConvergenceAuditAccumulatorDto finalAccumulator)
        {
            var map = new Dictionary<string, List<ReplicaDigestDto>>(StringComparer.Ordinal);
            if (finalAccumulator.Digests is not null)
            {
                foreach (var d in finalAccumulator.Digests)
                {
                    if (!map.TryGetValue(d.DocumentId, out var list))
                    {
                        list = new List<ReplicaDigestDto>();
                        map[d.DocumentId] = list;
                    }

                    list.Add(d);
                }
            }

            return map;
        }
    }

    public readonly record struct FindDeviceInputDto(string DeviceId) : IEquatable<FindDeviceInputDto>
    {
        public bool Equals(FindDeviceInputDto other) => string.Equals(DeviceId, other.DeviceId, StringComparison.Ordinal);

        public override int GetHashCode() => DeviceId?.GetHashCode(StringComparison.Ordinal) ?? 0;
    }

    public readonly record struct FindDeviceResultDto(bool Found, string? DocumentId)
        : IEquatable<FindDeviceResultDto>
    {
        public bool Equals(FindDeviceResultDto other)
            => Found == other.Found && string.Equals(DocumentId, other.DocumentId, StringComparison.Ordinal);

        public override int GetHashCode() => HashCode.Combine(Found, DocumentId);
    }

    public sealed class FindDeviceShortCircuitQueryHandler
        : ICrdtClusterQuery<FleetTestState, FindDeviceInputDto, FindDeviceResultDto>
    {
        public ValueTask<FindDeviceResultDto> MapDocumentAsync(
            CrdtMapContext<FleetTestState> context,
            FindDeviceInputDto input,
            CancellationToken cancellationToken = default)
        {
            if (context.Document.Data?.Devices is not null &&
                context.Document.Data.Devices.ContainsKey(input.DeviceId))
            {
                return new ValueTask<FindDeviceResultDto>(new FindDeviceResultDto(true, context.DocumentId));
            }

            return new ValueTask<FindDeviceResultDto>(new FindDeviceResultDto(false, null));
        }

        public FindDeviceResultDto Combine(FindDeviceInputDto input, IReadOnlyList<FindDeviceResultDto> accumulators)
        {
            for (var i = 0; i < accumulators.Count; i++)
            {
                if (accumulators[i].Found)
                {
                    return accumulators[i];
                }
            }

            return new FindDeviceResultDto(false, null);
        }

        public bool CanShortCircuit(FindDeviceInputDto input, FindDeviceResultDto currentAccumulator)
            => currentAccumulator.Found;
    }

    private sealed class PrefixShardIdProvider : IShardIdProvider
    {
        public string GetShardId<T>(CrdtDocument<T> document) where T : class
        {
            if (document.Data is FleetTestState state && !string.IsNullOrWhiteSpace(state.Id))
            {
                return ExtractShardId(state.Id);
            }

            return "default";
        }

        public string GetShardId<T>(IComparable documentId) where T : class
            => GetShardId(documentId, typeof(T));

        public string GetShardId(IComparable documentId, Type documentType)
        {
            ArgumentNullException.ThrowIfNull(documentId);
            return ExtractShardId(documentId.ToString() ?? string.Empty);
        }

        private static string ExtractShardId(string docId)
        {
            var colonIdx = docId.IndexOf(':', StringComparison.Ordinal);
            return colonIdx > 0 ? $"domain-{docId[..colonIdx]}" : $"shard-{docId}";
        }
    }

    private readonly List<TestClusterNode> nodes = new();

    [IntegrationFact]
    public async Task ClusterQuery_And_MultiStageStatisticalMapReduce_AcrossShardedCluster_CombinesAndReducesResults()
    {
        // Arrange - 3-node cluster with ReplicationFactor = 1 so shards are strictly partitioned across nodes
        using var fabric = new TestMeshFabric();
        var node1 = await CreateAndStartNodeAsync("Replica-1", replicationFactor: 1);
        var node2 = await CreateAndStartNodeAsync("Replica-2", replicationFactor: 1);
        var node3 = await CreateAndStartNodeAsync("Replica-3", replicationFactor: 1);

        await fabric.JoinNodeAsync(node1);
        await fabric.JoinNodeAsync(node2);
        await fabric.JoinNodeAsync(node3);

        var docIds = Enumerable.Range(1, 9).Select(i => $"fleet-{i}").ToArray();
        for (var i = 0; i < docIds.Length; i++)
        {
            await node1.Orchestrator.CreateDocumentAsync(docIds[i], DocTypeAlias, CancellationToken.None);
            // Each fleet gets 2 devices: one low-battery (10%) offline, one high-battery (90%) online
            await node1.Orchestrator.ApplyIntentAsync<FleetTestState>(
                docIds[i],
                new MapSetIntent("dev-low", new DeviceTestItem("dev-low", false, 10)),
                nameof(FleetTestState.Devices),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);

            await node1.Orchestrator.ApplyIntentAsync<FleetTestState>(
                docIds[i],
                new MapSetIntent("dev-high", new DeviceTestItem("dev-high", true, 90)),
                nameof(FleetTestState.Devices),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
        }

        await SyncAllNodesAsync(fabric);

        // Act 1: Run Cluster Query for low battery devices (<= 20%) from Node 1
        var queryOutcome = await node1.Orchestrator.QueryClusterAsync<FleetTestState, LowBatteryQueryDto, LowBatteryResultDto>(
            new LowBatteryQueryDto(MaxBattery: 20),
            default,
            CancellationToken.None);

        // Assert 1: All 9 fleets evaluated, 9 low-battery devices found, at most 2 remote peers contacted (1 batched RPC per peer)
        queryOutcome.TotalDocumentsTargeted.ShouldBe(9);
        (queryOutcome.LocalDocumentsEvaluated + queryOutcome.RemoteDocumentsEvaluated).ShouldBe(9);
        queryOutcome.RemotePeersContacted.ShouldBeLessThanOrEqualTo(2);
        queryOutcome.Result.MatchedDeviceKeys.Count.ShouldBe(9);

        // Act 2: Run Multi-Stage Statistical Map/Reduce (TAccumulate != TResult) across all 18 devices
        var statsOutcome = await node2.Orchestrator.ExecuteMapReduceAsync<FleetTestState, BatteryStatsInputDto, BatteryStatsAccumulatorDto, BatteryStatsReportDto>(
            new BatteryStatsInputDto(OnlineOnly: false),
            default,
            CancellationToken.None);

        // Assert 2: 18 total devices, average of 10% and 90% is 50.0%
        statsOutcome.Result.TotalCount.ShouldBe(18);
        statsOutcome.Result.AverageBattery.ShouldBe(50.0d);
        statsOutcome.Result.MinBattery.ShouldBe(10);
        statsOutcome.Result.MaxBattery.ShouldBe(90);
    }

    [IntegrationFact]
    public async Task ReadWriteMapReduce_And_AllReplicasAudit_MutatesInPlaceAndVerifiesReplicaConvergence()
    {
        // Arrange - 3-node cluster with ReplicationFactor = 2
        using var fabric = new TestMeshFabric();
        var node1 = await CreateAndStartNodeAsync("Replica-1", replicationFactor: 2);
        var node2 = await CreateAndStartNodeAsync("Replica-2", replicationFactor: 2);
        var node3 = await CreateAndStartNodeAsync("Replica-3", replicationFactor: 2);

        await fabric.JoinNodeAsync(node1);
        await fabric.JoinNodeAsync(node2);
        await fabric.JoinNodeAsync(node3);

        var docIds = Enumerable.Range(1, 6).Select(i => $"mutable-fleet-{i}").ToArray();
        foreach (var docId in docIds)
        {
            await node1.Orchestrator.CreateDocumentAsync(docId, DocTypeAlias, CancellationToken.None);
            await node1.Orchestrator.ApplyIntentAsync<FleetTestState>(
                docId,
                new MapSetIntent("offline-dead", new DeviceTestItem("offline-dead", false, 5)),
                nameof(FleetTestState.Devices),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
            await node1.Orchestrator.ApplyIntentAsync<FleetTestState>(
                docId,
                new MapSetIntent("online-good", new DeviceTestItem("online-good", true, 85)),
                nameof(FleetTestState.Devices),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
        }

        await SyncAllNodesAsync(fabric);

        // Act 1: In-place bulk purge of offline devices (<= 10%) using PrimaryReplicaOnly routing
        var purgeScope = new CrdtMapReduceScopeDto(RoutingMode: CrdtMapReduceRoutingMode.PrimaryReplicaOnly);
        var purgeOutcome = await node1.Orchestrator.QueryClusterAsync<FleetTestState, PurgeOfflineCommandDto, PurgeOfflineResultDto>(
            new PurgeOfflineCommandDto(MaxBattery: 10),
            purgeScope,
            CancellationToken.None);

        purgeOutcome.Result.PurgedCount.ShouldBe(6);
        purgeOutcome.Result.ModifiedDocuments.ShouldBe(6);

        await SyncAllNodesAsync(fabric);

        // Act 2: Run AllReplicas convergence audit to verify every replica hosting each shard has converged to 1 device
        var auditScope = new CrdtMapReduceScopeDto(RoutingMode: CrdtMapReduceRoutingMode.AllReplicas);
        var auditOutcome = await node1.Orchestrator.ExecuteMapReduceAsync<FleetTestState, ConvergenceAuditInputDto, ConvergenceAuditAccumulatorDto, Dictionary<string, List<ReplicaDigestDto>>>(
            new ConvergenceAuditInputDto(IncludeAll: true),
            auditScope,
            CancellationToken.None);

        // Assert 2: Because ReplicationFactor = 2, every document is reported by 2 distinct replicas, each having DeviceCount == 1
        auditOutcome.Result.Count.ShouldBe(6);
        foreach (var kvp in auditOutcome.Result)
        {
            kvp.Value.Count.ShouldBe(2);
            kvp.Value.All(d => d.DeviceCount == 1).ShouldBeTrue();
        }
    }

    [IntegrationFact]
    public async Task DomainAlignedSharding_And_ShortCircuit_And_ReplicaFailover_BehavesAsExpected()
    {
        // Arrange - 3-node cluster with ReplicationFactor = 2 and PrefixShardIdProvider ("productA:lang" -> "domain-productA")
        using var fabric = new TestMeshFabric();
        var node1 = await CreateAndStartNodeAsync("Replica-1", replicationFactor: 2);
        var node2 = await CreateAndStartNodeAsync("Replica-2", replicationFactor: 2);
        var node3 = await CreateAndStartNodeAsync("Replica-3", replicationFactor: 2);

        await fabric.JoinNodeAsync(node1);
        await fabric.JoinNodeAsync(node2);
        await fabric.JoinNodeAsync(node3);

        // Co-locate 4 documents on shard "domain-productA" and 4 documents on "domain-productB"
        var productADocs = new[] { "productA:en", "productA:el", "productA:de", "productA:fr" };
        var productBDocs = new[] { "productB:en", "productB:el", "productB:de", "productB:fr" };

        foreach (var docId in productADocs.Concat(productBDocs))
        {
            await node1.Orchestrator.CreateDocumentAsync(docId, DocTypeAlias, CancellationToken.None);
            await node1.Orchestrator.ApplyIntentAsync<FleetTestState>(
                docId,
                new MapSetIntent($"dev-{docId}", new DeviceTestItem($"dev-{docId}", true, 15)),
                nameof(FleetTestState.Devices),
                IntentDeliveryMode.Acknowledged,
                CancellationToken.None);
        }

        await SyncAllNodesAsync(fabric);

        // Act 1: Domain-scoped query targeting ONLY shard "domain-productA"
        var shardScope = new CrdtMapReduceScopeDto(TargetShardIds: new[] { "domain-productA" });
        var domainQueryOutcome = await node1.Orchestrator.QueryClusterAsync<FleetTestState, LowBatteryQueryDto, LowBatteryResultDto>(
            new LowBatteryQueryDto(MaxBattery: 20),
            shardScope,
            CancellationToken.None);

        // Assert 1: Only the 4 productA documents were targeted, and at most 1 peer was contacted because all 4 co-locate on 1 shard!
        domainQueryOutcome.TotalDocumentsTargeted.ShouldBe(4);
        domainQueryOutcome.RemotePeersContacted.ShouldBeLessThanOrEqualTo(1);
        domainQueryOutcome.Result.MatchedDeviceKeys.Count.ShouldBe(4);

        // Act 2: Short-circuit lookup for a device hosted locally on Node 1
        var localDocOnNode1 = node1.Orchestrator.GetActiveDocuments()
            .First(d => d.DocumentId != Constants.SystemRegistryDocumentId)
            .DocumentId;

        var shortCircuitOutcome = await node1.Orchestrator.QueryClusterAsync<FleetTestState, FindDeviceInputDto, FindDeviceResultDto>(
            new FindDeviceInputDto($"dev-{localDocOnNode1}"),
            default,
            CancellationToken.None);

        // Assert 2: Found locally and short-circuited without contacting any remote peers!
        shortCircuitOutcome.Result.Found.ShouldBeTrue();
        shortCircuitOutcome.Result.DocumentId.ShouldBe(localDocOnNode1);
        shortCircuitOutcome.WasShortCircuited.ShouldBeTrue();
        shortCircuitOutcome.RemotePeersContacted.ShouldBe(0);

        // Act 3: Isolate Node 3's network (unannounced crash) and verify cluster-wide query automatically fails over to secondary replicas
        fabric.IsolateNodeNetwork(node3);

        var failoverOutcome = await node1.Orchestrator.QueryClusterAsync<FleetTestState, LowBatteryQueryDto, LowBatteryResultDto>(
            new LowBatteryQueryDto(MaxBattery: 20),
            default,
            CancellationToken.None);

        // Assert 3: All 8 documents across productA and productB are still evaluated with 0 unreachable documents
        failoverOutcome.TotalDocumentsTargeted.ShouldBe(8);
        failoverOutcome.UnreachableDocumentIds.ShouldBeEmpty();
        failoverOutcome.Result.MatchedDeviceKeys.Count.ShouldBe(8);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var node in nodes)
        {
            foreach (var hs in node.HostedServices)
            {
                await hs.StopAsync(CancellationToken.None);
            }

            if (node.ServiceProvider is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync();
            }
            else if (node.ServiceProvider is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
    }

    private async Task<TestClusterNode> CreateAndStartNodeAsync(string replicaId, int replicationFactor)
    {
        var senderMock = new Mock<IDirectMessageSender>();
        var gossipMock = new Mock<IP2pAlgorithm>();
        var peerId = new PeerId(Guid.NewGuid());

        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            opt.EnableSharding = true;
            opt.ShardReplicationFactor = replicationFactor;
            opt.VirtualNodesPerReplica = 32;
            opt.ActiveSyncEnabled = true;
            opt.ActiveSyncDebounceMilliseconds = 0;
            opt.AntiEntropyInitialDelaySeconds = 3600;
            opt.AntiEntropyIntervalSeconds = 3600;
            opt.CheckpointIntervalSeconds = 3600;
            opt.MaintenanceIntervalSeconds = 3600;
            opt.PeerEvictionTtlSeconds = 0;
            opt.RpcTimeoutMilliseconds = 150;
            opt.RpcMaxRetries = 3;
            opt.RpcRetryDelayMilliseconds = 10;
            opt.EnableGracefulShutdown = false;
        });

        services.AddCrdt()
                .AddCrdtAotContext(new ClusterMapReduceTestAotContext())
                .AddCrdtJsonTypeInfoResolver(ClusterMapReduceTestJsonContext.Default)
                .AddCrdtSerializableType<DeviceTestItem>("test-device-item")
                .AddCrdtShardIdProvider<PrefixShardIdProvider>();

        services.AddDistributedDocumentType<FleetTestState>(DocTypeAlias);
        services.AddDistributedCrdtQuery<FleetTestState, LowBatteryQueryDto, LowBatteryResultDto, LowBatterySearchQueryHandler>("test-low-battery-query");
        services.AddDistributedCrdtMapReduce<FleetTestState, BatteryStatsInputDto, BatteryStatsAccumulatorDto, BatteryStatsReportDto, BatteryStatsMapReduceHandler>("test-battery-stats-mr");
        services.AddDistributedCrdtQuery<FleetTestState, PurgeOfflineCommandDto, PurgeOfflineResultDto, PurgeOfflineMapReduceHandler>("test-purge-offline-mr");
        services.AddDistributedCrdtMapReduce<FleetTestState, ConvergenceAuditInputDto, ConvergenceAuditAccumulatorDto, Dictionary<string, List<ReplicaDigestDto>>, ConvergenceAuditMapReduceHandler>("test-convergence-audit-mr");
        services.AddDistributedCrdtQuery<FleetTestState, FindDeviceInputDto, FindDeviceResultDto, FindDeviceShortCircuitQueryHandler>("test-find-device-sc");

        services.AddDistributedCrdtReplica(replicaId, MeshId);

        services.AddSingleton(senderMock.Object);
        services.AddSingleton(gossipMock.Object);
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        services.AddSingleton<IP2pRpcCorrelator, P2pRpcCorrelator>();
        services.AddSingleton(new P2pMeshMetadata(MeshId));

        var sp = services.BuildServiceProvider();
        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices)
        {
            await hs.StartAsync(CancellationToken.None);
        }

        var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope(replicaId);
        await scope.Orchestrator.InitializeAsync(CancellationToken.None);

        var node = new TestClusterNode(
            replicaId,
            peerId,
            sp,
            scope.Orchestrator,
            scope.ClusterTracker,
            sp.GetRequiredService<IPeerRegistry>(),
            sp.GetRequiredKeyedService<IApplicationPayloadHandler>(MeshId),
            senderMock,
            gossipMock,
            hostedServices);

        nodes.Add(node);
        return node;
    }

    private static async Task SyncAllNodesAsync(TestMeshFabric fabric)
    {
        var currentNodes = fabric.GetActiveNodes();
        for (var round = 0; round < 2; round++)
        {
            foreach (var source in currentNodes)
            {
                foreach (var target in currentNodes)
                {
                    if (source.PeerId == target.PeerId) continue;
                    await source.Orchestrator.DispatchAntiEntropyStateAsync(target.PeerId, CancellationToken.None);
                }
            }

            await fabric.DrainNetworkAsync();

            foreach (var node in currentNodes)
            {
                await node.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
            }

            await fabric.DrainNetworkAsync();
        }
    }

    private sealed record TestClusterNode(
        string ReplicaId,
        PeerId PeerId,
        IServiceProvider ServiceProvider,
        ICrdtDocumentOrchestrator Orchestrator,
        IClusterStateTracker ClusterTracker,
        IPeerRegistry PeerRegistry,
        IApplicationPayloadHandler PayloadHandler,
        Mock<IDirectMessageSender> SenderMock,
        Mock<IP2pAlgorithm> GossipMock,
        IReadOnlyList<IHostedService> HostedServices);

    private sealed class TestMeshFabric : IDisposable
    {
        private readonly ConcurrentDictionary<PeerId, TestClusterNode> activeNodes = new();
        private readonly ConcurrentDictionary<PeerId, byte> isolatedPeers = new();
        private readonly ConcurrentBag<Task> inFlightPackets = new();
        private volatile bool isDisposed;

        public IReadOnlyList<TestClusterNode> GetActiveNodes() => activeNodes.Values.ToList();

        public void IsolateNodeNetwork(TestClusterNode node)
        {
            isolatedPeers[node.PeerId] = 1;
        }

        public async Task JoinNodeAsync(TestClusterNode joiningNode)
        {
            isolatedPeers.TryRemove(joiningNode.PeerId, out _);
            WireNodeTransport(joiningNode);
            activeNodes[joiningNode.PeerId] = joiningNode;

            var allNodes = activeNodes.Values.ToList();
            var port = 7000;

            foreach (var nodeA in allNodes)
            {
                foreach (var nodeB in allNodes)
                {
                    if (nodeA.PeerId == nodeB.PeerId) continue;

                    var peerNodeB = new PeerNode(nodeB.PeerId, new TcpPeerEndpoint("127.0.0.1", ++port));
                    await nodeA.PeerRegistry.AddOrUpdatePeerAsync(MeshId, peerNodeB, PeerStatus.Active, CancellationToken.None);
                    nodeA.ClusterTracker.UpdatePeerState(nodeB.ReplicaId, nodeB.PeerId.Value.ToString(), new ShardVersionVectorMap());
                }
            }

            foreach (var node in allNodes)
            {
                await node.Orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
            }
        }

        public async Task DrainNetworkAsync()
        {
            while (inFlightPackets.TryTake(out var task))
            {
                await task.ConfigureAwait(false);
            }
        }

        public void Dispose()
        {
            isDisposed = true;
        }

        private void WireNodeTransport(TestClusterNode node)
        {
            node.SenderMock
                .Setup(s => s.SendDirectAsync(It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns((PeerId targetPeerId, ReadOnlyMemory<byte> payload, CancellationToken _) =>
                {
                    if (isDisposed || isolatedPeers.ContainsKey(node.PeerId) || isolatedPeers.ContainsKey(targetPeerId))
                    {
                        return Task.CompletedTask;
                    }

                    var copy = payload.ToArray();
                    inFlightPackets.Add(Task.Run(async () =>
                    {
                        if (!isDisposed && activeNodes.TryGetValue(targetPeerId, out var targetNode) && !isolatedPeers.ContainsKey(targetPeerId))
                        {
                            await targetNode.PayloadHandler.HandlePayloadAsync(MeshId, node.PeerId, copy, CancellationToken.None).ConfigureAwait(false);
                        }
                    }));

                    return Task.CompletedTask;
                });

            node.GossipMock
                .Setup(g => g.BroadcastAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns((ReadOnlyMemory<byte> payload, CancellationToken _) =>
                {
                    if (isDisposed || isolatedPeers.ContainsKey(node.PeerId))
                    {
                        return Task.CompletedTask;
                    }

                    var copy = payload.ToArray();
                    var recipients = activeNodes.Values
                        .Where(other => other.PeerId != node.PeerId && !isolatedPeers.ContainsKey(other.PeerId))
                        .ToList();

                    inFlightPackets.Add(Task.Run(async () =>
                    {
                        var tasks = recipients.Select(r => r.PayloadHandler.HandlePayloadAsync(MeshId, node.PeerId, copy, CancellationToken.None));
                        await Task.WhenAll(tasks).ConfigureAwait(false);
                    }));

                    return Task.CompletedTask;
                });
        }
    }
}