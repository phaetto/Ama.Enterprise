namespace Ama.Enterprise.CRDT.Distributed.IntegrationTests;

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Attributes;
using Ama.CRDT.Attributes.Strategies;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Models.Intents;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.Documents;
using Ama.Enterprise.CRDT.Distributed.Services.MapReduce;
using Ama.Enterprise.CRDT.Distributed.Services.Scopes;
using Ama.Enterprise.CRDT.Distributed.Services.Storage;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Shouldly;

[CrdtAotType(typeof(DocumentResidencyModeIntegrationTests.ResidencyTestState))]
[CrdtAotType(typeof(List<string>))]
[CrdtAotType(typeof(Dictionary<string, int>))]
public sealed partial class ResidencyModeTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(DocumentResidencyModeIntegrationTests.ResidencyTestState))]
[JsonSerializable(typeof(CrdtDocument<DocumentResidencyModeIntegrationTests.ResidencyTestState>))]
[JsonSerializable(typeof(DocumentResidencyModeIntegrationTests.ResidencyQueryInputDto))]
[JsonSerializable(typeof(DocumentResidencyModeIntegrationTests.ResidencyQueryResultDto))]
public sealed partial class ResidencyModeTestJsonContext : JsonSerializerContext
{
}

public sealed class DocumentResidencyModeIntegrationTests
{
    public sealed class ResidencyTestState : IEquatable<ResidencyTestState>
    {
        public string Id { get; set; } = string.Empty;

        public string Title { get; set; } = string.Empty;

        [CrdtCounterStrategy]
        public int Counter { get; set; }

        [CrdtOrSetStrategy]
        public List<string> Tags { get; set; } = new();

        [CrdtLwwMapStrategy]
        public Dictionary<string, int> Metrics { get; set; } = new(StringComparer.Ordinal);

        public bool Equals(ResidencyTestState? other)
        {
            if (other is null)
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            if (!string.Equals(Id, other.Id, StringComparison.Ordinal) ||
                !string.Equals(Title, other.Title, StringComparison.Ordinal) ||
                Counter != other.Counter ||
                Tags.Count != other.Tags.Count ||
                Metrics.Count != other.Metrics.Count)
            {
                return false;
            }

            for (int i = 0; i < Tags.Count; i++)
            {
                if (!string.Equals(Tags[i], other.Tags[i], StringComparison.Ordinal))
                {
                    return false;
                }
            }

            foreach (var kvp in Metrics)
            {
                if (!other.Metrics.TryGetValue(kvp.Key, out var otherVal) || otherVal != kvp.Value)
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj) => Equals(obj as ResidencyTestState);

        public override int GetHashCode() => HashCode.Combine(Id, Title, Counter, Tags.Count, Metrics.Count);
    }

    public readonly record struct ResidencyQueryInputDto(int MinCounter) : IEquatable<ResidencyQueryInputDto>;

    public readonly record struct ResidencyQueryResultDto(int MatchingDocuments, int TotalCounterSum, int TotalTagCount) : IEquatable<ResidencyQueryResultDto>;

    public sealed class ResidencyStateClusterQuery : ICrdtClusterQuery<ResidencyTestState, ResidencyQueryInputDto, ResidencyQueryResultDto>
    {
        public ValueTask<ResidencyQueryResultDto> MapDocumentAsync(
            CrdtMapContext<ResidencyTestState> context,
            ResidencyQueryInputDto input,
            CancellationToken cancellationToken = default)
        {
            var data = context.Data;
            if (data.Counter >= input.MinCounter)
            {
                return new ValueTask<ResidencyQueryResultDto>(new ResidencyQueryResultDto(
                    1,
                    data.Counter,
                    data.Tags.Count));
            }

            return new ValueTask<ResidencyQueryResultDto>(new ResidencyQueryResultDto(0, 0, 0));
        }

        public ResidencyQueryResultDto Combine(
            ResidencyQueryInputDto input,
            IReadOnlyList<ResidencyQueryResultDto> accumulators)
        {
            var matchingDocs = 0;
            var totalCounterSum = 0;
            var totalTagCount = 0;

            for (var i = 0; i < accumulators.Count; i++)
            {
                matchingDocs += accumulators[i].MatchingDocuments;
                totalCounterSum += accumulators[i].TotalCounterSum;
                totalTagCount += accumulators[i].TotalTagCount;
            }

            return new ResidencyQueryResultDto(matchingDocs, totalCounterSum, totalTagCount);
        }
    }

    [IntegrationFact]
    public async Task AllResidencyModes_ShouldConvergeAcrossIntentsPatchesSnapshotsCheckpointsCompactionAndMapReduce()
    {
        var modes = new[]
        {
            CrdtDocumentResidencyMode.Unpacked,
            CrdtDocumentResidencyMode.PackedMetadata,
            CrdtDocumentResidencyMode.FullySerialized
        };

        foreach (var mode in modes)
        {
            var sharedStorage = CreateMemoryStorage(out var spReplicaA, "ReplicaA", mode);
            using var providerA = spReplicaA;
            using var providerB = BuildNode("ReplicaB", mode, sharedStorage);

            var scopeA = await providerA.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScopeAsync("ReplicaA", cancellationToken: CancellationToken.None);
            var scopeB = await providerB.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScopeAsync("ReplicaB", cancellationToken: CancellationToken.None);

            var orchA = scopeA.Orchestrator;
            var orchB = scopeB.Orchestrator;

            // 1. Create & apply intents on ReplicaA
            const string docId1 = "residency-doc-1";
            const string docId2 = "residency-doc-2";

            await orchA.CreateDocumentAsync(docId1, "residency-doc", CancellationToken.None);
            await orchA.CreateDocumentAsync(docId2, "residency-doc", CancellationToken.None);

            var localDoc1A = orchA.TryGetLocalDocument<ResidencyTestState>(docId1);
            localDoc1A.ShouldNotBeNull();
            localDoc1A!.ResidencyMode.ShouldBe(mode);
            orchA.Registry.ResidencyMode.ShouldBe(CrdtDocumentResidencyMode.Unpacked);

            await orchA.ApplyIntentAsync<ResidencyTestState>(docId1, new SetIntent("Alpha"), nameof(ResidencyTestState.Title), CancellationToken.None);
            await orchA.ApplyIntentAsync<ResidencyTestState>(docId1, new IncrementIntent(15), nameof(ResidencyTestState.Counter), CancellationToken.None);
            await orchA.ApplyIntentAsync<ResidencyTestState>(docId1, new AddIntent("tag-a"), nameof(ResidencyTestState.Tags), CancellationToken.None);
            await orchA.ApplyIntentAsync<ResidencyTestState>(docId1, new AddIntent("tag-b"), nameof(ResidencyTestState.Tags), CancellationToken.None);
            await orchA.ApplyIntentAsync<ResidencyTestState>(docId1, new MapSetIntent("cpu", 80), nameof(ResidencyTestState.Metrics), CancellationToken.None);

            await orchA.ApplyIntentAsync<ResidencyTestState>(docId2, new SetIntent("Beta"), nameof(ResidencyTestState.Title), CancellationToken.None);
            await orchA.ApplyIntentAsync<ResidencyTestState>(docId2, new IncrementIntent(25), nameof(ResidencyTestState.Counter), CancellationToken.None);
            await orchA.ApplyIntentAsync<ResidencyTestState>(docId2, new AddIntent("tag-c"), nameof(ResidencyTestState.Tags), CancellationToken.None);

            // Verify ReadData() and GetDocumentDataAsync() vs ReadFullDocument()
            var data1A = await orchA.GetDocumentDataAsync<ResidencyTestState>(docId1, CancellationToken.None);
            data1A.Title.ShouldBe("Alpha");
            data1A.Counter.ShouldBe(15);
            data1A.Tags.ShouldContain("tag-a");
            data1A.Tags.ShouldContain("tag-b");
            data1A.Metrics["cpu"].ShouldBe(80);

            var fullDoc1A = localDoc1A.ReadFullDocument();
            fullDoc1A.Data.ShouldNotBeNull();
            fullDoc1A.Metadata.ShouldNotBeNull();
            fullDoc1A.Metadata!.VersionVector.ShouldContainKey("ReplicaA");

            // 2. Apply remote patch from ReplicaB onto ReplicaA
            await orchB.CreateDocumentAsync(docId1, "residency-doc", CancellationToken.None);
            await orchB.ApplyIntentAsync<ResidencyTestState>(docId1, new IncrementIntent(5), nameof(ResidencyTestState.Counter), CancellationToken.None);
            await orchB.ApplyIntentAsync<ResidencyTestState>(docId1, new AddIntent("tag-from-b"), nameof(ResidencyTestState.Tags), CancellationToken.None);

            var bOps = new List<CrdtOperation>();
            await foreach (var jOp in scopeB.Storage.GetAllJournaledOperationsAsync(CancellationToken.None))
            {
                if (string.Equals(jOp.DocumentId?.ToString(), CrdtRegistryEntry.GetRegistryKey("residency-doc", docId1), StringComparison.Ordinal) ||
                    string.Equals(jOp.DocumentId?.ToString(), docId1, StringComparison.Ordinal))
                {
                    bOps.Add(jOp.Operation);
                }
            }

            bOps.Count.ShouldBeGreaterThan(0);
            await localDoc1A.ApplyRemotePatchAsync(new CrdtPatch(bOps), CancellationToken.None);

            var mergedData1A = localDoc1A.ReadData();
            mergedData1A.Counter.ShouldBe(20);
            mergedData1A.Tags.ShouldContain("tag-from-b");

            // 3. Snapshot extraction & merge onto ReplicaB
            var snapshotDto = await localDoc1A.GetSnapshotDataAsync(CancellationToken.None);
            snapshotDto.SnapshotData.Length.ShouldBeGreaterThan(0);

            var localDoc1B = orchB.TryGetLocalDocument<ResidencyTestState>(docId1)!;
            localDoc1B.ResidencyMode.ShouldBe(mode);
            await localDoc1B.MergeSnapshotAsync(snapshotDto.SnapshotData, snapshotDto.GlobalState, CancellationToken.None);

            var data1B = localDoc1B.ReadData();
            data1B.Title.ShouldBe("Alpha");
            data1B.Counter.ShouldBe(20);
            data1B.Tags.Count.ShouldBe(3);
            data1B.Metrics["cpu"].ShouldBe(80);

            // 4. Metadata Compaction & Replica Eviction
            await localDoc1A.CompactMetadataAsync(CancellationToken.None);
            await localDoc1A.EvictReplicaAsync("EvictedReplicaId", CancellationToken.None);

            var footprint = await localDoc1A.EstimateMemoryFootprintAsync(CancellationToken.None);
            footprint.WholeDocumentBytes.ShouldBeGreaterThan(0);
            footprint.DataAndMetadataBytes.ShouldBeGreaterThan(0);

            // 5. Map/Reduce Cluster Query execution
            var queryResult = await orchA.QueryClusterAsync<ResidencyTestState, ResidencyQueryInputDto, ResidencyQueryResultDto>(
                new ResidencyQueryInputDto(MinCounter: 10),
                CancellationToken.None);

            queryResult.MatchingDocuments.ShouldBe(2);
            queryResult.TotalCounterSum.ShouldBe(45); // 20 + 25
            queryResult.TotalTagCount.ShouldBe(4);   // 3 + 1

            // 6. Checkpoint and reload in a fresh scope using the same storage
            await orchA.Registry.CheckpointAsync(CancellationToken.None);
            await localDoc1A.CheckpointAsync(CancellationToken.None);
            await orchA.TryGetLocalDocument<ResidencyTestState>(docId2)!.CheckpointAsync(CancellationToken.None);

            var reloadedScope = providerA.GetRequiredService<IDistributedCrdtScopeFactory>().CreateScope("reloaded-scope", "ReplicaA");
            using (reloadedScope)
            {
                await reloadedScope.Orchestrator.InitializeAsync(CancellationToken.None);
                var reloadedDoc1 = reloadedScope.Orchestrator.TryGetLocalDocument<ResidencyTestState>(docId1);
                reloadedDoc1.ShouldNotBeNull();
                reloadedDoc1!.ResidencyMode.ShouldBe(mode);

                var reloadedData1 = reloadedDoc1.ReadData();
                reloadedData1.Title.ShouldBe("Alpha");
                reloadedData1.Counter.ShouldBe(20);
                reloadedData1.Tags.Count.ShouldBe(3);
                reloadedData1.Metrics["cpu"].ShouldBe(80);
            }
        }
    }

    [IntegrationFact]
    public async Task PerDocumentTypeResidencyModeOverride_ShouldHonorExplicitModeOverDefaultOption()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(options =>
        {
            options.DefaultDocumentResidencyMode = CrdtDocumentResidencyMode.Unpacked;
        });

        services.AddDistributedCrdtReplica("ReplicaOverride");

        services.AddCrdt()
                .AddCrdtAotContext(new ResidencyModeTestAotContext())
                .AddCrdtJsonTypeInfoResolver(ResidencyModeTestJsonContext.Default);

        services.AddDistributedDocumentType<ResidencyTestState>("packed-alias", CrdtDocumentResidencyMode.PackedMetadata);
        services.AddDistributedDocumentType<ResidencyTestState>("serialized-alias", CrdtDocumentResidencyMode.FullySerialized);
        services.AddDistributedDocumentType<ResidencyTestState>("default-alias");

        var mockP2p = new Mock<IP2pAlgorithm>();
        mockP2p.SetReturnsDefault(Task.CompletedTask);
        services.AddSingleton(mockP2p.Object);

        using var sp = services.BuildServiceProvider();
        var scope = await sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScopeAsync("ReplicaOverride", cancellationToken: CancellationToken.None);
        var orchestrator = scope.Orchestrator;

        await orchestrator.CreateDocumentAsync("doc-packed", "packed-alias", CancellationToken.None);
        await orchestrator.CreateDocumentAsync("doc-serialized", "serialized-alias", CancellationToken.None);
        await orchestrator.CreateDocumentAsync("doc-default", "default-alias", CancellationToken.None);

        var activeDocs = orchestrator.GetActiveDocuments();
        IDistributedCrdtDocument? packedDoc = null;
        IDistributedCrdtDocument? serializedDoc = null;
        IDistributedCrdtDocument? defaultDoc = null;

        for (int i = 0; i < activeDocs.Count; i++)
        {
            var d = activeDocs[i];
            if (d.DocumentId == "doc-packed") packedDoc = d;
            else if (d.DocumentId == "doc-serialized") serializedDoc = d;
            else if (d.DocumentId == "doc-default") defaultDoc = d;
        }

        (packedDoc as IDistributedCrdtDocument<ResidencyTestState>)!.ResidencyMode.ShouldBe(CrdtDocumentResidencyMode.PackedMetadata);
        (serializedDoc as IDistributedCrdtDocument<ResidencyTestState>)!.ResidencyMode.ShouldBe(CrdtDocumentResidencyMode.FullySerialized);
        (defaultDoc as IDistributedCrdtDocument<ResidencyTestState>)!.ResidencyMode.ShouldBe(CrdtDocumentResidencyMode.Unpacked);
    }

    [IntegrationFact]
    public async Task MemoryCrdtStorage_ShouldStoreOnlySerializedByteArrays_AndNeverRetainLiveObjectReferences()
    {
        var storage = CreateMemoryStorage(out var sp, "ReplicaStorageTest", CrdtDocumentResidencyMode.Unpacked);
        using var provider = sp;

        // 1. Structural reflection check: MemoryCrdtStorage must not have any Dictionary<string, object> field holding live documents,
        // and its serializedDocuments dictionary must be Dictionary<string, byte[]>.
        var fields = typeof(MemoryCrdtStorage).GetFields(BindingFlags.Instance | BindingFlags.NonPublic);
        FieldInfo? serializedDocsField = null;

        foreach (var field in fields)
        {
            field.FieldType.ShouldNotBe(typeof(Dictionary<string, object>));
            if (field.Name == "serializedDocuments")
            {
                serializedDocsField = field;
            }
        }

        serializedDocsField.ShouldNotBeNull();
        serializedDocsField!.FieldType.ShouldBe(typeof(Dictionary<string, byte[]>));

        // 2. Behavioral mutation isolation check: Mutating a model or metadata after SaveDocumentAsync
        // or after LoadDocumentAsync must never mutate the stored bytes inside MemoryCrdtStorage.
        var originalState = new ResidencyTestState
        {
            Id = "isolated-doc",
            Title = "OriginalTitle",
            Counter = 100,
            Tags = new List<string> { "t1" },
            Metrics = new Dictionary<string, int>(StringComparer.Ordinal) { ["m1"] = 1 }
        };

        var metadata = new CrdtMetadata();
        metadata.VersionVector["ReplicaStorageTest"] = 7;
        var originalDoc = new CrdtDocument<ResidencyTestState>(originalState, metadata);

        await storage.SaveDocumentAsync("shard-1", "isolated-doc", originalDoc, CancellationToken.None);

        // Mutate the original in-memory objects after saving
        originalState.Title = "MutatedInPlaceAfterSave";
        originalState.Counter = 9999;
        originalState.Tags.Add("mutated-tag");
        metadata.VersionVector["ReplicaStorageTest"] = 999;

        // Verify stored raw bytes are non-empty and uncorrupted
        var rawBytes1 = await storage.LoadSerializedDocumentAsync("shard-1", "isolated-doc", CancellationToken.None);
        rawBytes1.ShouldNotBeNull();
        rawBytes1!.Length.ShouldBeGreaterThan(0);

        // Mutating the returned byte[] copy must also not mutate MemoryCrdtStorage's internal byte[]
        rawBytes1[0] ^= 0xFF;

        var loadedDoc1 = await storage.LoadDocumentAsync<ResidencyTestState>("shard-1", "isolated-doc", CancellationToken.None);
        loadedDoc1.ShouldNotBeNull();
        loadedDoc1!.Value.Data.ShouldNotBeSameAs(originalState);
        loadedDoc1.Value.Metadata.ShouldNotBeSameAs(metadata);
        loadedDoc1.Value.Data!.Title.ShouldBe("OriginalTitle");
        loadedDoc1.Value.Data.Counter.ShouldBe(100);
        loadedDoc1.Value.Data.Tags.Count.ShouldBe(1);
        loadedDoc1.Value.Metadata!.VersionVector["ReplicaStorageTest"].ShouldBe(7);

        // Mutating the loaded instance must not affect a subsequent load
        loadedDoc1.Value.Data.Title = "MutatedAfterLoad";
        var loadedDoc2 = await storage.LoadDocumentFromAnyShardAsync<ResidencyTestState>("isolated-doc", CancellationToken.None);
        loadedDoc2.ShouldNotBeNull();
        loadedDoc2!.Value.Data.ShouldNotBeSameAs(loadedDoc1.Value.Data);
        loadedDoc2.Value.Data!.Title.ShouldBe("OriginalTitle");
    }

    private static MemoryCrdtStorage CreateMemoryStorage(
        out ServiceProvider serviceProvider,
        string replicaId,
        CrdtDocumentResidencyMode residencyMode)
    {
        serviceProvider = BuildNode(replicaId, residencyMode);
        return (MemoryCrdtStorage)serviceProvider.GetRequiredService<IDistributedCrdtStorage>();
    }

    private static ServiceProvider BuildNode(
        string replicaId,
        CrdtDocumentResidencyMode residencyMode,
        IDistributedCrdtStorage? sharedStorage = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(options =>
        {
            options.DefaultDocumentResidencyMode = residencyMode;
            options.CompactionTtlSeconds = 60;
        });

        services.AddDistributedCrdtReplica(replicaId);

        services.AddCrdt()
                .AddCrdtAotContext(new ResidencyModeTestAotContext())
                .AddCrdtJsonTypeInfoResolver(ResidencyModeTestJsonContext.Default);

        services.AddDistributedDocumentType<ResidencyTestState>("residency-doc", residencyMode);
        services.AddDistributedCrdtQuery<ResidencyTestState, ResidencyQueryInputDto, ResidencyQueryResultDto, ResidencyStateClusterQuery>("residency-state-query");

        if (sharedStorage != null)
        {
            services.AddSingleton<IDistributedCrdtStorage>(sharedStorage);
        }
        else
        {
            services.AddSingleton<MemoryCrdtStorage>();
            services.AddSingleton<IDistributedCrdtStorage>(sp => sp.GetRequiredService<MemoryCrdtStorage>());
        }

        var mockP2p = new Mock<IP2pAlgorithm>();
        mockP2p.SetReturnsDefault(Task.CompletedTask);
        services.AddSingleton(mockP2p.Object);

        return services.BuildServiceProvider();
    }
}