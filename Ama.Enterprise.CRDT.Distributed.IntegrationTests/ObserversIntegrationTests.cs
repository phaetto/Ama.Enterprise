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
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;
using Shouldly;

[CrdtAotType(typeof(ObserversIntegrationTests.ObserverTestState))]
[CrdtAotType(typeof(Dictionary<string, string>))]
public sealed partial class ObserversIntegrationTestAotContext : CrdtAotContext
{
}

[JsonSerializable(typeof(ObserversIntegrationTests.ObserverTestState))]
[JsonSerializable(typeof(CrdtDocument<ObserversIntegrationTests.ObserverTestState>))]
public sealed partial class ObserversIntegrationTestJsonContext : JsonSerializerContext
{
}

/// <summary>
/// Integration tests verifying that all observer contracts (<see cref="IDocumentStateObserver{TState}"/>,
/// <see cref="IDocumentPatchObserver"/>, <see cref="ICrdtOrchestratorObserver"/>, and <see cref="IClusterStateObserver"/>)
/// are invoked across every execution path and isolate exceptions thrown by faulty observers.
/// </summary>
public sealed class ObserversIntegrationTests
{
    public sealed class ObserverTestState : IEquatable<ObserverTestState>
    {
        public string Id { get; set; } = "obs-doc";

        public Dictionary<string, string> DataMap { get; set; } = new(StringComparer.Ordinal);

        public bool Equals(ObserverTestState? other)
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
                if (!other.DataMap.TryGetValue(kvp.Key, out var value) || !string.Equals(kvp.Value, value, StringComparison.Ordinal))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj) => Equals(obj as ObserverTestState);

        public override int GetHashCode() => HashCode.Combine(Id, DataMap.Count);
    }

    public readonly record struct RecordedPatchNotificationDto(string DocumentId, string? DocumentTypeAlias, CrdtPatch Patch);

    public sealed class RecordingDocumentStateObserver<TState> : IDocumentStateObserver<TState> where TState : class, new()
    {
        public ConcurrentQueue<string> Notifications { get; } = new();

        public Task OnStateChangedAsync(string documentId, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(documentId);
            Notifications.Enqueue(documentId);
            return Task.CompletedTask;
        }
    }

    public sealed class FaultyDocumentStateObserver<TState> : IDocumentStateObserver<TState> where TState : class, new()
    {
        public int InvocationCount;

        public Task OnStateChangedAsync(string documentId, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref InvocationCount);
            throw new InvalidOperationException("Simulated state observer failure.");
        }
    }

    public sealed class RecordingDocumentPatchObserver : IDocumentPatchObserver
    {
        public ConcurrentQueue<RecordedPatchNotificationDto> Notifications { get; } = new();

        public int FlushCount;

        public Task OnPatchGeneratedAsync(string documentId, string? documentTypeAlias, CrdtPatch patch, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(documentId);
            Notifications.Enqueue(new RecordedPatchNotificationDto(documentId, documentTypeAlias, patch));
            return Task.CompletedTask;
        }

        public Task FlushAsync(CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref FlushCount);
            return Task.CompletedTask;
        }
    }

    public sealed class FaultyDocumentPatchObserver : IDocumentPatchObserver
    {
        public int InvocationCount;

        public Task OnPatchGeneratedAsync(string documentId, string? documentTypeAlias, CrdtPatch patch, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref InvocationCount);
            throw new InvalidOperationException("Simulated patch observer failure.");
        }
    }

    public sealed class RecordingOrchestratorObserver : ICrdtOrchestratorObserver
    {
        public int TopologyChangeCount;

        public Task OnTopologyChangedAsync(CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref TopologyChangeCount);
            return Task.CompletedTask;
        }
    }

    public sealed class FaultyOrchestratorObserver : ICrdtOrchestratorObserver
    {
        public int InvocationCount;

        public Task OnTopologyChangedAsync(CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref InvocationCount);
            throw new InvalidOperationException("Simulated orchestrator observer failure.");
        }
    }

    public sealed class RecordingClusterStateObserver : IClusterStateObserver
    {
        public int TopologyChangeCount;

        public void OnClusterTopologyChanged()
        {
            Interlocked.Increment(ref TopologyChangeCount);
        }
    }

    public sealed class FaultyClusterStateObserver : IClusterStateObserver
    {
        public int InvocationCount;

        public void OnClusterTopologyChanged()
        {
            Interlocked.Increment(ref InvocationCount);
            throw new InvalidOperationException("Simulated cluster state observer failure.");
        }
    }

    [IntegrationFact]
    public async Task DocumentStateObserver_IsInvokedOnEveryDocumentLifecycleAndMutationPath_AndIsolatesFaultyObserver()
    {
        // Arrange
        var faultyObserver = new FaultyDocumentStateObserver<ObserverTestState>();
        var recordingObserver = new RecordingDocumentStateObserver<ObserverTestState>();
        var registryStateObserver = new RecordingDocumentStateObserver<CrdtRegistryState>();

        var mockStorage = new Mock<IDistributedCrdtStorage>();
        mockStorage.SetReturnsDefault(Task.CompletedTask);

        var sp = BuildNode(
            "ReplicaStateObs",
            opt =>
            {
                opt.ActiveSyncEnabled = true;
                opt.ActiveSyncDebounceMilliseconds = 0;
            },
            services =>
            {
                services.Replace(ServiceDescriptor.Scoped(_ => mockStorage.Object));
                services.AddScoped<IDocumentStateObserver<ObserverTestState>>(_ => faultyObserver);
                services.AddScoped<IDocumentStateObserver<ObserverTestState>>(_ => recordingObserver);
                services.AddScoped<IDocumentStateObserver<CrdtRegistryState>>(_ => registryStateObserver);
            });

        var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaStateObs");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var metadataManager = scope.ServiceProvider.GetRequiredService<ICrdtMetadataManager>();
        var patcher = scope.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        var applicator = scope.ServiceProvider.GetRequiredService<IAsyncCrdtApplicator>();
        var serializer = scope.ServiceProvider.GetRequiredService<ICrdtSerializer>();

        // Prepare stored document for Path 1a (InitializeAsync from primary storage)
        var storedState = new ObserverTestState { Id = "doc-primary-init" };
        storedState.DataMap["InitKey"] = "InitVal";
        var storedDoc = new CrdtDocument<ObserverTestState>(storedState, metadataManager.Initialize(storedState));
        mockStorage
            .Setup(s => s.LoadDocumentAsync<ObserverTestState>("default", CrdtRegistryEntry.GetRegistryKey("obs-doc", "doc-primary-init"), It.IsAny<CancellationToken>()))
            .ReturnsAsync(storedDoc);

        // Prepare orphaned document for Path 1b (InitializeAsync from orphaned storage fallback)
        var orphanedState = new ObserverTestState { Id = "doc-orphan-init" };
        orphanedState.DataMap["OrphanKey"] = "OrphanVal";
        var orphanedDoc = new CrdtDocument<ObserverTestState>(orphanedState, metadataManager.Initialize(orphanedState));
        mockStorage
            .Setup(s => s.LoadOrphanedDocumentAsync<ObserverTestState>("default", CrdtRegistryEntry.GetRegistryKey("obs-doc", "doc-orphan-init"), It.IsAny<CancellationToken>()))
            .ReturnsAsync(orphanedDoc);

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Path 1a: InitializeAsync when primary stored document is loaded
        await orchestrator.CreateDocumentAsync("doc-primary-init", "obs-doc", CancellationToken.None);
        recordingObserver.Notifications.ShouldContain("doc-primary-init");
        registryStateObserver.Notifications.ShouldContain(Constants.SystemRegistryDocumentId);

        // Path 1b: InitializeAsync when orphaned stored document is loaded
        await orchestrator.CreateDocumentAsync("doc-orphan-init", "obs-doc", CancellationToken.None);
        recordingObserver.Notifications.ShouldContain("doc-orphan-init");

        // Create target document for all mutation paths (no stored doc -> InitializeAsync does not fire observer on empty boot)
        await orchestrator.CreateDocumentAsync("doc-all-paths", "obs-doc", CancellationToken.None);
        var doc = orchestrator.TryGetLocalDocument<ObserverTestState>("doc-all-paths")!;
        recordingObserver.Notifications.Clear();

        // Path 2a: ApplyIntentAsync (Acknowledged)
        await orchestrator.ApplyIntentAsync<ObserverTestState>(
            "doc-all-paths",
            new MapSetIntent("K_Intent_Ack", "V1"),
            nameof(ObserverTestState.DataMap),
            IntentDeliveryMode.Acknowledged,
            CancellationToken.None);
        AssertAndClearSingleNotification(recordingObserver.Notifications, "doc-all-paths");

        // Path 2b: ApplyIntentAsync (FireAndForget + FlushAsync)
        await orchestrator.ApplyIntentAsync<ObserverTestState>(
            "doc-all-paths",
            new MapSetIntent("K_Intent_FF", "V2"),
            nameof(ObserverTestState.DataMap),
            IntentDeliveryMode.FireAndForget,
            CancellationToken.None);
        await scope.ServiceProvider.GetRequiredService<IDocumentCommandPipeline>().FlushAsync(CancellationToken.None);
        AssertAndClearSingleNotification(recordingObserver.Notifications, "doc-all-paths");

        // Rebuild a fresh scope with an open command pipeline for remaining paths
        var sp2 = BuildNode(
            "ReplicaStateObs2",
            opt =>
            {
                opt.ActiveSyncEnabled = true;
                opt.ActiveSyncDebounceMilliseconds = 0;
            },
            services =>
            {
                services.AddScoped<IDocumentStateObserver<ObserverTestState>>(_ => faultyObserver);
                services.AddScoped<IDocumentStateObserver<ObserverTestState>>(_ => recordingObserver);
            });

        var scope2 = sp2.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaStateObs2");
        var orchestrator2 = scope2.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator2.InitializeAsync(CancellationToken.None);
        await orchestrator2.CreateDocumentAsync("doc-all-paths", "obs-doc", CancellationToken.None);
        var doc2 = orchestrator2.TryGetLocalDocument<ObserverTestState>("doc-all-paths")!;
        recordingObserver.Notifications.Clear();

        // Helper to generate valid CRDT operations from a remote replica
        var remoteTemplateState = new ObserverTestState { Id = "doc-all-paths" };
        var remoteTemplateDoc = new CrdtDocument<ObserverTestState>(remoteTemplateState, metadataManager.Initialize(remoteTemplateState));
        var generatedOp1 = await patcher.GenerateOperationAsync(remoteTemplateDoc, x => x.DataMap, new MapSetIntent("K_Patch_Ack", "V3"), CancellationToken.None);
        var generatedOp2 = await patcher.GenerateOperationAsync(remoteTemplateDoc, x => x.DataMap, new MapSetIntent("K_Patch_FF", "V4"), CancellationToken.None);
        var remoteOp1 = generatedOp1 with { Id = Guid.NewGuid(), ReplicaId = "RemotePeer", ShardId = "default", ShardClock = 1 };
        var remoteOp2 = generatedOp2 with { Id = Guid.NewGuid(), ReplicaId = "RemotePeer", ShardId = "default", ShardClock = 2 };
        var journalOp = generatedOp2 with { Id = Guid.NewGuid(), ReplicaId = "JournalPeer", ShardId = "default", ShardClock = 1 };

        // Path 3a: ApplyPatchAsync (Acknowledged)
        await orchestrator2.ApplyPatchAsync<ObserverTestState>(
            "doc-all-paths",
            new CrdtPatch([generatedOp1]),
            IntentDeliveryMode.Acknowledged,
            CancellationToken.None);
        AssertAndClearSingleNotification(recordingObserver.Notifications, "doc-all-paths");

        // Path 3b: ApplyPatchAsync (FireAndForget)
        await orchestrator2.ApplyPatchAsync<ObserverTestState>(
            "doc-all-paths",
            new CrdtPatch([generatedOp2]),
            IntentDeliveryMode.FireAndForget,
            CancellationToken.None);
        await WaitForNotificationAsync(recordingObserver.Notifications, "doc-all-paths");

        // Path 4a: ApplyRemotePatchAsync (Acknowledged)
        await doc2.ApplyRemotePatchAsync(new CrdtPatch([remoteOp1]), IntentDeliveryMode.Acknowledged, CancellationToken.None);
        AssertAndClearSingleNotification(recordingObserver.Notifications, "doc-all-paths");

        // Path 4b: ApplyRemotePatchAsync (FireAndForget)
        await doc2.ApplyRemotePatchAsync(new CrdtPatch([remoteOp2]), IntentDeliveryMode.FireAndForget, CancellationToken.None);
        await WaitForNotificationAsync(recordingObserver.Notifications, "doc-all-paths");

        // Path 4c: ApplyRemotePatchAsync with bounced-back own-replica operation (filtered out -> should NOT notify)
        var bouncedOwnOp = generatedOp1 with { Id = Guid.NewGuid(), ReplicaId = "ReplicaStateObs2", ShardId = "default", ShardClock = 99 };
        await doc2.ApplyRemotePatchAsync(new CrdtPatch([bouncedOwnOp]), IntentDeliveryMode.Acknowledged, CancellationToken.None);
        recordingObserver.Notifications.ShouldBeEmpty();

        // Path 5: ApplyJournaledPatchAsync (WAL replay path)
        await doc2.ApplyJournaledPatchAsync(new CrdtPatch([journalOp]), CancellationToken.None);
        AssertAndClearSingleNotification(recordingObserver.Notifications, "doc-all-paths");

        // Path 6a: MergeSnapshotAsync (Acknowledged)
        var snapApplied = await applicator.ApplyPatchAsync(remoteTemplateDoc, new CrdtPatch([remoteOp1]));
        var snapshotBytes = serializer.SerializeToBytes(snapApplied.Document);
        await doc2.MergeSnapshotAsync(snapshotBytes, new ShardVersionVectorMap(), IntentDeliveryMode.Acknowledged, CancellationToken.None);
        AssertAndClearSingleNotification(recordingObserver.Notifications, "doc-all-paths");

        // Path 6b: MergeSnapshotAsync (FireAndForget)
        await doc2.MergeSnapshotAsync(snapshotBytes, new ShardVersionVectorMap(), IntentDeliveryMode.FireAndForget, CancellationToken.None);
        await WaitForNotificationAsync(recordingObserver.Notifications, "doc-all-paths");

        // Path 7a: EvictReplicaAsync (Acknowledged)
        await doc2.EvictReplicaAsync("RemotePeer", IntentDeliveryMode.Acknowledged, CancellationToken.None);
        AssertAndClearSingleNotification(recordingObserver.Notifications, "doc-all-paths");

        // Path 7b: EvictReplicaAsync (FireAndForget)
        await doc2.EvictReplicaAsync("JournalPeer", IntentDeliveryMode.FireAndForget, CancellationToken.None);
        await WaitForNotificationAsync(recordingObserver.Notifications, "doc-all-paths");

        // Path 8: ResetLocalStateAsync (Amnesia / Eviction identity reboot path)
        await doc2.ResetLocalStateAsync("ReplicaStateObs2", CancellationToken.None);
        AssertAndClearSingleNotification(recordingObserver.Notifications, "doc-all-paths");

        // Verify the faulty observer was hit on every single path without preventing the recording observer from receiving results
        faultyObserver.InvocationCount.ShouldBeGreaterThanOrEqualTo(12);
    }

    [IntegrationFact]
    public async Task DocumentPatchObserver_IsInvokedOnAllLocalAndDebouncedPaths_NeverOnRemoteOrJournalPaths_AndIsolatesFaults()
    {
        // Arrange - Node 1: Immediate active sync (Debounce = 0)
        var faultyPatchObserver = new FaultyDocumentPatchObserver();
        var recordingPatchObserver = new RecordingDocumentPatchObserver();

        var spImmediate = BuildNode(
            "ReplicaPatchImmediate",
            opt =>
            {
                opt.ActiveSyncEnabled = true;
                opt.ActiveSyncDebounceMilliseconds = 0;
            },
            services =>
            {
                services.AddScoped<IDocumentPatchObserver>(_ => faultyPatchObserver);
                services.AddScoped<IDocumentPatchObserver>(_ => recordingPatchObserver);
            });

        var scopeImmediate = spImmediate.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaPatchImmediate");
        var orchImmediate = scopeImmediate.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var patcher = scopeImmediate.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();
        var metadataManager = scopeImmediate.ServiceProvider.GetRequiredService<ICrdtMetadataManager>();
        var serializer = scopeImmediate.ServiceProvider.GetRequiredService<ICrdtSerializer>();

        await orchImmediate.InitializeAsync(CancellationToken.None);

        // Path 1: CreateDocumentAsync generates a patch on the system-document-registry document
        await orchImmediate.CreateDocumentAsync("patch-doc-1", "obs-doc", CancellationToken.None);
        recordingPatchObserver.Notifications.ShouldContain(n => n.DocumentId == Constants.SystemRegistryDocumentId);
        recordingPatchObserver.Notifications.Clear();

        // Path 2: ApplyIntentAsync (Acknowledged)
        await orchImmediate.ApplyIntentAsync<ObserverTestState>(
            "patch-doc-1",
            new MapSetIntent("PKey1", "PVal1"),
            nameof(ObserverTestState.DataMap),
            IntentDeliveryMode.Acknowledged,
            CancellationToken.None);
        recordingPatchObserver.Notifications.TryDequeue(out var intentAckNotif).ShouldBeTrue();
        intentAckNotif.DocumentId.ShouldBe("patch-doc-1");
        intentAckNotif.DocumentTypeAlias.ShouldBe("obs-doc");
        intentAckNotif.Patch.Operations.Count.ShouldBe(1);

        // Path 3: ApplyIntentAsync (FireAndForget)
        await orchImmediate.ApplyIntentAsync<ObserverTestState>(
            "patch-doc-1",
            new MapSetIntent("PKey2", "PVal2"),
            nameof(ObserverTestState.DataMap),
            IntentDeliveryMode.FireAndForget,
            CancellationToken.None);
        var intentFfNotif = await WaitForPatchNotificationAsync(recordingPatchObserver.Notifications, "patch-doc-1");
        intentFfNotif.Patch.Operations.Count.ShouldBe(1);

        // Path 4: ApplyPatchAsync (Acknowledged & FireAndForget)
        var docImmediate = orchImmediate.TryGetLocalDocument<ObserverTestState>("patch-doc-1")!;
        var currentDocState = await docImmediate.GetDocumentAsync(CancellationToken.None);
        var localOpAck = await patcher.GenerateOperationAsync(currentDocState, x => x.DataMap, new MapSetIntent("PKey3", "PVal3"), CancellationToken.None);
        var localOpFf = await patcher.GenerateOperationAsync(currentDocState, x => x.DataMap, new MapSetIntent("PKey4", "PVal4"), CancellationToken.None);

        await orchImmediate.ApplyPatchAsync<ObserverTestState>("patch-doc-1", new CrdtPatch([localOpAck]), IntentDeliveryMode.Acknowledged, CancellationToken.None);
        recordingPatchObserver.Notifications.TryDequeue(out var patchAckNotif).ShouldBeTrue();
        patchAckNotif.DocumentId.ShouldBe("patch-doc-1");

        await orchImmediate.ApplyPatchAsync<ObserverTestState>("patch-doc-1", new CrdtPatch([localOpFf]), IntentDeliveryMode.FireAndForget, CancellationToken.None);
        _ = await WaitForPatchNotificationAsync(recordingPatchObserver.Notifications, "patch-doc-1");

        // Negative Paths: ApplyRemotePatchAsync, ApplyJournaledPatchAsync, and MergeSnapshotAsync must NOT trigger IDocumentPatchObserver
        recordingPatchObserver.Notifications.Clear();
        var remoteOp = localOpAck with { Id = Guid.NewGuid(), ReplicaId = "RemotePeer", ShardId = "default", ShardClock = 10 };
        await docImmediate.ApplyRemotePatchAsync(new CrdtPatch([remoteOp]), IntentDeliveryMode.Acknowledged, CancellationToken.None);
        await docImmediate.ApplyJournaledPatchAsync(new CrdtPatch([remoteOp with { Id = Guid.NewGuid(), ShardClock = 11 }]), CancellationToken.None);

        var snapState = new ObserverTestState { Id = "patch-doc-1" };
        snapState.DataMap["SnapK"] = "SnapV";
        var snapDoc = new CrdtDocument<ObserverTestState>(snapState, metadataManager.Initialize(snapState));
        await docImmediate.MergeSnapshotAsync(serializer.SerializeToBytes(snapDoc), new ShardVersionVectorMap(), IntentDeliveryMode.Acknowledged, CancellationToken.None);

        recordingPatchObserver.Notifications.ShouldBeEmpty();
        faultyPatchObserver.InvocationCount.ShouldBeGreaterThanOrEqualTo(5);

        // Arrange - Node 2: Debounced active sync (Timer expiration path + explicit FlushAsync path)
        var debouncedPatchObserver = new RecordingDocumentPatchObserver();
        var spDebounced = BuildNode(
            "ReplicaPatchDebounced",
            opt =>
            {
                opt.ActiveSyncEnabled = true;
                opt.ActiveSyncDebounceMilliseconds = 120;
            },
            services =>
            {
                services.AddScoped<IDocumentPatchObserver>(_ => debouncedPatchObserver);
            });

        var scopeDebounced = spDebounced.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaPatchDebounced");
        var orchDebounced = scopeDebounced.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchDebounced.InitializeAsync(CancellationToken.None);
        await orchDebounced.CreateDocumentAsync("debounced-doc", "obs-doc", CancellationToken.None);

        // Flush registry debounce buffer first
        await orchDebounced.Registry.FlushAsync(CancellationToken.None);
        debouncedPatchObserver.Notifications.Clear();

        // Path 5: Debounce timer expiration coalesces multiple operations into a single OnPatchGeneratedAsync call
        await orchDebounced.ApplyIntentAsync<ObserverTestState>("debounced-doc", new MapSetIntent("D1", "V1"), nameof(ObserverTestState.DataMap), CancellationToken.None);
        await orchDebounced.ApplyIntentAsync<ObserverTestState>("debounced-doc", new MapSetIntent("D2", "V2"), nameof(ObserverTestState.DataMap), CancellationToken.None);
        debouncedPatchObserver.Notifications.ShouldBeEmpty();

        var timerBatched = await WaitForPatchNotificationAsync(debouncedPatchObserver.Notifications, "debounced-doc");
        timerBatched.Patch.Operations.Count.ShouldBe(2);

        // Path 6: Explicit Document.FlushAsync / CrdtShutdownCoordinator.FlushAllScopesAsync drains pending debounced operations immediately
        var docDebounced = orchDebounced.TryGetLocalDocument<ObserverTestState>("debounced-doc")!;
        await orchDebounced.ApplyIntentAsync<ObserverTestState>("debounced-doc", new MapSetIntent("D3", "V3"), nameof(ObserverTestState.DataMap), CancellationToken.None);
        await orchDebounced.ApplyIntentAsync<ObserverTestState>("debounced-doc", new MapSetIntent("D4", "V4"), nameof(ObserverTestState.DataMap), CancellationToken.None);

        await docDebounced.FlushAsync(CancellationToken.None);
        debouncedPatchObserver.Notifications.TryDequeue(out var flushedBatch).ShouldBeTrue();
        flushedBatch.DocumentId.ShouldBe("debounced-doc");
        flushedBatch.Patch.Operations.Count.ShouldBe(2);

        // Path 7: Shutdown coordinator invokes IDocumentPatchObserver.FlushAsync
        var shutdownCoordinator = spDebounced.GetRequiredService<ICrdtShutdownCoordinator>();
        await shutdownCoordinator.FlushAllScopesAsync("ObserverFlushTest", CancellationToken.None);
        debouncedPatchObserver.FlushCount.ShouldBeGreaterThanOrEqualTo(1);
    }

    [IntegrationFact]
    public async Task OrchestratorObserver_IsInvokedOnAllDocumentPromotionDemotionAndDeletionPaths_AndIsolatesFaults()
    {
        // Arrange
        var faultyOrchObserver = new FaultyOrchestratorObserver();
        var recordingOrchObserver = new RecordingOrchestratorObserver();

        bool remoteShardIsLocal = true;
        var mockDistribution = new Mock<IShardDistributionManager>();
        mockDistribution
            .Setup(d => d.IsShardLocalAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string shardId, CancellationToken _) =>
                new ValueTask<bool>(shardId == "remote-shard" ? remoteShardIsLocal : true));

        var mockShardIdProvider = new Mock<IShardIdProvider>();
        mockShardIdProvider
            .Setup(p => p.GetShardId(It.IsAny<CrdtDocument<CrdtRegistryState>>()))
            .Returns(Constants.SystemRegistryDocumentId);
        mockShardIdProvider
            .Setup(p => p.GetShardId(It.IsAny<CrdtDocument<ObserverTestState>>()))
            .Returns((CrdtDocument<ObserverTestState> d) => d.Data.Id.StartsWith("demote-", StringComparison.Ordinal) ? "remote-shard" : "local-shard");
        mockShardIdProvider
            .Setup(p => p.GetShardId<ObserverTestState>(It.IsAny<IComparable>()))
            .Returns((IComparable id) => id.ToString()!.StartsWith("demote-", StringComparison.Ordinal) ? "remote-shard" : "local-shard");
        mockShardIdProvider
            .Setup(p => p.GetShardId(It.IsAny<IComparable>(), It.IsAny<Type>()))
            .Returns((IComparable id, Type _) => id.ToString()!.StartsWith("demote-", StringComparison.Ordinal) ? "remote-shard" : "local-shard");

        var sp = BuildNode(
            "ReplicaOrchObs",
            opt => opt.ActiveSyncEnabled = false,
            services =>
            {
                services.Replace(ServiceDescriptor.Scoped(_ => mockDistribution.Object));
                services.Replace(ServiceDescriptor.Singleton(_ => mockShardIdProvider.Object));
                services.AddScoped<ICrdtOrchestratorObserver>(_ => faultyOrchObserver);
                services.AddScoped<ICrdtOrchestratorObserver>(_ => recordingOrchObserver);
            });

        var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaOrchObs");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        await orchestrator.InitializeAsync(CancellationToken.None);

        var countBefore = Volatile.Read(ref recordingOrchObserver.TopologyChangeCount);

        // Path 1: CreateDocumentAsync (Acknowledged -> PromoteSingleDocumentAsync -> ProcessPromoteDocumentInternalAsync)
        await orchestrator.CreateDocumentAsync("doc-create-ack", "obs-doc", IntentDeliveryMode.Acknowledged, CancellationToken.None);
        Volatile.Read(ref recordingOrchObserver.TopologyChangeCount).ShouldBeGreaterThan(countBefore);
        countBefore = Volatile.Read(ref recordingOrchObserver.TopologyChangeCount);

        // Path 2: CreateDocumentAsync (FireAndForget -> OrchestratorRegistryStateObserver -> ReevaluateTopologyAsync -> ProcessSyncDocumentsInternalAsync)
        await orchestrator.CreateDocumentAsync("doc-create-ff", "obs-doc", IntentDeliveryMode.FireAndForget, CancellationToken.None);
        await WaitUntilCountIncreasesAsync(() => Volatile.Read(ref recordingOrchObserver.TopologyChangeCount), countBefore);
        countBefore = Volatile.Read(ref recordingOrchObserver.TopologyChangeCount);

        // Path 3: ResolveOrPromoteLocalDocumentAsync (Lazy promotion of a previously unpromoted document)
        var lazilyPromoted = await orchestrator.ResolveOrPromoteLocalDocumentAsync("demote-lazy-doc", "obs-doc", CancellationToken.None);
        lazilyPromoted.ShouldNotBeNull();
        Volatile.Read(ref recordingOrchObserver.TopologyChangeCount).ShouldBeGreaterThan(countBefore);
        countBefore = Volatile.Read(ref recordingOrchObserver.TopologyChangeCount);

        // Path 4: ReevaluateTopologyAsync when a shard is no longer local (Demotion path)
        remoteShardIsLocal = false;
        await orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
        Volatile.Read(ref recordingOrchObserver.TopologyChangeCount).ShouldBeGreaterThan(countBefore);
        countBefore = Volatile.Read(ref recordingOrchObserver.TopologyChangeCount);

        // Path 5: ReevaluateTopologyAsync when a shard becomes local again (Promotion during sync scan)
        await orchestrator.CreateDocumentAsync("demote-registered-doc", "obs-doc", IntentDeliveryMode.Acknowledged, CancellationToken.None);
        remoteShardIsLocal = true;
        await orchestrator.ReevaluateTopologyAsync(IntentDeliveryMode.Acknowledged, CancellationToken.None);
        Volatile.Read(ref recordingOrchObserver.TopologyChangeCount).ShouldBeGreaterThan(countBefore);
        countBefore = Volatile.Read(ref recordingOrchObserver.TopologyChangeCount);

        // Path 6: DeleteDocumentAsync (Tombstone deletion removes active document and notifies observer)
        await orchestrator.DeleteDocumentAsync("doc-create-ack", "obs-doc", IntentDeliveryMode.Acknowledged, CancellationToken.None);
        Volatile.Read(ref recordingOrchObserver.TopologyChangeCount).ShouldBeGreaterThan(countBefore);

        faultyOrchObserver.InvocationCount.ShouldBe(Volatile.Read(ref recordingOrchObserver.TopologyChangeCount));
    }

    [IntegrationFact]
    public async Task ClusterStateObserver_IsInvokedOnAllClusterTrackerMutationPaths_AndTriggersOrchestratorTopologyReevaluation()
    {
        // Arrange
        var faultyClusterObserver = new FaultyClusterStateObserver();
        var recordingClusterObserver = new RecordingClusterStateObserver();
        var recordingOrchObserver = new RecordingOrchestratorObserver();

        bool shardIsLocalForNewPeer = false;
        var mockDistribution = new Mock<IShardDistributionManager>();
        mockDistribution
            .Setup(d => d.IsShardLocalAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string shardId, CancellationToken _) =>
                new ValueTask<bool>(shardId == Constants.SystemRegistryDocumentId || shardIsLocalForNewPeer));

        var sp = BuildNode(
            "ReplicaClusterObs",
            opt => opt.ActiveSyncEnabled = false,
            services =>
            {
                services.Replace(ServiceDescriptor.Scoped(_ => mockDistribution.Object));
                services.AddScoped<IClusterStateObserver>(_ => faultyClusterObserver);
                services.AddScoped<IClusterStateObserver>(_ => recordingClusterObserver);
                services.AddScoped<ICrdtOrchestratorObserver>(_ => recordingOrchObserver);
            });

        var scope = sp.GetRequiredService<DistributedCrdtScopeManager>().GetOrCreateScope("ReplicaClusterObs");
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        var tracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();

        await orchestrator.InitializeAsync(CancellationToken.None);

        // Register a document in the registry while its shard is considered remote (not promoted yet)
        await orchestrator.CreateDocumentAsync("cluster-triggered-doc", "obs-doc", IntentDeliveryMode.Acknowledged, CancellationToken.None);
        orchestrator.TryGetLocalDocument<ObserverTestState>("cluster-triggered-doc").ShouldBeNull();

        // Now flip shard locality to true so the next cluster topology change promotes "cluster-triggered-doc" via OrchestratorClusterStateObserver
        shardIsLocalForNewPeer = true;
        var initialOrchCount = Volatile.Read(ref recordingOrchObserver.TopologyChangeCount);

        var dvv = new DottedVersionVector();
        dvv.Versions["PeerA"] = 1;
        var stateMap = new ShardVersionVectorMap { { "default", dvv } };

        // Path 1: UpdatePeerState (New peer added) -> notifies IClusterStateObserver AND triggers OrchestratorClusterStateObserver
        tracker.UpdatePeerState("PeerA", "NetA", stateMap);
        Volatile.Read(ref recordingClusterObserver.TopologyChangeCount).ShouldBe(1);
        await WaitUntilCountIncreasesAsync(() => Volatile.Read(ref recordingOrchObserver.TopologyChangeCount), initialOrchCount);
        orchestrator.TryGetLocalDocument<ObserverTestState>("cluster-triggered-doc").ShouldNotBeNull();

        // Path 2: UpdatePeerState (Existing peer updated with new network session ID)
        tracker.UpdatePeerState("PeerA", "NetA_New", stateMap);
        Volatile.Read(ref recordingClusterObserver.TopologyChangeCount).ShouldBe(2);

        // Path 3: RemovePeerByNetworkId
        tracker.RemovePeerByNetworkId("NetA_New");
        Volatile.Read(ref recordingClusterObserver.TopologyChangeCount).ShouldBe(3);

        // Path 4: TombstoneReplica
        tracker.TombstoneReplica("PeerA");
        Volatile.Read(ref recordingClusterObserver.TopologyChangeCount).ShouldBe(4);

        // Path 5: TombstonePeerByNetworkId
        tracker.UpdatePeerState("PeerB", "NetB", stateMap);
        Volatile.Read(ref recordingClusterObserver.TopologyChangeCount).ShouldBe(5);

        var tombstonedReplica = tracker.TombstonePeerByNetworkId("NetB");
        tombstonedReplica.ShouldBe("PeerB");
        Volatile.Read(ref recordingClusterObserver.TopologyChangeCount).ShouldBe(6);

        // Path 6: GetAndTombstoneExpiredPeers
        tracker.UpdatePeerState("PeerC", "NetC", stateMap);
        Volatile.Read(ref recordingClusterObserver.TopologyChangeCount).ShouldBe(7);

        await Task.Delay(30);
        var expiredPeers = tracker.GetAndTombstoneExpiredPeers(TimeSpan.FromMilliseconds(5));
        expiredPeers.ShouldContain("PeerC");
        Volatile.Read(ref recordingClusterObserver.TopologyChangeCount).ShouldBe(8);

        // Verify faulty observer was invoked on all 9 cluster state transitions without blocking other observers
        faultyClusterObserver.InvocationCount.ShouldBe(8);
    }

    private static IServiceProvider BuildNode(
        string replicaId,
        Action<DistributedCrdtOptions>? configureOptions = null,
        Action<IServiceCollection>? configureExtra = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDistributedCrdtCore(opt =>
        {
            opt.CheckpointIntervalSeconds = 3600;
            opt.MaintenanceIntervalSeconds = 3600;
            opt.AntiEntropyIntervalSeconds = 3600;
            opt.AntiEntropyInitialDelaySeconds = 3600;
            opt.CompactionIntervalSeconds = 3600;
            opt.WatchAzureWebJobsShutdownFile = false;

            configureOptions?.Invoke(opt);
        });

        services.AddDistributedCrdtReplica(replicaId);

        services.AddCrdt()
                .AddCrdtAotContext(new ObserversIntegrationTestAotContext())
                .AddCrdtJsonTypeInfoResolver(ObserversIntegrationTestJsonContext.Default);

        services.AddDistributedDocumentType<ObserverTestState>("obs-doc");
        services.AddDistributedCrdtP2p("ObserverMesh", replicaId);

        services.AddSingleton(Mock.Of<IP2pAlgorithm>());
        services.AddSingleton(Mock.Of<IDirectMessageSender>());

        configureExtra?.Invoke(services);

        return services.BuildServiceProvider();
    }

    private static void AssertAndClearSingleNotification(ConcurrentQueue<string> queue, string expectedDocumentId)
    {
        queue.TryDequeue(out var actualDocumentId).ShouldBeTrue();
        actualDocumentId.ShouldBe(expectedDocumentId);
        queue.ShouldBeEmpty();
    }

    private static async Task WaitForNotificationAsync(ConcurrentQueue<string> queue, string expectedDocumentId)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!cts.Token.IsCancellationRequested)
        {
            if (queue.TryDequeue(out var docId))
            {
                docId.ShouldBe(expectedDocumentId);
                return;
            }

            await Task.Delay(15, cts.Token);
        }

        throw new TimeoutException($"Timed out waiting for state observer notification on document '{expectedDocumentId}'.");
    }

    private static async Task<RecordedPatchNotificationDto> WaitForPatchNotificationAsync(
        ConcurrentQueue<RecordedPatchNotificationDto> queue,
        string expectedDocumentId)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!cts.Token.IsCancellationRequested)
        {
            if (queue.TryDequeue(out var item) && string.Equals(item.DocumentId, expectedDocumentId, StringComparison.Ordinal))
            {
                return item;
            }

            await Task.Delay(15, cts.Token);
        }

        throw new TimeoutException($"Timed out waiting for patch observer notification on document '{expectedDocumentId}'.");
    }

    private static async Task WaitUntilCountIncreasesAsync(Func<int> getCurrentCount, int baselineCount)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!cts.Token.IsCancellationRequested)
        {
            if (getCurrentCount() > baselineCount)
            {
                return;
            }

            await Task.Delay(15, cts.Token);
        }

        getCurrentCount().ShouldBeGreaterThan(baselineCount);
    }
}