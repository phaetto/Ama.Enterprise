| File Path | Description |
| --- | --- |
| `$/.editorconfig` | No description provided. |
| `$/.github/workflows/ci.yml` | Added packing and verification steps to ensure `Ama.Enterprise.CRDT.Distributed.Analyzers.dll` is bundled inside the `Ama.Enterprise.CRDT.Distributed` NuGet package alongside the MessagePack analyzer check. |
| `$/.github/workflows/codeql.yml` | No description provided. |
| `$/.github/workflows/publish-preview-to-nuget.yml` | Updated prerelease unlisting step to query the NuGet V3 Registration API for listed-only packages (excluding the newly published preview version) and capped unlisting to the last 10 versions. |
| `$/.github/workflows/publish-release-to-nuget-manual.yml` | Updated release versioning to use user-entered `major_version` and `minor_version` with an automatically generated `YYMMDD` patch number and valid `< 65536` `AssemblyVersion` build/revision numbers. |
| `$/.gitignore` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Analyzers.UnitTests/Ama.Enterprise.CRDT.Distributed.Analyzers.UnitTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.CRDT.Distributed.Analyzers.UnitTests/CrdtReplicaAndMeshConfigurationAnalyzerTests.cs` | Set `ReferenceAssemblies = ReferenceAssemblies.Net.Net80` on `CSharpAnalyzerTest` to resolve BCL types when running analyzer tests on .NET 10. |
| `$/Ama.Enterprise.CRDT.Distributed.Analyzers/Ama.Enterprise.CRDT.Distributed.Analyzers.csproj` | Added `<Features>runtime-async=on</Features>`. |
| `$/Ama.Enterprise.CRDT.Distributed.Analyzers/CrdtReplicaAndMeshConfigurationAnalyzer.cs` | Updated analyzer rules and invocation analysis to support the unified `AddDistributedCrdtReplica(replicaId, params meshIds)` and `BindCrdtReplica` APIs. |
| `$/Ama.Enterprise.CRDT.Distributed.Analyzers/IsExternalInit.cs` | Polyfill for `System.Runtime.CompilerServices.IsExternalInit` to support primary constructor `record struct` properties on `netstandard2.0`. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/Ama.Enterprise.CRDT.Distributed.IntegrationTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/AntiEntropyStateSyncIntegrationTests.cs` | Flushed `node2Orch` after fire-and-forget snapshot batch delivery in `ProcessPatchAndSnapshotBatch_WithThousandsOfPerDocumentShards_ShouldNotIncurQuadraticScalingBottleneck`. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/AssemblyInfo.cs` | Replaced obsolete `CollectionBehavior(DisableTestParallelization = true)` with `[assembly: Parallelization(Mode = ParallelizationMode.None)]`. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/ClusterMapReduceIntegrationTests.cs` | Drained in-flight demotion snapshots and flushed orchestrator pipelines in `JoinNodeAsync` and `SyncAllNodesAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/ClusterStatePersistenceIntegrationTests.cs` | Added edge-case tests verifying `UpdatePeerState` rejection for tombstoned replicas and `EvictStalePeers` handling of zero/negative TTLs. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/ClusterTopologyObserverIntegrationTests.cs` | Integration tests verifying that `IClusterStateObserver` and `ICrdtOrchestratorObserver` do not fire repeatedly or cause anti-entropy resync loops when non-local shard DVVs are evicted from memory. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/ConcurrencyIntegrationTests.cs` | Updated `CrdtDocumentOrchestrator_ConcurrentMessageDispatching_EvaluatesWithoutBlocking` to flush `IDocumentPatchObserver` and assert `Times.Between(1, expectedCount, Range.Inclusive)` to account for `CrdtPatchBroadcaster` patch coalescing. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/ConsistentHashShardDistributionManagerIntegrationTests.cs` | Added edge-case test verifying multi-mesh replica bindings aggregate and deduplicate active peers across bound meshes while ignoring unrelated meshes. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/DocumentOrchestratorIntegrationTests.cs` | Resolved each dynamic scope by `scopeId` instead of `"ReplicaA"` in `OrchestratorCommandStripePool_ShouldMultiplexConcurrentScopes_AndFlushCleanly`. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/DocumentResidencyModeIntegrationTests.cs` | Registered `MemoryCrdtStorage` and `IDistributedCrdtStorage` as singletons in `BuildNode` so all scopes created within a test node share the same in-memory storage instance. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/DvvExchangeAndOscillationIntegrationTests.cs` | Fixed race condition in `NetworkSimulationTracker.DrainAsync` and `TwoReplicas_AmnesiaRestartWithTrimmedRemoteJournal_ShouldRestoreOwnDvvFromSnapshot_AndNeverOscillateSnapshots` where in-flight response tasks on Node A were still running when simulated amnesia reset and `recordedMessages.Clear()` executed. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/DynamicShardingTopologyIntegrationTests.cs` | Drained in-flight packets and flushed orchestrator pipelines in `JoinNodeAsync` and `SyncAllNodesAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/EvictionIntegrationTests.cs` | Registered and resolved shared `MemoryCrdtStorage` via DI. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/GracefulShutdownIntegrationTests.cs` | Registered and resolved `MemoryCrdtStorage` instances via DI. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/JournalingAndSnapshottingIntegrationTests.cs` | Captured `DocumentId` values in a `Callback` before `CrdtCheckpointService` clears its reusable `batchPayload` list in the `finally` block. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/LiveMigrationIntegrationTests.cs` | Updated `WaitUntilAsync` condition in `MultiNodeP2pMeshes_LiveGossipOnMeshV1_FlowsThroughMediator_AndBroadcastsToRemoteNodeOnMeshV2_WithoutReversePollution` to wait for both initial migrated tasks to arrive on `orchestratorPureV2`. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/MultiReplicaSyncIntegrationTests.cs` | Updated `CrdtMessageWrapper` instantiations to pass the mandatory `SenderReplicaId` parameter. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/ObserversIntegrationTests.cs` | Integration tests verifying that `IDocumentStateObserver<TState>`, `IDocumentPatchObserver`, `ICrdtOrchestratorObserver`, and `IClusterStateObserver` are invoked across all execution paths and isolate exceptions thrown by faulty observers. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/ScopeMemoryOverheadIntegrationTests.cs` | Registered mocked `IDirectMessageSender` and `IP2pRpcCorrelator` in `BuildNode` to satisfy `CrdtRemoteProxyClient` dependencies during shard demotion tests. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/ScopeTopologyProviderIntegrationTests.cs` | Registered and resolved shared `MemoryCrdtStorage` instances via DI. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Ama.Enterprise.CRDT.Distributed.ShowCase.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Constants.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/DeviceStatus.cs` | Data structure representing the status of an IoT device. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/FleetMapReduceModels.cs` | Query, accumulator, report, and command DTOs for cluster-wide fleet low-battery search, statistical battery health aggregation, and in-place offline device purging. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/FleetState.cs` | Root CRDT document model representing fleet devices status. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/ShowCaseCrdtAotContext.cs` | CRDT AOT reflection context mapping types used by the showcase documents. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/ShowCaseJsonContext.cs` | Registered Map/Reduce and cluster query DTOs in the showcase AOT JSON context. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/TaskItem.cs` | Data structure representing an individual task item. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/TaskListState.cs` | Root CRDT document model representing a task list. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Program.cs` | Updated `DrawMemoryStatsAsync` to report live GC heap after collection alongside reclaimed garbage and orchestrator/string-pool overhead. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/FleetBatteryAnalyticsMapReduce.cs` | Updated `MapDocumentAsync` to read `context.Data` directly instead of `context.Document.Data`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/FleetManager.cs` | Updated `GetDevicesAsync` to use `GetDocumentDataAsync<FleetState>` and `MapDocumentAsync` to read `context.Data`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/IFleetManager.cs` | Added dedicated fire-and-forget methods `CreateFleetFireAndForgetAsync`, `SetDeviceFireAndForgetAsync`, and `RemoveDeviceFireAndForgetAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ITaskManager.cs` | Added dedicated fire-and-forget methods `CreateTaskListFireAndForgetAsync`, `SetTaskFireAndForgetAsync`, and `RemoveTaskFireAndForgetAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/PurgeOfflineDevicesMapReduce.cs` | Updated `MapDocumentAsync` to read `context.Data` directly instead of `context.Document.Data`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ShowCaseCrdtStorage.cs` | Implemented transactional `SaveSerializedDocumentBatchAsync`, routed `GetJournalCountAsync` through the persistent `readConnection` under `readLock`, and added `PRAGMA busy_timeout = 60000`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ShowCaseShardIdProvider.cs` | Updated shard ID provider to map documents deterministically across 10 logical shards (`shard-0` through `shard-9`). |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/TaskManager.cs` | Updated `GetTasksAsync` to use `GetDocumentDataAsync<TaskListState>`. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Ama.Enterprise.CRDT.Distributed.TableStorage.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Extensions/ServiceCollectionExtensions.cs` | Simplified Table Storage DI extensions to strictly register a single unified scoped storage using `TryAddSingleton` to register default AOT JSON fallbacks safely bridging implementations. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Models/CrdtTableEntity.cs` | Refactored into a static helper directly mapping chunks onto the dictionary-backed `TableEntity` avoiding reflection-heavy SDK deserialization limits ensuring AOT compatibility. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Models/TableStorageCrdtOptions.cs` | Configuration structure holding Azure Table Storage endpoints and table bindings. Added `UseBinarySerialization` option to pivot between human-readable JSON rows and cost-optimized high-performance binary structures without tight coupling to a specific format. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Services/TableStorageDistributedCrdtStorage.cs` | Implemented `LoadSerializedDocumentAsync` and `LoadSerializedDocumentFromAnyShardAsync` for binary-serialized table entities. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Constants.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/DeviceStatus.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/FleetState.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/ShowCaseCrdtAotContext.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/ShowCaseJsonContext.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/ShowCaseNodeContext.cs` | Data structure encapsulating local session boundaries tracking active role and region. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/TaskItem.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/TaskListState.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Program.cs` | Updated registry inspection in `DrawTopologyAndPeersAsync` to use `GetDataAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/FleetManager.cs` | Updated `GetDevicesAsync` to use `GetDocumentDataAsync<FleetState>`. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/IFleetManager.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/IRbacScopeAccessEvaluator.cs` | Interface for evaluating role and regional access rules for CRDT scopes and physical P2P node flows. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/ITaskManager.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/RbacMeshRoutingPolicy.cs` | Injected `IRbacScopeAccessEvaluator` via DI to evaluate scope and physical node routing rules. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/RbacScopeAccessEvaluator.cs` | Implementation of `IRbacScopeAccessEvaluator` encapsulating scope role/region parsing and authorization rules. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/RbacScopeTopologyProvider.cs` | Delegated scope role and region authorization checks to the injected `IRbacScopeAccessEvaluator` and removed `IsPeerExpectedAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/ShowCaseCrdtStorage.cs` | Implemented `LoadSerializedDocumentAsync` and `LoadSerializedDocumentFromAnyShardAsync` over SQLite `Payload` blobs. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/ShowCaseTokenValidator.cs` | Refactored token validator evaluating structured session constraints to resolve Keyed DI construction limits. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/TaskManager.cs` | Updated `GetTasksAsync` to use `GetDocumentDataAsync<TaskListState>`. |
| `$/Ama.Enterprise.CRDT.Distributed/Ama.Enterprise.CRDT.Distributed.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.CRDT.Distributed/Constants.cs` | Global constants for the distributed CRDT module, including the reserved `SystemRegistryDocumentId` (`system-document-registry`). |
| `$/Ama.Enterprise.CRDT.Distributed/Extensions/ServiceCollectionExtensions.cs` | Registered `CrdtTrimCoordinator` as a singleton per DI container instead of a process-wide static lock. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/ClusterStateSnapshotDto.cs` | Updated to map tombstoned replicas to their eviction timestamp tracking the exact eviction moment for safe cooldown expirations. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtAntiEntropyWorkLoadDto.cs` | Readonly record struct reporting busy state, channel queue depth, and estimated pending sync operations for adaptive anti-entropy scheduling. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtDocumentResidencyMode.cs` | Enum defining `Unpacked`, `PackedMetadata`, and `FullySerialized` in-memory residency representations for distributed CRDT documents. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtEvictionRejectionMessage.cs` | Message broadcasted to forcefully reject and re-bootstrap nodes that have been tombstoned by the cluster, preventing amnesia edge cases. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtIntentForwardRequestDto.cs` | Added `DocumentTypeAlias` property to support lazy document instantiation on target shards when direct RPCs outrace registry gossip. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtIntentForwardResponseDto.cs` | Data structure representing the distinct evaluated result bounds of a remote intent proxy execution mapping success statuses securely. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtJournalMode.cs` | Enum defining `Direct`, `InMemoryBuffer`, and `DiskWal` operation journaling strategies. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMapContext.cs` | Updated `CrdtMapContext<TState>` to expose `TState Data` directly alongside `ReadFullDocument()` so Map/Reduce handlers operate on domain data without unpacking metadata in `PackedMetadata` mode. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMapReduceExecutionOutcomeDto.cs` | Internal DTO returned by `ICrdtMapReduceExecutor` for a local batch of evaluated documents. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMapReduceRequestDto.cs` | P2P RPC wire request DTO for `"CrdtMapReduceReq"`. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMapReduceResponseDto.cs` | P2P RPC wire response DTO for `"CrdtMapReduceRes"`. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMapReduceResultDto.cs` | Result wrapper containing the reduced output of a distributed Map/Reduce job along with execution diagnostics. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMapReduceRoutingMode.cs` | Enum defining `MinimalShardCover`, `PrimaryReplicaOnly`, and `AllReplicas` routing modes for distributed Map/Reduce jobs. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMapReduceScopeDto.cs` | DTO defining optional target shard IDs, target document IDs, routing mode, and partial result failure tolerance for a distributed Map/Reduce job. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMessageWrapper.cs` | Made `SenderReplicaId` a mandatory positional parameter on `CrdtMessageWrapper`. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMigrationOptions.cs` | Configuration model for defining source/target replicas, type aliases, concurrency, shard partitioning, and tombstone propagation for live migrations. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMigrationStage.cs` | Enum defining the lifecycle stages (`NotStarted`, `Initializing`, `Backfilling`, `StreamingLive`, `Converged`, `Drained`) of a live CRDT migration bridge. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMigrationStatusDto.cs` | Readonly record struct reporting real-time migration metrics, queue depth, and convergence status. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtPatchMessage.cs` | Transmission model conveying replicated CRDT patches targeted asynchronously across active nodes. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtReadRequestDto.cs` | Added `DocumentTypeAlias` property to support lazy document instantiation on remote read requests. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtReadResponseDto.cs` | DTO returning the isolated serialized materialized CRDT payload natively mapped against an RPC correlation boundary. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtRegistryEntry.cs` | Pooled `DocumentId`, `TypeAlias`, and composite registry keys via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtRegistryState.cs` | Pooled the `Id` property via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtScopeContext.cs` | Scoped context model tracking `ScopeId`, `HostReplicaId`, and composite `ReplicaId` (`{hostReplicaId}@{scopeId}`). |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtShutdownFlushMode.cs` | Enum defining `JournalOnly`, `JournalAndCheckpoints`, and `FullSyncAndCheckpoint` shutdown persistence strategies. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtSnapshotDataDto.cs` | DTO representing a serialized snapshot payload, avoiding tuple usage across generic bounds. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtSnapshotMessage.cs` | Added optional `ShardDocumentIds` property to carry the sender's expected shard snapshot batch document keys. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtStateSyncMessage.cs` | Converted `CrdtStateSyncMessage` to a `sealed record` implementing `IExtensibleDistributedPayload` and `IEquatable<CrdtStateSyncMessage>`. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtOptions.cs` | Added `DefaultDocumentResidencyMode` option defaulting to `CrdtDocumentResidencyMode.Unpacked`. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtP2pJsonContext.cs` | Added `[JsonSerializable]` attributes for `CrdtMapReduceRequestDto` and `CrdtMapReduceResponseDto`. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtP2pRegistration.cs` | Configuration payload explicitly mapping a P2P mesh to a local CRDT replica for targeted topological evaluations. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtReplicaRegistration.cs` | Represents a dynamically registered Replica ID enforcing discrete CRDT multi-mesh state architectures. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtSystemAotContext.cs` | Added `ConcurrentDictionary<string, CrdtRegistryEntry>` to the AOT reflection context for the internal registry state. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtSystemJsonContext.cs` | Added `JournaledOperation` and `CrdtOperation` to the AOT JSON context to support `DiskWriteAheadBuffer` serialization. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DocumentMemoryFootprintDto.cs` | Added `DataBytes` and `MetadataBytes` properties to break down the estimated memory footprint between user data and CRDT metadata. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/IntentDeliveryMode.cs` | Enum specifying `Acknowledged` vs `FireAndForget` delivery modes when forwarding operation intents to distributed CRDT shards. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/OrchestratorInitializationStage.cs` | Enum defining `NotStarted`, `Initializing`, and `Initialized` lifecycle stages for `ICrdtDocumentOrchestrator`. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/PooledDocumentCommand.cs` | Added pre-enqueue `capturedVersion` and two-party reference counting (`PrepareExecution` / `Reset`) so background workers never race with `ExecuteAsync()` or caller pool return on `ManualResetValueTaskSourceCore`. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/PooledOrchestratorCommand.cs` | Added pre-enqueue `capturedVersion` and two-party reference counting (`PrepareExecution` / `Reset`) to prevent `ManualResetValueTaskSourceCore` token and pool reuse races under heavy concurrency. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/ScopeAccessContext.cs` | Readonly record struct carrying `ScopeId`, `PeerNetworkId`, `MeshId`, and `MessageType` for `IScopeAccessPolicy` evaluations. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/SerializedDocumentCheckpointDto.cs` | Readonly record struct representing a pre-serialized CRDT document checkpoint payload for batch persistence. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/AntiEntropy/CrdtAntiEntropyCoordinator.cs` | Populated `IsPartialShardSelection` on `CrdtStateSyncMessage` when busy shards are filtered out of an anti-entropy dispatch. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/AntiEntropy/CrdtAntiEntropyService.cs` | Updated background loop to skip busy cycles and dynamically scale the delay interval based on estimated pending sync operations and channel queue depth. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/AntiEntropy/ICrdtAntiEntropyCoordinator.cs` | Interface for coordinating anti-entropy workload evaluation, shard concurrency locks, topology announcements, and outbound snapshot dispatches. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/AntiEntropy/IShardVersionVectorManager.cs` | Added `IsCurrentContextHoldingAnyShardLock` to detect whether the current async execution flow is inside an active `ExecuteShardLockedAsync` scope. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/AntiEntropy/ShardVersionVectorManager.cs` | Implemented `IsCurrentContextHoldingAnyShardLock` using an `AsyncLocal<int>` depth counter in `ExecuteShardLockedAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Commands/DocumentCommandPipeline.cs` | Converted `DocumentCommandPipeline` into a lightweight scoped facade over `IDocumentCommandStripePool` with zero per-scope channels, background loops, or gauge registrations. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Commands/DocumentCommandStripePool.cs` | Captured `IValueTaskSource` version token prior to channel write in `EnqueueAndWaitAsync` and `FlushStripeAsync` to prevent fast worker thread race conditions. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Commands/IDocumentCommandPipeline.cs` | Added `PendingCommandCount` to expose current document pipeline queue and execution depth. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Commands/IDocumentCommandStripePool.cs` | Interface for the singleton striped document command processor. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Commands/IDocumentCommandTarget.cs` | Added `ScopeReplicaId` property to support deterministic routing of document commands across worker stripes. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Commands/IOrchestratorCommandStripePool.cs` | Interface for the singleton striped orchestrator command processor. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Commands/IOrchestratorCommandTarget.cs` | Internal interface implemented by `CrdtDocumentOrchestrator` for deterministic stripe routing and command execution. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Commands/OrchestratorCommandStripePool.cs` | Captured `IValueTaskSource` version token prior to channel write in `EnqueueAndWaitAsync` and `FlushScopeAsync` to prevent fast worker thread race conditions. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtDocumentOrchestrator.cs` | Replaced `Registry.CurrentDocument` and `Registry.GetDocumentAsync` lookups with `Registry.ReadData()` and implemented `GetDocumentDataAsync<TState>`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DistributedCrdtScopeManager.cs` | Referenced `DistributedCrdtScopeFactory.DistributedCrdtScope` in `GetOrCreateScopeAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/Buffers/CrdtMemoryEstimator.cs` | Accounted for actual rented `ArrayPool<byte>` buffer capacity and dictionary bucket slack in memory footprint estimates. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/Buffers/FullySerializedDocumentBuffer.cs` | Passed `rentedDocumentBuffer.Length` to `EstimateFullySerializedDocumentFootprint` so `WholeDocumentBytes` reflects actual `ArrayPool<byte>` bucket size. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/Buffers/ICrdtDocumentBuffer.cs` | Pluggable in-memory document buffer interface managing `Unpacked`, `PackedMetadata`, and `FullySerialized` representations and `IDisposable` pooled buffer lifecycles. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/Buffers/ICrdtMemoryEstimator.cs` | Added optional allocated pooled buffer capacity parameters to `EstimatePackedMetadataDocumentFootprint` and `EstimateFullySerializedDocumentFootprint`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/Buffers/PackedMetadataDocumentBuffer.cs` | Passed `rentedMetadataBuffer.Length` to `EstimatePackedMetadataDocumentFootprint` so `WholeDocumentBytes` reflects actual `ArrayPool<byte>` bucket size. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/Buffers/UnpackedDocumentBuffer.cs` | `ICrdtDocumentBuffer<TState>` implementation keeping the full `CrdtDocument<TState>` materialized on the heap. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/DistributedCrdtDocument.cs` | Implemented `TryPrepareCheckpoint` and `RestoreDirtyAfterFailedCheckpoint` so dirty documents can be serialized in memory and persisted in batch transactions. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/DocumentFactory.cs` | Wrapped scoped `IServiceProvider`, `IShardIdProvider`, and `IDocumentIdProvider` in `DocumentFactory<TState>` with `SystemRegistryServiceProvider` and system registry decorators so `CrdtRegistryState` operations always use `Constants.SystemRegistryDocumentId` as their `ShardId`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/DocumentFactoryRegistry.cs` | Default singleton implementation of `IDocumentFactoryRegistry` caching type-to-alias and alias-to-factory mappings across all scopes. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/IDistributedCrdtDocument.cs` | Added `TryPrepareCheckpoint` and `RestoreDirtyAfterFailedCheckpoint` to support zero-overhead batched document checkpointing. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/IDocumentFactory.cs` | Factory interface for AOT-friendly creation and deserialization of strongly-typed distributed CRDT document containers. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/IDocumentFactoryRegistry.cs` | Singleton registry interface mapping document state types and string aliases to `IDocumentFactory` instances. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/SystemRegistryDocumentIdProviderDecorator.cs` | Added lazy `ServiceDescriptor` resolution in `EnsureInnerResolved` for singleton decorator registration. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/SystemRegistryServiceProvider.cs` | Returned `this` when `IServiceProvider` is requested from `SystemRegistryServiceProvider`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Documents/SystemRegistryShardIdProviderDecorator.cs` | Removed indiscriminate `:` prefix stripping from `SystemRegistryShardIdProviderDecorator` so domain-prefixed document IDs (e.g. `productA:en`) are passed intact to custom `IShardIdProvider` implementations. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ICrdtDocumentOrchestrator.cs` | Added `GetDocumentDataAsync<TState>` to retrieve materialized domain data without unpacking CRDT metadata on local reads. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Lifecycle/CrdtCheckpointService.cs` | Updated periodic checkpointing to batch dirty document writes in chunks of 2,048 via `SaveSerializedDocumentBatchAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Lifecycle/CrdtCompactionService.cs` | Used `EnumerateActiveDocuments()` instead of `GetActiveDocuments()` to avoid allocating a full list copy of all active documents during compaction cycles. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Lifecycle/CrdtInitializationService.cs` | Triggered a compacting full GC collection after initial checkpoint loading and journal replay complete. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Lifecycle/CrdtJournalFlushService.cs` | Multiplexed WAL batch flushing across active scopes with `PendingCount > 0` instead of spawning permanent per-scope background loops. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Lifecycle/CrdtMaintenanceService.cs` | Updated maintenance checkpoint pass to use `CrdtCheckpointService.CheckpointActiveDocumentsInBatchesAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Lifecycle/CrdtShutdownCoordinator.cs` | Implemented `ShutdownToken` and `IDisposable` to signal cancellation immediately upon shutdown initiation. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Lifecycle/CrdtShutdownService.cs` | Updated `StartAsync` and `StopAsync` to idempotently initialize shutdown signal handlers and execute the scope flush pipeline when running in AOT slim or `IHostedService`-only contexts where `IHostedLifecycleService` methods (`StartingAsync`/`StoppingAsync`) are not invoked. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Lifecycle/DistributedCrdtTelemetry.cs` | Made `IMeterFactory?` optional with a fallback to `new Meter(...)`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Lifecycle/ICrdtShutdownCoordinator.cs` | Added `ShutdownToken` to expose a cancellation token triggered as soon as a shutdown or scope flush begins. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Lifecycle/IDistributedCrdtTelemetry.cs` | Added `RecordOrchestratorMapReduceExecuted` to track distributed Map/Reduce and cluster query executions. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/MapReduce/CrdtMapReduceCoordinator.cs` | Updated local document and registry evaluations to use `ReadData()` instead of `CurrentDocument` to avoid unpacking metadata during Map/Reduce jobs. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/MapReduce/CrdtMapReduceExecutor.cs` | Updated local batch evaluation to pass `liveDoc.ReadData()` into `CrdtMapContext<TState>` instead of unpacking `CurrentDocument`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/MapReduce/ICrdtClusterQuery.cs` | Specialization of `ICrdtMapReduce` where the intermediate accumulator and final result types are identical. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/MapReduce/ICrdtMapReduce.cs` | Core interface for distributed Map -> Combine -> Reduce computations over in-memory CRDT documents. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/MapReduce/ICrdtMapReduceCoordinator.cs` | Interface for coordinating distributed Map/Reduce jobs and cluster queries across local and remote CRDT shards. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/MapReduce/ICrdtMapReduceExecutor.cs` | Internal non-generic AOT execution adapter interface for keyed DI resolution of Map/Reduce handlers. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Migrations/CrdtMigrationBridgeService.cs` | Replaced `registryDoc.GetDocumentAsync` calls with `registryDoc.GetDataAsync` when scanning the source registry. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Migrations/CrdtMigrationManager.cs` | Default implementation of `ICrdtMigrationManager` aggregating status and drain operations across all registered migration bridges. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Migrations/ICrdtDocumentMigrator.cs` | Added `previousSourceDocument` parameter to `MigrateDocumentAsync` to support three-way merging during live migrations. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Migrations/ICrdtMigrationBridge.cs` | Interface for inspecting status, enqueueing live document changes, and draining a specific migration bridge. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Migrations/ICrdtMigrationManager.cs` | Coordinator interface for checking cluster-wide migration convergence and draining all active migration bridges before cutover. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Observers/IClusterStateObserver.cs` | Observer contract for tracking topological state modifications directly within the CRDT Cluster State Tracker. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Observers/ICrdtOrchestratorObserver.cs` | Observer interface tracking global document matrix bounding topologies dynamically tracking generic promotions and explicit structural tombstones natively isolating logic seamlessly smoothly exactly smoothly intelligently. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Observers/IDocumentPatchObserver.cs` | Added default `FlushAsync` method to `IDocumentPatchObserver`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Observers/IDocumentStateObserver.cs` | Generic structural evaluation observer securely signaling document modifications bypassing legacy rigid event memory leak vulnerabilities explicitly efficiently safely gracefully successfully exactly securely. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Observers/MigrationSourceStateObserver.cs` | Resolved `ICrdtMigrationBridge` instances lazily to avoid circular DI resolution when `DistributedCrdtScopeManager` initializes replica scopes in its constructor. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Observers/OrchestratorClusterStateObserver.cs` | Replaced `Task.Run` background task allocation with `ReevaluateTopologyAsync(IntentDeliveryMode.FireAndForget)`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Observers/OrchestratorRegistryStateObserver.cs` | Observer implementation for `IDocumentStateObserver<CrdtRegistryState>` that triggers orchestrator topology re-evaluation when the system registry state changes. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Orchestration/CrdtRemoteProxyClient.cs` | Serialized demoted document snapshots once per document and dispatched snapshot transfers to target peers in parallel. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Orchestration/ICrdtRemoteProxyClient.cs` | Interface for outbound P2P RPC forwarding of document reads, operation intents, patches, and demoted shard snapshots. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtP2pPayloadHandler.cs` | Deserialized `CrdtMessageWrapper` directly from `payload.Span` without calling `payload.ToArray()`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtPatchBroadcaster.cs` | Made `CrdtPatchBroadcaster.Dispose()` idempotent so both `DistributedCrdtScope.DisposePatchObservers()` and the DI scope container can safely dispose it. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtRpcPayloadProcessor.cs` | Flushed target document and `IDocumentPatchObserver` instances before returning `CrdtIntentRes` for acknowledged remote intent RPCs so generated patches are dispatched to secondary shard replicas before acknowledging the write. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtStateSyncPayloadProcessor.cs` | Scoped `recentSnapshotDispatches` cooldown dictionary keys by `(ScopeReplicaId, PeerReplicaId, ShardId)` so multiple scopes in the same singleton processor never collide when peers share replica names. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtTopologyObserver.cs` | Simplified peer join scope checks to use `IScopeTopologyProvider` directly. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/ICrdtRpcPayloadProcessor.cs` | Interface for processing incoming P2P RPC request and response payloads for remote document reads, intent forwarding, and Map/Reduce batches. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/ICrdtStateSyncPayloadProcessor.cs` | Interface for processing incoming anti-entropy `CrdtSync` messages, evaluating causal journal requirements, and dispatching missing operations or snapshots. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Scopes/DistributedCrdtScopeFactory.cs` | Changed `DistributedCrdtScope` visibility to `internal` so `DistributedCrdtScopeManager` can invoke `EnsureInitializedAsync` directly. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Scopes/IDistributedCrdtScope.cs` | Added `IsPinnedHostScope`, `LastActivityTimestamp`, and `TouchActivity()` to track scope lifecycle and activity. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Scopes/IDistributedCrdtScopeFactory.cs` | Added `isPinnedHostScope` parameter to `CreateScope(string scopeId, string hostReplicaId, bool isPinnedHostScope = false)`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Sharding/ClusterStateTracker.cs` | Filtered out non-positive (`<= 0`) version entries in `CloneDvv` and sanitized incoming DVVs before merging in `UpdatePeerState`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Sharding/ConsistentHashShardDistributionManager.cs` | Short-circuited single-node ring lookups in `GetTargetNodesForShardAsync` and eliminated byte array allocations in `ComputeHash` using `stackalloc` and `ArrayPool<byte>`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Sharding/CrdtEvictionService.cs` | Removed `doc.ResetLocalStateAsync` from `RebootLocalIdentityAsync` so rebooting local replica identity preserves offline local document data while clearing shard version vectors. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Sharding/DefaultScopeTopologyProvider.cs` | Implemented `CanAcceptScopeFromPeerAsync` and `CanReplicateScopeToPeerAsync` directly on `DefaultScopeTopologyProvider`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Sharding/IClusterStateTracker.cs` | Added `TryGetPeerState` to inspect a specific peer's last known shard version vector map. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Sharding/ICrdtEvictionService.cs` | Added `IntentDeliveryMode` overload for `EvictPeersAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Sharding/IScopeTopologyProvider.cs` | Removed unused `IsPeerExpectedAsync` method from `IScopeTopologyProvider`, leaving `CanAcceptScopeFromPeerAsync` and `CanReplicateScopeToPeerAsync` as the core contract. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Sharding/IShardDistributionManager.cs` | Converted targeted explicit evaluation structures natively executing `ValueTask` dynamically mapping active peer registries asynchronously. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Storage/BufferedDistributedCrdtStorageDecorator.cs` | Delegated `SaveSerializedDocumentBatchAsync` to the underlying storage implementation. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Storage/CrdtTrimCoordinator.cs` | Converted `CrdtTrimCoordinator` from a static global lock into an injectable per-node singleton service so parallel test nodes do not block one another. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Storage/DiskWriteAheadBuffer.cs` | Implemented `IDisposable` alongside `IAsyncDisposable` so scoped DI containers can dispose `DiskWriteAheadBuffer` synchronously. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Storage/ICrdtWriteAheadBuffer.cs` | Inherited `IDisposable` alongside `IAsyncDisposable` on `ICrdtWriteAheadBuffer`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Storage/IDistributedCrdtStorage.cs` | Added `SaveSerializedDocumentBatchAsync` with a default fallback implementation for batching document checkpoints. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Storage/InMemoryWriteAheadBuffer.cs` | Implemented `IDisposable` alongside `IAsyncDisposable` so scoped DI containers can dispose `InMemoryWriteAheadBuffer` synchronously. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Storage/MemoryCrdtStorage.cs` | Implemented `SaveSerializedDocumentBatchAsync` under a single lock acquisition. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/Storage/StorageJournalForwarder.cs` | Updated aggressive journal trim to batch-checkpoint dirty documents and drain pending WAL buffers before trimming storage. |
| `$/Ama.Enterprise.CRDT.MessagePack.IntegrationTests/Ama.Enterprise.CRDT.MessagePack.IntegrationTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.CRDT.MessagePack.IntegrationTests/Models/IntegrationTestModels.cs` | Added `ConcurrentDictionaryModel` to test `ConcurrentDictionary<TKey, TValue>` AOT MessagePack serialization. |
| `$/Ama.Enterprise.CRDT.MessagePack.IntegrationTests/Services/MessagePackSerializerIntegrationTests.cs` | Added integration test verifying `ConcurrentDictionary<TKey, TValue>` serialization and deserialization. |
| `$/Ama.Enterprise.CRDT.MessagePack.IntegrationTests/Services/TolerantReaderIntegrationTests.cs` | Integration tests validating standard STJ contextual mappings and MessagePack arrays tracking tolerant reader capabilities and executing DI resolutions. |
| `$/Ama.Enterprise.CRDT.MessagePack.SourceGenerators.UnitTests/Ama.Enterprise.CRDT.MessagePack.SourceGenerators.UnitTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.CRDT.MessagePack.SourceGenerators.UnitTests/Architecture/KnownContextsArchitectureTests.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.MessagePack.SourceGenerators/Ama.Enterprise.CRDT.MessagePack.SourceGenerators.csproj` | Added `<Features>runtime-async=on</Features>`. |
| `$/Ama.Enterprise.CRDT.MessagePack.SourceGenerators/Generators/MessagePackFormatterGenerator.cs` | Updated generated MessagePack deserializers to pool deserialized `string` properties and constructor arguments via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.CRDT.MessagePack.UnitTests/Ama.Enterprise.CRDT.MessagePack.UnitTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.CRDT.MessagePack.UnitTests/Formatters/CrdtPolymorphicMessagePackFormatterTests.cs` | Unit tests for `CrdtPolymorphicMessagePackFormatter` verifying array formatting logic and unregistered polymorphic bounds throwing. |
| `$/Ama.Enterprise.CRDT.MessagePack.UnitTests/Formatters/CrdtPolymorphicMessagePackRegistryTests.cs` | Unit tests verifying generic AOT delegate caching and resolution bounded in CrdtPolymorphicMessagePackRegistry. |
| `$/Ama.Enterprise.CRDT.MessagePack.UnitTests/Models/TestModel.cs` | Test model simulating a binary serializable DTO utilizing `MessagePackObject` annotations. |
| `$/Ama.Enterprise.CRDT.MessagePack.UnitTests/Services/MessagePackCrdtSerializerTests.cs` | Appended structural serialization boundaries verifying `byte[]` arrays convert to MessagePack BIN formats rather than polymorphic generics. |
| `$/Ama.Enterprise.CRDT.MessagePack/Ama.Enterprise.CRDT.MessagePack.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.CRDT.MessagePack/Extensions/ServiceCollectionExtensions.cs` | Dependency injection extensions bootstrapping the decoupled custom `MessagePackCrdtSerializer` instance globally. Exposed `params` overload bridging multi-assembly mapped bounds and chaining execution fallback boundaries. |
| `$/Ama.Enterprise.CRDT.MessagePack/Formatters/CrdtPolymorphicMessagePackFormatter.cs` | Pooled polymorphic type discriminators and deserialized string payloads via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.CRDT.MessagePack/Formatters/CrdtPolymorphicMessagePackRegistry.cs` | Centralized registry caching typed AOT polymorphic serialization delegates for MessagePack. |
| `$/Ama.Enterprise.CRDT.MessagePack/MessagePackCrdtSerializer.cs` | Replaced `bytes.ToArray()` allocations in `DeserializeFromBytes` with `ArrayPool<byte>.Shared` buffers. |
| `$/Ama.Enterprise.FeatureFlags.IntegrationTests/Ama.Enterprise.FeatureFlags.IntegrationTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.FeatureFlags.IntegrationTests/Services/FeatureFlagDomainIntegrationTests.cs` | Added test assertions verifying that identically named feature flags across different product/tenant documents do not overwrite one another in `GetAllFlagsAsync`. |
| `$/Ama.Enterprise.FeatureFlags.ShowCase/Ama.Enterprise.FeatureFlags.ShowCase.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.FeatureFlags.ShowCase/Program.cs` | Redrew feature flags immediately after `set` and `del` commands, observed `CrdtRegistryState` changes for cross-shard redraws, and lowered showcase `GossipInterval` to 250ms. |
| `$/Ama.Enterprise.FeatureFlags.ShowCase/start-azurite.bat` | Windows batch script that automatically discovers and executes natively installed Azurite instances (via Visual Studio or NPM) bypassing Docker requirements, explicitly detached via PowerShell to prevent MSBuild hangs. |
| `$/Ama.Enterprise.FeatureFlags/Ama.Enterprise.FeatureFlags.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.FeatureFlags/Constants.cs` | Removed `GlobalDocumentId` and `DefaultScope`; updated `GetDocumentId(productId, tenantId)` to require non-empty `productId` and `tenantId`. |
| `$/Ama.Enterprise.FeatureFlags/Extensions/ServiceCollectionExtensions.cs` | Removed `FeatureFlagBootstrapper` registration as feature flag documents are now lazily created per product and tenant shard. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlag.cs` | Represents a single enterprise feature flag in the system incorporating audit and ownership structures. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagAudit.cs` | Audit trace structure tracking temporal modifications for a specific feature flag. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagMetadata.cs` | Metadata structure encapsulating enterprise multi-tenancy and product domain boundaries. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagOwnership.cs` | Ownership details specifying team contact routing for a feature flag. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagQueryDto.cs` | Request DTO for filtering feature flags by product and tenant across sharded cluster documents. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagQueryResultDto.cs` | Result DTO containing aggregated feature flags collected across sharded product and tenant documents. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagState.cs` | Defaulted `Id` to `string.Empty` instead of a global singleton document ID. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsCrdtAotContext.cs` | Registered `FeatureFlagQueryDto` and `FeatureFlagQueryResultDto` in the AOT reflection context. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsJsonContext.cs` | Registered `FeatureFlagQueryDto` and `FeatureFlagQueryResultDto` in the AOT JSON serialization context. |
| `$/Ama.Enterprise.FeatureFlags/Services/FeatureFlagClusterQuery.cs` | Updated `MapDocumentAsync` to read `context.Data` directly instead of `context.Document.Data`. |
| `$/Ama.Enterprise.FeatureFlags/Services/FeatureFlagCrdtManager.cs` | Updated flag and registry reads to use `GetDocumentDataAsync<FeatureFlagState>` and `Registry.GetDataAsync`. |
| `$/Ama.Enterprise.FeatureFlags/Services/FeatureFlagStateObserver.cs` | Updated `OnStateChangedAsync` to match `feature-flag:{productId}:{tenantId}` document IDs without any global document fallback. |
| `$/Ama.Enterprise.FeatureFlags/Services/IFeatureFlagCrdtManager.cs` | Updated contract to require `productId` and `tenantId` on `SetFlagAsync`, `RemoveFlagAsync`, and `GetFlagsAsync`, plus `GetAllFlagsAsync` for cluster-wide queries. |
| `$/Ama.Enterprise.FeatureFlags/Services/IFeatureFlagStateObserver.cs` | Dedicated Feature Flag domain observer contract enabling consumer systems explicitly routing decoupled tracking dynamically perfectly securely. |
| `$/Ama.Enterprise.Licensing.UnitTests/Ama.Enterprise.Licensing.UnitTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.Licensing.UnitTests/Services/CertificateLoaderTests.cs` | Unit tests verifying certificate loading capabilities from multiple inputs evaluating null constraints and cryptographic formats. |
| `$/Ama.Enterprise.Licensing.UnitTests/Services/HonorLicenseManagerTests.cs` | Evaluates generic honor checks mapping cryptography bounds. Updated constructor payloads mapping new structural bounds matching the expanded payload fields evaluating assertions. |
| `$/Ama.Enterprise.Licensing/Ama.Enterprise.Licensing.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.Licensing/Extensions/ServiceCollectionExtensions.cs` | Redesigned configuration pipeline splitting Open Source and Enterprise declarations into distinct APIs. Applied `[UnsupportedOSPlatform("browser")]` to the Enterprise configuration to enforce compile-time compiler warnings/errors on Blazor WebAssembly. |
| `$/Ama.Enterprise.Licensing/Models/DeclaredLicenseType.cs` | Enum defining user choices for license declaration (OpenSource, Enterprise, Unknown). |
| `$/Ama.Enterprise.Licensing/Models/LicenseOptions.cs` | Configuration structure holding license strings and extended options. Adjusted `DeclaredLicenseType` to utilize an internal set preventing external bypasses to the DI assignment bounds. |
| `$/Ama.Enterprise.Licensing/Models/LicensePayload.cs` | DTO representing the parsed JSON structure contained within an Enterprise license cryptographic payload defining boundaries. Added expanded fields such as LicenseId, ExpirationDate, Tier, and MaxNodes. |
| `$/Ama.Enterprise.Licensing/Models/LicensingJsonContext.cs` | Source-generated AOT JSON serialization context ensuring parsing for licensing JSON payload structures. |
| `$/Ama.Enterprise.Licensing/Services/CertificateLoader.cs` | Simplified embedded resource loading explicitly dropping `.pfx` bounds, supporting only public `.cer` structures securely. |
| `$/Ama.Enterprise.Licensing/Services/HonorLicenseManager.cs` | Implementation evaluating honor-based constraints. Refactored null assignments into a cleaner `SetLicenseState` method bypassing redundant code lines and mapping the expanded payload fields. |
| `$/Ama.Enterprise.Licensing/Services/ICertificateLoader.cs` | Removed password parameter from `LoadFromEmbeddedResource` to enforce public-only embedded certificates. |
| `$/Ama.Enterprise.Licensing/Services/ILicenseManager.cs` | Interface separating generic honor-based logic capabilities avoiding tightly coupled bounds. Added explicit getters matching the expanded structured bounds. |
| `$/Ama.Enterprise.Licensing/Services/LicenseStartupService.cs` | Starts up evaluations executing internal generic licensing checks. |
| `$/Ama.Enterprise.Licensing/ama-license.cer` | No description provided. |
| `$/Ama.Enterprise.Monitoring.Cli/Ama.Enterprise.Monitoring.Cli.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.Monitoring.Cli/Program.cs` | Refactored internal architecture mapping UI generation explicitly bounded to modular distinct generic boundaries decoupled from program setups traversing dependencies explicitly. |
| `$/Ama.Enterprise.Monitoring.Cli/Services/IUserInterfaceOrchestrator.cs` | Contract decoupling terminal GUI orchestrations avoiding inline Program allocations and generic boundaries. |
| `$/Ama.Enterprise.Monitoring.Cli/Services/UserInterfaceOrchestrator.cs` | Implements CLI user interface initializations managing standard generic window layouts. |
| `$/Ama.Enterprise.Monitoring.Cli/UI/CommandPane.cs` | Terminal UI frame evaluating input controls decoupling internal commands traversing text inputs. |
| `$/Ama.Enterprise.Monitoring.Cli/UI/DashboardPane.cs` | Terminal UI frame managing the dashboards generic bounds. |
| `$/Ama.Enterprise.Monitoring.Cli/UI/MainWindow.cs` | Standard application main layout structure binding command inputs. |
| `$/Ama.Enterprise.Monitoring/Ama.Enterprise.Monitoring.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.Monitoring/Constants.cs` | Global constants for the monitoring module, defining default identifiers. |
| `$/Ama.Enterprise.Monitoring/Extensions/ServiceCollectionExtensions.cs` | Updated `AddMonitoringMesh` builder transparently executing explicit internal MessagePack pipeline executions mapping injected DI delegations decoupled. |
| `$/Ama.Enterprise.Monitoring/Models/MonitoringOptions.cs` | Added explicit decoupled properties `UseBinarySerialization` and `ConfigureBinarySerialization` exposing format orchestration hooks directly within the monitoring DI bounds. |
| `$/Ama.Enterprise.Monitoring/Services/WebRtcMonitoringBootstrapper.cs` | Background hosted service managing automatic out-of-band WebRTC signaling connections discovering a central monitoring aggregator node. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Ama.Enterprise.P2p.AspNetCore.IntegrationTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Services/AspNetCorePeerDiscoveryIntegrationTests.cs` | Updated integration test mapping evaluating `TargetHost`, `TargetPort`, and `TargetUseHttps` decoupling dynamically built `UriBuilder` HTTP routing, bypassing string concatenation vulnerabilities. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Services/AspNetCorePeerHandshakeIntegrationTests.cs` | Refactored integration tests verifying the decoupled ASP.NET Core Standalone peer handshaker discovering topologies. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Services/AspNetCoreTransportIntegrationTests.cs` | Upgraded end-to-end multi-mesh generic routing tests evaluating ASP.NET Core and Standalone structures. |
| `$/Ama.Enterprise.P2p.AspNetCore/Ama.Enterprise.P2p.AspNetCore.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.AspNetCore/Extensions/AspNetCoreDiscoveryServiceCollectionExtensions.cs` | Updated DI registrations to inject `IPeerRegistry` and `IFailureDetector` dependencies into the `AspNetCorePeerHandshaker` bounds. |
| `$/Ama.Enterprise.P2p.AspNetCore/Extensions/EndpointRouteBuilderExtensions.cs` | Refactored ASP.NET Core endpoint routing to use `RequestDelegate` handlers extracting routing context, resolving AOT serialization and trimming warnings. |
| `$/Ama.Enterprise.P2p.AspNetCore/Extensions/ServiceCollectionExtensions.cs` | Removed implicit `IsEnabled` assignments mapping abstract generic pipeline dependencies. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCoreDiscoveryOptions.cs` | Replaced legacy single `DiscoveryUrl` parameter splitting properties into decoupled `TargetHost`, `TargetPort`, and `TargetUseHttps` matching Phase 2 routed capabilities, avoiding tuple mismatches. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCoreHandshakeOptions.cs` | Configuration structure for isolated ASP.NET Core handshaking parameters tracking decoupled Integrated and Standalone topological bounding. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCoreHostingMode.cs` | Determines whether the ASP.NET Core P2P transport relies on the host application's HTTP pipeline routed via MapP2pMeshEndpoints() or spins up an isolated Standalone Kestrel web server decoupled internally. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCoreJsonContext.cs` | Generates isolated interoperability processing structured models. Appended Kestrel bounds resolving AOT serialization bounds. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCorePeerEndpoint.cs` | Identifies and bridges standard inbound external mapping boundaries and routed ports. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCoreTransportOptions.cs` | Removed `IsEnabled` flag adhering to DI-driven component configuration bounds tracking decoupled configurations. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/HttpPayloadProcessResult.cs` | Enumeration identifying deterministic HTTP processing outcomes. Consolidated from removed Http.Core package. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/AspNetCoreTransport.cs` | Removed `IsEnabled` validations relying on DI registration for lifecycle execution. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/AspNetCoreTransportListener.cs` | Refactored standalone listener initialization formatting standard library path constraints guaranteeing distinct execution spaces. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/Discovery/AspNetCorePeerDiscovery.cs` | Refactored discovery orchestrator computing polling URIs dynamically leveraging structured targeting definitions isolating bounds explicitly matching isolated prefixes. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/Discovery/AspNetCorePeerHandshaker.cs` | Updated ASP.NET Core handshaker integrating registry and failure detector. Evaluates inbound HTTP handshake requests, dynamically establishing explicit bidirectional peer mappings instantly without relying on delayed discovery loops. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/HttpInboundDispatcher.cs` | Used `MemoryStream.TryGetBuffer` to pass a `ReadOnlySpan<byte>` to `IMeshWireEncoder.Decode` without allocating a duplicate byte array. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/IHttpInboundDispatcher.cs` | Contract decoupling generic abstract HTTP routing frameworks. Consolidated from removed Http.Core package. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/CertificateNetworkIntegrationTests.cs` | Integration tests verifying network connectivity and drop routines mapping dynamically evaluated certificate authentication boundaries resolving decentralized topological rules. Appended E2E end-to-end multi-node tests verifying deduplication mechanics and dynamic adaptability when nodes join and drop using strict certificate authentication. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/Handlers/TestMessageHandler.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/Models/TestNode.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/P2pAdvancedIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/P2pNetworkIntegrationTests.cs` | Contains complex integration tests validating TCP binding, deduplications, and payload distributions. Removed obsolete direct `HttpClient` testing replacing it with `ITransportRouter`. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/P2pVersioningIntegrationTests.cs` | Updated `Network_ShouldAcceptMessages_WithCurrentYamlProtocolVersion_0_1_to_0_2` to exchange a mix of `0.1.0` and `0.2.0` protocol version messages between two nodes. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/PushPullGossipIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/QuicNetworkIntegrationTests.cs` | Added integration testing evaluating multiplexed encrypted QUIC TLS 1.3 behaviors validating standard networking flows. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/SessionRoutingIntegrationTests.cs` | Integration tests verifying end-to-end multi-mesh Zero-Trust routing policies tracking token validations, outbound drops, and inbound structural rejections implicitly. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/TcpNetworkIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/UdpNetworkIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/UdpPeerDiscoveryIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/WireEncryptionIntegrationTests.cs` | Integration tests validating End-to-End (E2E) AES-GCM data-in-transit wire encryption logic mapping dynamically without topology drops across shared network configurations. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Ama.Enterprise.P2p.IntegrationTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Architecture/VersioningArchitectureTests.cs` | Architectural tests that parse the CI/CD deployment files ensuring specific deployed versions possess test coverage. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Discovery/DnsPeerDiscoveryIntegrationTests.cs` | Integration tests verifying DNS peer discovery resolves target domains and dispatches Phase 2 handshakes against discovered IPs. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Ama.Enterprise.P2p.Mqtt.IntegrationTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Services/MqttPeerDiscoveryIntegrationTests.cs` | Integration tests verifying MQTT peer discovery mapping decoupled multi-mesh architectures. Migrated to use `TcpTransport` and `TcpPeerEndpoint` dropping obsolete HTTP transport bindings. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Services/MqttTransportIntegrationTests.cs` | Integration tests verifying end-to-end MQTT transport functionality evaluating isolated inbound subscriptions. |
| `$/Ama.Enterprise.P2p.Mqtt/Ama.Enterprise.P2p.Mqtt.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.Mqtt/Extensions/MqttDiscoveryServiceCollectionExtensions.cs` | Updated DI registrations to inject `IPeerRegistry` and `IFailureDetector` dependencies natively into the `MqttPeerHandshaker` bounds. |
| `$/Ama.Enterprise.P2p.Mqtt/Extensions/ServiceCollectionExtensions.cs` | Extracted MQTT JSON AOT polymorphic registrations into a reusable idempotent block tracking distinct bounds, ensuring generic network initializations. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttDiscoveryJsonContext.cs` | JSON serialization context for Phase 1 and Phase 2 generic discovery primitives in MQTT. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttDiscoveryMessage.cs` | Modified payload projecting local Phase 2 routing pseudo-ports ensuring decoupled message isolation. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttDiscoveryOptions.cs` | Configuration options for tuning active MQTT peer discovery broadcast intervals and topic suffixes. Now decoupled from the generic MQTT transports, providing isolated broker connection settings for discovery architectures. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttHandshakeMessage.cs` | Data structure representing the Phase 2 MQTT network handshake payload exchange. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttHandshakeOptions.cs` | Appended local `HandshakePort` parameter isolating connection profiles avoiding nested IP overlapping bounds. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttJsonContext.cs` | Source-generated AOT JSON serialization context for the MQTT endpoint model structure. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttPeerEndpoint.cs` | Inherited PeerEndpoint model representing an isolated MQTT destination node defined by its internal client identity identifier. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttRoutingEndPoint.cs` | Custom end point representing an MQTT routing target identifying a client. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttTransportOptions.cs` | Configuration record setting broker connection host endpoints credentials and specific topic routing bounds. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/Discovery/MqttPeerDiscovery.cs` | Enhanced discovery by orchestrating encapsulated payloads and metrics. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/Discovery/MqttPeerHandshaker.cs` | Updated MQTT handshaker integrating registry and failure detector. Evaluates inbound MQTT handshake topics, actively enforcing immediate bidirectional tracking without relying on delayed discovery loops. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/IMqttClientManager.cs` | Interface establishing lifecycle controls for individual MQTT client subscriptions and active payloads publications. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttClientManager.cs` | Updated to isolate topic subscriptions and client connection IDs by injecting the `meshId`, preventing cross-mesh broker collisions. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttTransport.cs` | Integrated unified format mapping encrypting outgoing structural streams. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttTransportListener.cs` | Extracted the deserialization bridging directly via generic decoders handling incoming payloads. |
| `$/Ama.Enterprise.P2p.Telemetry.Cli/Ama.Enterprise.P2p.Telemetry.Cli.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.Telemetry.Cli/Program.cs` | Refactored internal architecture replacing `Spectre.Console` bounds with `Terminal.Gui` structural mappings. Addressed UI framerate stuttering by shifting string building and data sorting to background threads avoiding blocking UI capabilities, while standardizing single-assignment `IList<string>` data sources circumventing layout recalculation storms present in `ObservableCollection`. Enforced standard coding practices removing prefixed fields, introducing null guard checks, and utilizing interfaces. |
| `$/Ama.Enterprise.P2p.Telemetry.IntegrationTests/Ama.Enterprise.P2p.Telemetry.IntegrationTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.Telemetry.IntegrationTests/Services/TelemetryNetworkIntegrationTests.cs` | Updated to test custom meter boundaries by asserting on custom metric scopes configurations. |
| `$/Ama.Enterprise.P2p.Telemetry.UnitTests/Ama.Enterprise.P2p.Telemetry.UnitTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.Telemetry.UnitTests/Services/ClusterMetricsAggregatorTests.cs` | Unit tests validating the `ClusterMetricsAggregator` tracking time series histories calculating deltas avoiding logic errors. Appended verification bounds securing standard behavior mappings across Histograms, UpDownCounters, and monotonic metric bounds. |
| `$/Ama.Enterprise.P2p.Telemetry/Ama.Enterprise.P2p.Telemetry.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.Telemetry/Constants.cs` | Defines the core meter names evaluated by the telemetry network listeners isolating generic algorithms. |
| `$/Ama.Enterprise.P2p.Telemetry/Extensions/ServiceCollectionExtensions.cs` | Updated DI to map singletons for the `TelemetryPushProtocol` ensuring generic instance isolation matching background network hooks. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/ClusterMetricAggregation.cs` | Data structure representing the computed aggregated statistics for a specific metric across a cluster of nodes. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/MetricSnapshotDto.cs` | Pooled `Name` and `Type` strings via `CrdtStringPool.Shared` in constructor and `init` accessors. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/MetricTagDto.cs` | Pooled `Key` and `Value` strings via `CrdtStringPool.Shared` in constructor and `init` accessors. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/TelemetryJsonContext.cs` | AOT generic bindings resolving metric serialization evaluating pure DTO constraints. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/TelemetryOptions.cs` | Updated to expose tracking configuration enabling generic meter dimension scopes via an `IEquatable` bounds implementation. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/TelemetryPayloadDto.cs` | Appended `MagicHeader` constant enabling fast-path binary stream identification prior to entering deserialization pipelines. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/ClusterMetricsAggregator.cs` | Pooled repeatable metric names, metric types, tag keys/values, and composite metric keys using `ICrdtStringPool`. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/IClusterMetricsAggregator.cs` | Contract for aggregating cluster-wide telemetry metrics across active nodes tracking mathematical trends. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/ITelemetryAggregator.cs` | Interface for centrally aggregating and retrieving in-memory telemetry network states. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryAggregator.cs` | Removed post-creation mutation of `init`-only metric properties now that `MetricSnapshotDto` and `MetricTagDto` pool strings upon construction. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryForwarderService.cs` | Prepended the fixed `MagicHeader` bytes to outgoing metric broadcasts ensuring compatibility with the fast-path telemetry boundary evaluation mechanism. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryPayloadHandler.cs` | Implemented binary header slicing, evaluating payloads for magic bytes and dropping unknown streams, bypassing heavy `MessagePackSerializationException` CPU spikes. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryPushAlgorithm.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Ama.Enterprise.P2p.UnitTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/Core/InMemoryPeerRegistryTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/Core/MessageDispatcherTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/Core/RandomPeerSelectorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/Core/TimeBasedFailureDetectorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore.IntegrationTests/Ama.Enterprise.P2p.WebRTC.AspNetCore.IntegrationTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore.IntegrationTests/Services/WebRtcSignalingIntegrationTests.cs` | Extended integration coverage to assess strict ASP.NET Core WSS (Secure WebSockets) Standalone and Integrated HTTPS WebRTC out-of-band signaling negotiation tracking local X.509 definitions and avoiding handshake drops. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Ama.Enterprise.P2p.WebRTC.AspNetCore.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Extensions/EndpointRouteBuilderExtensions.cs` | Maps inbound WebRTC signaling endpoints managing WebSocket isolated handshakes negotiating SDP descriptors, reading route prefixes directly from injected settings. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Extensions/ServiceCollectionExtensions.cs` | Updated the dependency injection setup matching the newly integrated zero-trust dependencies (`IPeerAuthenticator`, `P2pNodeOptions`, `PeerEndpoint`, `IFailureDetector`) securing ASP.NET Core WebRTC components explicitly. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Models/WebRtcSignalingAction.cs` | Added `AuthRequest` and `AuthResponse` protocol actions orchestrating strict zero-trust explicit out-of-band WebSockets signaling. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Models/WebRtcSignalingOptions.cs` | Removed `IsEnabled` explicitly delegating standard activation directly through DI bounds ensuring decoupled tracking. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/IWebRtcHttpPeerDiscovery.cs` | Contract for a service that orchestrates the out-of-band WebRTC signaling workflow against a specific remote HTTP endpoint. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/IWebRtcSignalingClient.cs` | Updated negotiation signatures requiring local zero-trust generic authentication payloads. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/WebRtcHttpPeerDiscovery.cs` | Extended WebRTC HTTP peer discovery to construct mapped local payloads and extract `PeerId`, preventing connection-based amnesia bugs. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/WebRtcSignalingClient.cs` | Injected local `AuthRequest` mappings prior to signaling bounds, returning dynamic remote configurations. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/WebRtcSignalingServer.cs` | Bound zero-trust evaluations mapping incoming `AuthRequest`, tracking client `PeerId` and dropping disconnected `ConnectionId` references. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/WebRtcSignalingWsHelper.cs` | Internal helper wrapping WebSockets bounds isolating generic out-of-band envelope negotiations. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling.IntegrationTests/Ama.Enterprise.P2p.WebRTC.DistributedSignaling.IntegrationTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling.IntegrationTests/Services/WebRtcDistributedSignalingIntegrationTests.cs` | Replaced `AddDistributedCrdtP2p` with `AddDistributedCrdtReplica(replicaId, meshId)`. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Ama.Enterprise.P2p.WebRTC.DistributedSignaling.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Constants.cs` | Defines global constants such as document type aliases and default IDs for the WebRTC CRDT distributed signaling components. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Extensions/EndpointRouteBuilderExtensions.cs` | Refactored routes to require `replicaId` mapping distinct localized `ICrdtSignalingManager` from `DistributedCrdtScopeManager`, bypassing global mesh boundaries. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Extensions/ServiceCollectionExtensions.cs` | Updated WebRTC Distributed Signaling DI pipeline replacing global singleton tracking with `AddDistributedCrdtService`, scoping the manager to local CRDT replica bounds, bypassing multi-mesh overlap limits. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Models/CrdtSignalingState.cs` | Root CRDT document model representing the WebRTC out-of-band signaling state drop-box updating definitions reflecting dynamic target tracking properties matching presence scopes. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Models/DistributedSignalingAotContext.cs` | Appended standard CRDT payload structs (CrdtDocument, JournaledOperation, DottedVersionVector) exposing explicit AOT-friendly serialization mappings. Cleaned up duplicate Dictionary definitions and adjusted class to `public sealed`. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Models/DistributedSignalingCrdtAotContext.cs` | Native AOT mapping explicitly bounding WebRTC DTOs and multidimensional dictionary topologies securing the distributed serialization pipeline. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Services/CrdtSignalingManager.cs` | Updated `GetJoinIntentsAsync`, `GetOffersAsync`, and `GetAnswersAsync` to use `GetDocumentDataAsync<CrdtSignalingState>`. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Services/ICrdtSignalingManager.cs` | Added overloads accepting `IntentDeliveryMode` for join intents, offers, and answers. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Services/IWebRtcDistributedSignalingClient.cs` | Replaced `meshId` generic parameters with strict `replicaId` mappings matching isolated CRDT localized multi-replica bounds. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Services/IWebRtcSignalingOrchestrator.cs` | Appended `replicaId` tracking executing constraints alongside `meshId` tracking bounds matching local CRDT generic orchestrations. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Services/WebRtcDistributedSignalingClient.cs` | Updated HTTP signaling boundaries executing `replicaId` targeted bounds tracking multi-replica architectural routes avoiding unstructured multi-mesh amnesia limits. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Services/WebRtcSignalingOrchestrator.cs` | Updated `replicaId` injection scoping active background multi-party synchronization evaluations against multi-mesh tracking states targeting explicit bounds. |
| `$/Ama.Enterprise.P2p.WebRTC.IntegrationTests/Ama.Enterprise.P2p.WebRTC.IntegrationTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.WebRTC.IntegrationTests/Services/WebRtcTransportIntegrationTests.cs` | Updated `TestMessage` implementing the newly enforced `ProtocolVersion` satisfying `IMeshMessage`. |
| `$/Ama.Enterprise.P2p.WebRTC/Ama.Enterprise.P2p.WebRTC.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p.WebRTC/Extensions/ServiceCollectionExtensions.cs` | WebRTC dependency injection pipeline overriding standard time-based failure detectors, replacing them with isolated `WebRtcFailureDetector` tracking. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcHandshakeMessage.cs` | In-band signaling structure notifying local identity topologies through initialized WebRTC channels. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcInvitationAnswer.cs` | DTO representing a WebRTC invitation answer containing the connection identifier and the SDP answer string. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcInvitationOffer.cs` | DTO representing a WebRTC invitation offer containing the connection identifier and the SDP offer string. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcJsonContext.cs` | Updated to `public` to ensure source generators can securely discover and synthesize formatters across decoupled assembly bounds. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcOptions.cs` | Configuration structure holding ICE servers bound to AOT-friendly serialization mappings. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcPeerEndpoint.cs` | WebRTC data channel connection endpoint identifying uniquely scoped connection states. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/IWebRtcConnectionManager.cs` | Interface isolating abstract signaling scopes exposing Data Channel inbound references. Updated to expose `IsConnectionActive` querying native state tracking. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/IWebRtcInvitationService.cs` | Generic mechanism exchanging SDP structures. Updated to use DTOs instead of tuples for SDP exchange. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcConnectionManager.cs` | Implements the management of WebRTC connections and out-of-band signaling. Exposes local real-time `IsConnectionActive` boolean checks based on the internal managed active states. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcFailureDetector.cs` | A dedicated implementation of `IFailureDetector` for WebRTC bypassing standard protocol heartbeats and evaluating exact local connection data channel presence. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcTransport.cs` | Delegated polymorphic bounding directly to internal encoder mapping for handling data. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcTransportListener.cs` | Transformed evaluation pipeline mapping directly generic structural decryption. |
| `$/Ama.Enterprise.P2p/Ama.Enterprise.P2p.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.P2p/Constants.cs` | Global constants for the P2P module, including protocol versions and payload size limits. |
| `$/Ama.Enterprise.P2p/Extensions/CertificateAuthenticatorServiceCollectionExtensions.cs` | Pooled `meshId` and registration keys via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Extensions/DnsDiscoveryServiceCollectionExtensions.cs` | Pooled `meshId` and keyed service identifiers via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Extensions/GossipNetworkServiceCollectionExtensions.cs` | Pooled `meshId` via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Extensions/IP2pMeshBuilder.cs` | Interface for building and configuring specific Keyed DI mesh profiles. |
| `$/Ama.Enterprise.P2p/Extensions/P2pMeshBuilder.cs` | Pooled `MeshId` on `P2pMeshBuilder` initialization via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Extensions/P2pMeshRegistrationTracker.cs` | Pooled `meshId` and composite registration keys via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Extensions/PushPullGossipNetworkServiceCollectionExtensions.cs` | Pooled `meshId` via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Extensions/QuicTransportServiceCollectionExtensions.cs` | Pooled `meshId`, registration keys, and endpoint host strings via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Extensions/ServiceCollectionExtensions.cs` | Pooled `meshId` and keyed service identifiers via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Extensions/SessionAuthenticationServiceCollectionExtensions.cs` | Pooled `meshId` and keyed service identifiers via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Extensions/TcpTransportServiceCollectionExtensions.cs` | Pooled `meshId`, registration keys, and endpoint host strings via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Extensions/UdpDiscoveryServiceCollectionExtensions.cs` | Pooled `meshId` and registration keys via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Extensions/UdpTransportServiceCollectionExtensions.cs` | Pooled `meshId`, registration keys, and endpoint host strings via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Extensions/WireEncoderServiceCollectionExtensions.cs` | Pooled `meshId` and registration keys via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Extensions/ZeroTrustRoutingServiceCollectionExtensions.cs` | Pooled `meshId` and keyed service identifiers via `CrdtStringPool.Shared`. |
| `$/Ama.Enterprise.P2p/Models/Algorithms/GossipMessage.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Algorithms/GossipMessageType.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Algorithms/GossipOptions.cs` | Added `SeenMessageRetention`, `MaxSeenMessages`, and `MaxOutboundQueueSize` to bound gossip memory usage. |
| `$/Ama.Enterprise.P2p/Models/Algorithms/PushPullGossipOptions.cs` | Added `MessageCacheRetention`, `MaxCachedMessages`, and `MaxOutboundQueueSize` to bound push-pull gossip memory usage. |
| `$/Ama.Enterprise.P2p/Models/Core/CertificateAuthenticatorOptions.cs` | Added `RevocationMode` property allowing configuration of X509 certificate revocation checks (e.g., NoCheck for air-gapped environments). |
| `$/Ama.Enterprise.P2p/Models/Core/FailureDetectorOptions.cs` | Configuration options for tuning generic protocol-agnostic failure detection components. |
| `$/Ama.Enterprise.P2p/Models/Core/IExtensibleDistributedPayload.cs` | Added `[JsonIgnore]` attribute to the `BinaryExtensionData` property explicitly avoiding System.Text.Json serializing empty or unmapped internal binary structures, correctly encapsulating STJ from MessagePack behaviors. |
| `$/Ama.Enterprise.P2p/Models/Core/IMeshMessage.cs` | Added required standardized `SenderId` bounding origin payloads decoupled traversing generic algorithms. |
| `$/Ama.Enterprise.P2p/Models/Core/P2pMeshMetadata.cs` | Metadata record registering a specific mesh identifier into the global dependency container for orchestration. |
| `$/Ama.Enterprise.P2p/Models/Core/P2pNodeOptions.cs` | Added `MinActivePeers`, `MaxActivePeers` watermarks, and `MaxDiscoveryInterval` to configure adaptive exponential backoff for discovery algorithms. |
| `$/Ama.Enterprise.P2p/Models/Core/P2pNodeOptionsValidator.cs` | Implementation of `IValidateOptions` enforcing logical boundaries and non-negative interval values for the `P2pNodeOptions` configuration structure. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerEndpoint.cs` | Abstract base record for peer endpoints, configured with JSON polymorphic attributes mapping same-assembly derivatives to support standard AOT serialization. Updated tolerant reader payload properties to be nullable avoiding record equality reference failures. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerHandshakePayload.cs` | DTO encapsulating the peer identity and its specific authentication handshake payload during Phase 2 generic negotiations. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerId.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerNode.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerStatus.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/SessionAuthenticatorOptions.cs` | Configuration structure for tuning session and token-based authentication capabilities within a Zero-Trust P2P mesh architecture. |
| `$/Ama.Enterprise.P2p/Models/Core/SessionContext.cs` | DTO representing an authenticated session context with explicit claims extracted from the network handshakes. |
| `$/Ama.Enterprise.P2p/Models/Core/WireEncoderOptions.cs` | Configuration options explicitly enforcing data-in-transit wire formatting and optional generic cryptographic bounds. |
| `$/Ama.Enterprise.P2p/Models/Discovery/DnsDiscoveryOptions.cs` | Configuration options for DNS-based peer discovery, extended to support SRV record resolution flags. |
| `$/Ama.Enterprise.P2p/Models/Discovery/SrvRecordTarget.cs` | Data structure representing a resolved target hostname and port from a DNS SRV query. |
| `$/Ama.Enterprise.P2p/Models/Discovery/UdpDiscoveryMessage.cs` | Introduced `HandshakePort` property mapping dynamically assigned Phase 2 protocol sockets. |
| `$/Ama.Enterprise.P2p/Models/Discovery/UdpDiscoveryOptions.cs` | Removed the AdvertisedHandshakePort, standardizing decoupled generic mesh boundaries. |
| `$/Ama.Enterprise.P2p/Models/Discovery/UdpHandshakeOptions.cs` | Removed obsolete `TargetPort` decoupling configurations enabling generic mapped payload allocations discovering inbound target bounds. |
| `$/Ama.Enterprise.P2p/Models/P2pJsonSerializerContext.cs` | Appended explicit `PeerHandshakePayload` mappings directly evaluating JSON boundaries. |
| `$/Ama.Enterprise.P2p/Models/Transports/QuicPeerEndpoint.cs` | Represents an isolated QUIC network address enabling the resolution of specific distributed multiplexed targets explicitly. |
| `$/Ama.Enterprise.P2p/Models/Transports/QuicTransportOptions.cs` | Removed `IsEnabled` delegating generic transport enablement standard constraints to DI registrations, bypassing decoupled configurations. |
| `$/Ama.Enterprise.P2p/Models/Transports/TcpPeerEndpoint.cs` | Represents a TCP network address endpoint. |
| `$/Ama.Enterprise.P2p/Models/Transports/TcpTransportOptions.cs` | Removed `IsEnabled` delegating transport enablement to isolated explicitly routed multi-mesh dependency injection allocations. |
| `$/Ama.Enterprise.P2p/Models/Transports/UdpPeerEndpoint.cs` | Represents a UDP network address endpoint. |
| `$/Ama.Enterprise.P2p/Models/Transports/UdpTransportOptions.cs` | Removed `IsEnabled` isolating specific activation tracking, decoupling standard mapped pipelines. |
| `$/Ama.Enterprise.P2p/README.md` | Primary introduction documentation explaining the core architecture, capabilities, getting started guide, and configuration options reflecting .NET Keyed DI bindings. |
| `$/Ama.Enterprise.P2p/Services/Algorithms/GossipAlgorithm.cs` | Drained `MessageQueue` when no peers are connected, capped `SeenMessages` and `MessageQueue` sizes with FIFO eviction, and grouped outbound forwarding tasks per peer. |
| `$/Ama.Enterprise.P2p/Services/Algorithms/PushPullGossipAlgorithm.cs` | Drained `MessageQueue` when no peers are connected, capped `MessageCache` and `MessageQueue` sizes with FIFO eviction, and grouped outbound forwarding tasks per peer. |
| `$/Ama.Enterprise.P2p/Services/Core/ApplicationPayloadDispatcher.cs` | Composite orchestrator dispatching to abstract domain observers. |
| `$/Ama.Enterprise.P2p/Services/Core/CertificatePeerAuthenticator.cs` | Updated certificate chain validation to respect the configured `RevocationMode` from options, enabling offline support. |
| `$/Ama.Enterprise.P2p/Services/Core/DirectMessageSender.cs` | Implements localized targeted point-to-point generic delivery fetching active peering bindings avoiding overarching network broadcast storms. |
| `$/Ama.Enterprise.P2p/Services/Core/IApplicationPayloadDispatcher.cs` | Dispatches targeted application payloads. |
| `$/Ama.Enterprise.P2p/Services/Core/IApplicationPayloadHandler.cs` | Defines a domain-level consumer decoupling underlying distribution protocols. |
| `$/Ama.Enterprise.P2p/Services/Core/IDirectMessageSender.cs` | Defines a targeted point-to-point payload delivery contract decoupling anti-entropy processes from gossip epidemic broadcasts honoring the Single Responsibility Principle. |
| `$/Ama.Enterprise.P2p/Services/Core/IFailureDetector.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IInboundMessageQueue.cs` | Added `PendingCount` property to expose the number of unprocessed inbound network messages waiting in the queue. |
| `$/Ama.Enterprise.P2p/Services/Core/IMeshRoutingPolicy.cs` | Interface for defining zero-trust routing policies that accept or reject messages based on session contexts. |
| `$/Ama.Enterprise.P2p/Services/Core/IMeshWireEncoder.cs` | Updated `Decode` to accept `ReadOnlySpan<byte>` instead of `byte[]`. |
| `$/Ama.Enterprise.P2p/Services/Core/IP2pAlgorithm.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IP2pRpcCorrelator.cs` | Contract for correlating asynchronous outbound P2P requests with inbound network responses enforcing strict stateless async mappings. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerAuthenticator.cs` | Expanded evaluating explicit outgoing local data arrays actively traversing generic topologies. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerDiscovery.cs` | Defines mechanisms for discovering other peers. Refactored to decouple active discovery probes and internal passive listeners from local background loops. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerHandshaker.cs` | Handshake interface bounds updated replacing generic identities mapping utilizing explicit encapsulated payload DTO resolving generic explicit. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerRegistry.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerSelector.cs` | Interface for algorithms that select a generic subset of peers for communication. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerSessionRegistry.cs` | Contract mapping active Peer identifiers to their authenticated Session Context tracking live untrusted multi-mesh topologies. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerTopologyObserver.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/ISessionTokenValidator.cs` | Interface for validating session authorization tokens like JWTs during peer handshakes. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransport.cs` | Generic interface defining the outbound network transport capabilities, augmented with endpoint routing capabilities. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransportListener.cs` | Generic interface defining the inbound network listener capabilities for receiving protocol messages. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransportRouter.cs` | Interface for routing outgoing messages to the appropriate transport based on the endpoint type. |
| `$/Ama.Enterprise.P2p/Services/Core/InMemoryPeerRegistry.cs` | Pooled `meshId` across peer registry operations using `ICrdtStringPool`. |
| `$/Ama.Enterprise.P2p/Services/Core/InMemoryPeerSessionRegistry.cs` | Thread-safe singleton mapping dynamically extracted session contexts per Peer across partitioned boundaries. |
| `$/Ama.Enterprise.P2p/Services/Core/InboundMessageQueue.cs` | Changed `InboundMessageQueue<TMessage>` from an unbounded channel to a bounded channel (capacity 8,192) to apply TCP backpressure during multi-node bursts. |
| `$/Ama.Enterprise.P2p/Services/Core/MeshWireEncoder.cs` | Eliminated intermediate `byte[]` allocations for encryption keys, nonces, unencrypted payload slices, and decrypted buffers by using `ReadOnlySpan<byte>`, `stackalloc`, and `ArrayPool<byte>.Shared`. |
| `$/Ama.Enterprise.P2p/Services/Core/P2pRpcCorrelator.cs` | Updated payload deserialization invoking correct `DeserializeFromBytes<T>` boundaries executing dynamically extracted generic spans. |
| `$/Ama.Enterprise.P2p/Services/Core/PassThroughPeerAuthenticator.cs` | Implemented required generic local array boundaries resolving mapping. |
| `$/Ama.Enterprise.P2p/Services/Core/PolicyEnforcingPayloadDispatcher.cs` | Decorator implementation over the application dispatcher executing strict inbound policy boundaries preventing unauthorized data evaluation. |
| `$/Ama.Enterprise.P2p/Services/Core/PolicyEnforcingTransportRouter.cs` | Decorator implementation over the outbound transport router preventing malicious or unauthorized broadcasts to generic client sessions explicitly. |
| `$/Ama.Enterprise.P2p/Services/Core/RandomPeerSelector.cs` | Implementation of IPeerSelector utilizing random distribution selection. |
| `$/Ama.Enterprise.P2p/Services/Core/SessionPeerAuthenticator.cs` | Peer authenticator establishing and tracking zero-trust session-based boundaries via tokens instead of traditional mutual TLS certificates. |
| `$/Ama.Enterprise.P2p/Services/Core/TimeBasedFailureDetector.cs` | Implementation of IFailureDetector using abstract heartbeats decoupled from specific protocol options. |
| `$/Ama.Enterprise.P2p/Services/Core/TransportRouter.cs` | Composite transport router that delegates sending messages to the correct specific transport implementation. |
| `$/Ama.Enterprise.P2p/Services/Discovery/DnsPeerDiscovery.cs` | Encapsulated targeted structures reading arrays mapping dynamically. |
| `$/Ama.Enterprise.P2p/Services/Discovery/IDnsSrvResolver.cs` | Interface defining the contract for resolving DNS SRV records, allowing abstraction over third-party DNS packages. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpPeerDiscovery.cs` | Separated the multicast receive window timeout from individual Phase 2 handshake timeouts so expiring the multicast listen window does not cancel in-flight peer handshakes. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpPeerHandshaker.cs` | Enabled `ExclusiveAddressUse` instead of `ReuseAddress` on the unicast UDP handshaker listener to prevent multiple processes from binding to the same unicast handshake port. |
| `$/Ama.Enterprise.P2p/Services/P2pHostedService.cs` | Implemented adaptive exponential backoff in the background discovery loop evaluating local peer counts against defined high/low watermarks, saving network resources while preventing isolated islands. |
| `$/Ama.Enterprise.P2p/Services/Transports/QuicTransport.cs` | Used `ArrayPool<byte>.Shared` for the 4-byte length prefix buffer instead of allocating a new `byte[4]` per message. |
| `$/Ama.Enterprise.P2p/Services/Transports/QuicTransportListener.cs` | Passed the rented buffer slice directly as `ReadOnlySpan<byte>` to `IMeshWireEncoder.Decode` and pooled the 4-byte length buffer. |
| `$/Ama.Enterprise.P2p/Services/Transports/TcpTransport.cs` | Enabled `TcpClient.NoDelay` and coalesced the 4-byte length prefix and message payload into a single `WriteAsync` socket call to avoid Nagle's algorithm + delayed ACK stalls. |
| `$/Ama.Enterprise.P2p/Services/Transports/TcpTransportListener.cs` | Passed the rented buffer slice directly as `ReadOnlySpan<byte>` to `IMeshWireEncoder.Decode` and pooled the 4-byte length buffer. |
| `$/Ama.Enterprise.P2p/Services/Transports/UdpTransport.cs` | Bounded data-in-transit payloads traversing network streams through explicit generic structured formatting. |
| `$/Ama.Enterprise.P2p/Services/Transports/UdpTransportListener.cs` | Overhauled byte extraction evaluating mapped decryptions via injected structural `IMeshWireEncoder` dependencies. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/Ama.Enterprise.Project.Analyzers.UnitTests.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/DirectSerializationUsageAnalyzerTests.cs` | Set `ReferenceAssemblies = ReferenceAssemblies.Net.Net80` on `CSharpAnalyzerTest` to resolve BCL types on .NET 10. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/JsonSerializableExtensiblePayloadAnalyzerTests.cs` | Set `ReferenceAssemblies = ReferenceAssemblies.Net.Net80` on `CSharpAnalyzerTest` to resolve `JsonSerializerContext` and BCL types on .NET 10. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/PropertyInfoUsageAnalyzerTests.cs` | Set `ReferenceAssemblies = ReferenceAssemblies.Net.Net80` on `CSharpAnalyzerTest` to resolve BCL types on .NET 10. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/SystemConvertUsageAnalyzerTests.cs` | Set `ReferenceAssemblies = ReferenceAssemblies.Net.Net80` on `CSharpAnalyzerTest` to resolve BCL types on .NET 10. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/TaskDelayWithoutOptionsAnalyzerTests.cs` | Set `ReferenceAssemblies = ReferenceAssemblies.Net.Net80` on `CSharpAnalyzerTest` to resolve BCL types on .NET 10. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/ThreadSleepUsageAnalyzerTests.cs` | Set `ReferenceAssemblies = ReferenceAssemblies.Net.Net80` on `CSharpAnalyzerTest` to resolve BCL types on .NET 10. |
| `$/Ama.Enterprise.Project.Analyzers/Ama.Enterprise.Project.Analyzers.csproj` | Added `<Features>runtime-async=on</Features>`. |
| `$/Ama.Enterprise.Project.Analyzers/DirectSerializationUsageAnalyzer.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/JsonSerializableExtensiblePayloadAnalyzer.cs` | Roslyn diagnostic analyzer enforcing that all models injected into P2P `JsonSerializerContext` classes correctly implement the `IExtensibleDistributedPayload` interface, identifying options structures erroneously injected into AOT mapping arrays explicitly. |
| `$/Ama.Enterprise.Project.Analyzers/PropertyInfoUsageAnalyzer.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/SystemConvertUsageAnalyzer.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/TaskDelayWithoutOptionsAnalyzer.cs` | Fixed a bug where `TimeSpan` static methods (like `FromSeconds`) parameterized with dynamically bound variables (e.g. nested calls like `Math.Max` acting on options) were incorrectly flagged as hardcoded constants. |
| `$/Ama.Enterprise.Project.Analyzers/ThreadSleepUsageAnalyzer.cs` | Roslyn diagnostic analyzer enforcing the prohibition of synchronous `Thread.Sleep` invocations to prevent thread starvation and poor asynchronous performance. |
| `$/Ama.Enterprise.Project.Tests.Common/Ama.Enterprise.Project.Tests.Common.csproj` | Scoped `<Features>runtime-async=on</Features>` to `net11.0` only. |
| `$/Ama.Enterprise.Project.Tests.Common/Attributes/IntegrationFactAttribute.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Tests.Common/Attributes/TestedProtocolVersionAttribute.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Tests.Common/Extensions/XunitLoggingBuilderExtensions.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Tests.Common/Logging/XunitLogger.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Tests.Common/Logging/XunitLoggerProvider.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Tests.Common/Networking/NetworkResourceManager.cs` | No description provided. |
| `$/Ama.Enterprise.slnx` | Updated solution structure definitions tracking explicitly configured bounds explicitly modifying renamed structural instances. |
| `$/CodingStandards.md` | No description provided. |
| `$/FilesDescription - Old.md` | No description provided. |
| `$/FilesDescription.md` | No description provided. |
| `$/Images/Enterprise CRDTs - Distributed CRDTs.png` | No description provided. |
| `$/Images/Enterprise CRDTs - End Products Overview.png` | No description provided. |
| `$/Images/Enterprise CRDTs - P2P.png` | No description provided. |
| `$/Images/ReadFF_B1_Run_NoPipelining_Msgpack_Encrypt_Http_01.png` | No description provided. |
| `$/Images/ReadFF_B1_Run_NoPipelining_Msgpack_Encrypt_Http_02.png` | No description provided. |
| `$/Images/ReadFF_B1_Run_Pipelining_Msgpack_Encrypt_Http_01.png` | No description provided. |
| `$/Images/ReadFF_B1_Run_Pipelining_Msgpack_Encrypt_Http_02.png` | No description provided. |
| `$/Images/ReadFF_P0V3_Run_NoPipelining_Msgpack_Encrypt_Http_01.png` | No description provided. |
| `$/Images/ReadFF_P0V3_Run_NoPipelining_Msgpack_Encrypt_Http_02.png` | No description provided. |
| `$/Images/ReadFF_P0V3_Run_Pipelining_Msgpack_Encrypt_Http_01.png` | No description provided. |
| `$/Images/ReadFF_P0V3_Run_Pipelining_Msgpack_Encrypt_Http_02.png` | No description provided. |
| `$/Images/SetFF_B1_Run_NoPipelining_Msgpack_Encrypt_Http_01.png` | No description provided. |
| `$/Images/SetFF_B1_Run_NoPipelining_Msgpack_Encrypt_Http_02.png` | No description provided. |
| `$/Images/SetFF_B1_Run_Pipelining_Msgpack_Encrypt_Http_01.png` | No description provided. |
| `$/Images/SetFF_B1_Run_Pipelining_Msgpack_Encrypt_Http_02.png` | No description provided. |
| `$/Images/SetFF_P0V3_Run_NoPipelining_Msgpack_Encrypt_Http_01.png` | No description provided. |
| `$/Images/SetFF_P0V3_Run_NoPipelining_Msgpack_Encrypt_Http_02.png` | No description provided. |
| `$/Images/SetFF_P0V3_Run_Pipelining_Msgpack_Encrypt_Http_01.png` | No description provided. |
| `$/Images/SetFF_P0V3_Run_Pipelining_Msgpack_Encrypt_Http_02.png` | No description provided. |
| `$/LICENSE` | Dual-license agreement detailing the revenue-capped Community and Enterprise honor-based terms, updated to enforce strictly constrained GPLv3 or standard license bounds for forks and derivative works. |
| `$/README.md` | Primary introduction documentation explaining the core architecture, capabilities, getting started guide, and repository structure for the decentralized P2P toolkit. Language simplified for easier reading. |
| `$/apps-todo.txt` | No description provided. |
| `$/global.json` | No description provided. |
| `$/journal-serialization-plan.md` | No description provided. |
| `$/performance.md` | Contains comprehensive load testing scenarios, architecture setups, and performance metrics validating the system's resilience and throughput under different Azure App Service plans. |
| `$/solution.settings.json` | No description provided. |
