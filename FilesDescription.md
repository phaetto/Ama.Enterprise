| File Path | Description |
| --- | --- |
| `$/.editorconfig` | No description provided. |
| `$/.github/workflows/ci.yml` | GitHub Actions workflow for building and testing the solution on PRs and non-master branch pushes. |
| `$/.github/workflows/publish-nuget-manual.yml` | Removed `Ama.Enterprise.P2p.Telemetry.Cli` from manual packing and automated pre-release cleanup arrays to halt its distribution temporarily. |
| `$/.github/workflows/publish-nuget.yml` | Reverted deployment steps for `Ama.Enterprise.P2p.Telemetry.Cli` removing it from active publication pipelines. |
| `$/.gitignore` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/Ama.Enterprise.CRDT.Distributed.IntegrationTests.csproj` | Updated reference from `Ama.Enterprise.UnitTests` to the renamed `Ama.Enterprise.Project.Tests.Common`. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/AntiEntropyStateSyncIntegrationTests.cs` | Replaced synchronous `ICrdtPatcher` usage with the asynchronous `IAsyncCrdtPatcher` to reflect updated internal patching pipelines supporting background thread delegation. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/BackgroundAndStorageIntegrationTests.cs` | Handled breaking DI changes by invoking `IApplicationPayloadHandler` avoiding obsolete generic envelopes during manual fallback sync testing. Fixed peer join test assertion to enforce direct anti-entropy calls. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/ClusterStatePersistenceIntegrationTests.cs` | Updated dictionary references fixing compile errors verifying `TombstonedReplicas` mappings evaluating DateTime timestamps explicitly instead of pure HashSets. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/ConcurrencyIntegrationTests.cs` | Integration tests verifying the lock-free multi-threaded command pooling and channel concurrency behaviors of the `CrdtDocumentOrchestrator` under heavy parallel loads. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/DocumentOrchestratorIntegrationTests.cs` | Integration tests verifying the multi-document orchestration matrix, dynamic CRDT lifecycle creation, and tombstoning limits. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/EvictionEdgeCasesIntegrationTests.cs` | Integration tests demonstrating and replicating the four edge case vulnerabilities associated with tombstoning, eviction data amnesia, unbounded journals, and snapshot overwrites. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/JournalingAndSnapshottingIntegrationTests.cs` | Upgraded to inject `IAsyncCrdtPatcher` across all operations and replaced naive snapshot override logic with definitive verification that merging isolated snapshots evaluates bound patches, maps removals and additions uniformly, and broadcasts intentions. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/MainServicesHappyPathIntegrationTests.cs` | Updated integration test to use `IApplicationPayloadHandler` instead of obsolete message handlers and cleaned up excessive comments. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/MultiReplicaSyncIntegrationTests.cs` | Added integration test verifying immediate evaluation and purging of expired tombstones during node initialization avoiding background loop delays. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/ScopeTopologyProviderIntegrationTests.cs` | Updated the test topology provider to implement the new async interface contract cleanly. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Ama.Enterprise.CRDT.Distributed.ShowCase.csproj` | Updated reference mapping the renamed `Ama.Enterprise.CRDT.MessagePack.SourceGenerators` compilation target. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Constants.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/DeviceStatus.cs` | Data structure representing the status of an IoT device. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/FleetState.cs` | Root CRDT document model representing fleet devices status, updated to inherit `IDistributedCrdtState`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/ShowCaseCrdtAotContext.cs` | CRDT AOT reflection context mapping types used by the showcase documents. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/ShowCaseJsonContext.cs` | AOT JSON context for the showcase multi-CRDT models. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/TaskItem.cs` | Data structure representing an individual task item. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/TaskListState.cs` | Root CRDT document model representing a task list, updated to inherit `IDistributedCrdtState`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Program.cs` | Refactored to leverage single strict unified storage DI setups. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/FleetManager.cs` | Replaced synchronous `ICrdtPatcher` interactions with `IAsyncCrdtPatcher`, updating operations and standardizing private method arrangements to comply with code boundaries. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/IFleetManager.cs` | Interface for managing the distributed fleet status CRDT document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ITaskManager.cs` | Interface for managing the distributed task list CRDT document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ShowCaseCrdtStorage.cs` | Extended bounded local single-file SQLite deployments tracking distributed network matrices alongside schema structures preventing offline deadlocks. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/TaskManager.cs` | Replaced synchronous `ICrdtPatcher` interactions with `IAsyncCrdtPatcher`, awaiting generation requests and repositioning private evaluation logic below structural boundaries. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Ama.Enterprise.CRDT.Distributed.TableStorage.csproj` | Updated to include and pack the solution-level README.md file as standard NuGet documentation resolving the `NU5046` package warning. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Extensions/ServiceCollectionExtensions.cs` | Simplified Table Storage DI extensions to strictly register a single unified scoped storage using `TryAddSingleton` to register default AOT JSON fallbacks safely bridging implementations. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Models/CrdtTableEntity.cs` | Refactored into a static helper directly mapping chunks onto the dictionary-backed `TableEntity` avoiding reflection-heavy SDK deserialization limits ensuring AOT compatibility. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Models/TableStorageCrdtOptions.cs` | Configuration structure holding Azure Table Storage endpoints and table bindings. Added `UseBinarySerialization` option to seamlessly pivot between human-readable JSON rows and cost-optimized high-performance binary structures without tight coupling to a specific format. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Services/TableStorageDistributedCrdtStorage.cs` | Bypassed generic reflection constraints replacing SDK maps with built-in dictionary-backed instances. Injects a decoupled standard `ICrdtSerializer` dynamically mapping binary serializers like MessagePack when enabled by the options configuration, bypassing rigid limitations safely. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Constants.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/DeviceStatus.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/FleetState.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/ShowCaseCrdtAotContext.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/ShowCaseJsonContext.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/ShowCaseNodeContext.cs` | Data structure encapsulating local session boundaries tracking active role and region. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/TaskItem.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Models/TaskListState.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Program.cs` | Refactored to cleanly separate P2P meshes per transport by introducing distinct 'server' (TCP/UDP) and 'user' (WebRTC) meshes, enforcing strict single-transport per mesh topologies while preserving multi-mesh bridging capabilities for CRDT synchronization. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/FleetManager.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/IFleetManager.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/ITaskManager.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/RbacMeshRoutingPolicy.cs` | Evaluates real-time P2P broadcast routing preventing Gossip payloads from leaking across multi-mesh bounds and enforcing Zero-Trust rules. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/RbacScopeTopologyProvider.cs` | Decoupled session evaluation boundaries executing structured `ShowCaseNodeContext`. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/ShowCaseCrdtStorage.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/ShowCaseTokenValidator.cs` | Refactored token validator evaluating structured session constraints to resolve Keyed DI construction limits. |
| `$/Ama.Enterprise.CRDT.Distributed.Topology.ShowCase/Services/TaskManager.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed/Ama.Enterprise.CRDT.Distributed.csproj` | Updated to include and pack the solution-level README.md file as standard NuGet documentation resolving the `NU5046` package warning. |
| `$/Ama.Enterprise.CRDT.Distributed/Extensions/ServiceCollectionExtensions.cs` | Bootstrapped `IScopeTopologyProvider` DI logic allowing decoupled causal scope exclusions tracking multi-mesh boundaries and avoiding hard references. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/ClusterStateSnapshotDto.cs` | Updated to map tombstoned replicas to their eviction timestamp tracking the exact eviction moment for safe cooldown expirations. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtEvictionRejectionMessage.cs` | Message broadcasted to forcefully reject and re-bootstrap nodes that have been tombstoned by the cluster, preventing amnesia edge cases. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMessageWrapper.cs` | Envelope wrapper mapping generic messages targeting specifically identified CRDT documents across the network topology. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtOperationsMessage.cs` | Transmission model conveying replicated CRDT intent patches targeted asynchronously across active nodes. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtPatchMessage.cs` | Transmission model conveying replicated CRDT patches targeted asynchronously across active nodes. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtRegistryEntry.cs` | Represents metadata about an active or tombstoned distributed CRDT document mapped via the global cluster registry. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtRegistryState.cs` | Global P2P synced directory state handling distributed multi-document topologies, ensuring active instantiation maps across nodes. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtSnapshotDataDto.cs` | DTO representing a serialized snapshot payload, avoiding tuple usage across generic bounds. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtSnapshotMessage.cs` | Message payload containing a complete materialized CRDT document snapshot, used as a fallback synchronization mechanism when log truncation gaps are detected. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtStateSyncMessage.cs` | Structure carrying generic synchronization states formatted across anti-entropy operations representing document DVV. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtOptions.cs` | Added `PeerTombstoneCooldownSeconds` configuring the exact cooldown duration before entirely purging a tombstoned replica from the cluster state tracking. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtP2pJsonContext.cs` | JSON serialization context mapping AOT bindings resolving eviction message constraints. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtReplicaRegistration.cs` | Represents a dynamically registered Replica ID enforcing discrete CRDT multi-mesh state architectures. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtSystemAotContext.cs` | AOT contextual reflection mapping for internal orchestrator registry CRDT scopes, bridging models. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtSystemJsonContext.cs` | Updated AOT JSON serialization context ensuring compatibility for explicitly tracked overarching cluster states. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/PooledDocumentCommand.cs` | Custom awaitable envelope implementing an IValueTaskSource object pool for the Single-Reader lock-free document channels, avoiding garbage blockages. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/PooledOrchestratorCommand.cs` | Custom awaitable envelope implementing an IValueTaskSource object pool for the lock-free Single-Reader orchestrator channel, executing commands sequentially and mapping constraints. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ClusterStateTracker.cs` | Updated `ImportState` and `CleanupExpiredTombstones` to bypass tombstone cleanup completely if `cooldown` is less than or equal to `TimeSpan.Zero`, treating the feature as explicitly disabled. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtCheckpointService.cs` | Extracted trimming and eviction logic to enforce SRP, allowing strict persistence cycles avoiding stalling delays. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtDocumentOrchestrator.cs` | Updated anti-entropy limits and fallback sync boundaries to `await IsPeerExpectedAsync` extracting bounds against decoupled active network states. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtEvictionService.cs` | Implementation of `ICrdtEvictionService` extracting the eviction logic, avoiding duplication. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtInitializationService.cs` | Updated extracting `DistributedCrdtOptions` propagating dynamic `tombstoneCooldown` durations directly feeding immediate startup evaluation tracking. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtMaintenanceService.cs` | Integrated the periodic tombstone cleanup invocation mapping directly tracking `PeerTombstoneCooldownSeconds`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DefaultScopeTopologyProvider.cs` | Updated to implement the asynchronous `IsPeerExpectedAsync` method returning synchronous wrappers. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DistributedCrdtDocument.cs` | Refactored snapshot merge logic utilizing `IAsyncCrdtPatcher` to evaluate true CRDT structural patch diffs instead of destructive state overwrites, and appended `PatchGenerated` invocation to broadcast merged state intentions. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DistributedCrdtScopeFactory.cs` | Factory mapping internal ServiceProvider boundaries generating isolated persistent generic structural boundaries. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DistributedCrdtScopeManager.cs` | Centralized singleton tracker managing long-lived background scopes per instantiated replica. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IClusterStateTracker.cs` | Updated `ImportState` signature explicitly passing the cooldown parameter ensuring accurate offline metric pruning dynamically mapping natively. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ICrdtDocumentOrchestrator.cs` | Generic manager facilitating multi-document runtime allocations, resolving decentralized P2P creation and deletion payloads. Updated Anti-Entropy state dispatch contract to support explicit target peer parameters. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ICrdtEvictionService.cs` | Interface for a dedicated service that orchestrates replica eviction and local identity re-bootstrapping across all CRDT documents. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtDocument.cs` | Removed tuple return type from `GetSnapshotDataAsync` returning a DTO to adhere to architecture rules. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtScope.cs` | Encapsulates the long-lived structural boundaries for a strictly identified generic localized CRDT replica. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtScopeFactory.cs` | Factory interface tracking CRDT scope instantiations and resolving bounds. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtStorage.cs` | Expanded unified bounds tracking defining dynamic state extraction and preservation for memory topologies. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDocumentFactory.cs` | AOT-friendly generic factory interface for resolving mapped distributed CRDT instances. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IScopeTopologyProvider.cs` | Converted `IsPeerExpected` to `IsPeerExpectedAsync` returning a `ValueTask<bool>` to allow asynchronous network and session lookups. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/MemoryCrdtStorage.cs` | Extended ephemeral storage mapping adhering to updated cluster tracker extraction mappings by storing cluster states, documents, and global version vectors. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtAntiEntropyService.cs` | Implemented network traffic smoothing jitter algorithms, preventing UDP/HTTP overflow "Thundering Herd" payload storms. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtP2pPayloadHandler.cs` | Extracted `IDirectMessageSender` to bound Anti-Entropy replies via targeted pushes preventing "Thundering Herd" broadcast storms. Evaluates targeted CRDT snapshots and drops concurrent DVV matrices preventing offline amnesia overwrites. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtTopologyObserver.cs` | Observes network connections and hooks into the core P2P protocols. Refactored new peer join events to trigger targeted state syncs directly to the new peer. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/StorageJournalForwarder.cs` | Implemented structured backpressure routines utilizing `PeriodicTimer`. Refactored `estimatedJournalCount` deductions post-trim ensuring real-time metric representation exposing backpressure limits without instantaneous resets. |
| `$/Ama.Enterprise.CRDT.MessagePack.IntegrationTests/Ama.Enterprise.CRDT.MessagePack.IntegrationTests.csproj` | Switched references to newly renamed `.SourceGenerators` and `.Tests.Common` projects correctly mapping logic. |
| `$/Ama.Enterprise.CRDT.MessagePack.IntegrationTests/Models/IntegrationTestModels.cs` | Included custom endpoint derivations evaluating `[JsonDerivedType]` bridges alongside convention fallback rules to extend STJ capabilities. |
| `$/Ama.Enterprise.CRDT.MessagePack.IntegrationTests/Services/MessagePackSerializerIntegrationTests.cs` | Added comprehensive integration testing capabilities evaluating dynamic Source Generator topological bindings resolving cross-boundary polymorphic models. |
| `$/Ama.Enterprise.CRDT.MessagePack.IntegrationTests/Services/TolerantReaderIntegrationTests.cs` | Integration tests validating standard STJ contextual mappings and MessagePack arrays tracking tolerant reader capabilities and executing DI resolutions. |
| `$/Ama.Enterprise.CRDT.MessagePack.SourceGenerators.UnitTests/Ama.Enterprise.CRDT.MessagePack.SourceGenerators.UnitTests.csproj` | Updated project reference to the renamed `Ama.Enterprise.CRDT.MessagePack.SourceGenerators` project. |
| `$/Ama.Enterprise.CRDT.MessagePack.SourceGenerators.UnitTests/Architecture/KnownContextsArchitectureTests.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.MessagePack.SourceGenerators/Ama.Enterprise.CRDT.MessagePack.SourceGenerators.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.MessagePack.SourceGenerators/Generators/MessagePackFormatterGenerator.cs` | Refactored the `MessagePackFormatterGenerator` to remove hardcoded dependency limits. Implemented dynamic assembly scanning for `JsonSerializerContext` boundaries. Replaced hardcoded polymorphic discriminator paths with convention mappings targeting standard configurations and extended capabilities to honor STJ `[JsonDerivedType]` bounds executed on user-defined endpoint classes. |
| `$/Ama.Enterprise.CRDT.MessagePack.UnitTests/Ama.Enterprise.CRDT.MessagePack.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.MessagePack.UnitTests/Formatters/CrdtPolymorphicMessagePackFormatterTests.cs` | Unit tests for `CrdtPolymorphicMessagePackFormatter` verifying array formatting logic and unregistered polymorphic bounds throwing. |
| `$/Ama.Enterprise.CRDT.MessagePack.UnitTests/Formatters/CrdtPolymorphicMessagePackRegistryTests.cs` | Unit tests verifying generic AOT delegate caching and resolution bounded in CrdtPolymorphicMessagePackRegistry. |
| `$/Ama.Enterprise.CRDT.MessagePack.UnitTests/Models/TestModel.cs` | Test model simulating a binary serializable DTO utilizing `MessagePackObject` annotations. |
| `$/Ama.Enterprise.CRDT.MessagePack.UnitTests/Services/MessagePackCrdtSerializerTests.cs` | Appended structural serialization boundaries verifying `byte[]` arrays convert to MessagePack BIN formats rather than polymorphic generics. |
| `$/Ama.Enterprise.CRDT.MessagePack/Ama.Enterprise.CRDT.MessagePack.csproj` | Modified analyzer bindings replacing references to use renamed `Ama.Enterprise.CRDT.MessagePack.SourceGenerators`. |
| `$/Ama.Enterprise.CRDT.MessagePack/Extensions/ServiceCollectionExtensions.cs` | Dependency injection extensions bootstrapping the decoupled custom `MessagePackCrdtSerializer` instance globally. Exposed `params` overload bridging multi-assembly mapped bounds and chaining execution fallback boundaries. |
| `$/Ama.Enterprise.CRDT.MessagePack/Formatters/CrdtPolymorphicMessagePackFormatter.cs` | Robust fallback polymorphic converter intercepting binary payload encoding/decoding mapping directly to `CrdtTypeRegistry` string bounds avoiding complex reflection. |
| `$/Ama.Enterprise.CRDT.MessagePack/Formatters/CrdtPolymorphicMessagePackRegistry.cs` | Centralized registry caching typed AOT polymorphic serialization delegates for MessagePack. |
| `$/Ama.Enterprise.CRDT.MessagePack/MessagePackCrdtSerializer.cs` | Implementation wrapping `ICrdtSerializer` targeting Native AOT structured MessagePack arrays and abstract Base64 fallbacks for string constraints. |
| `$/Ama.Enterprise.FeatureFlags.IntegrationTests/Ama.Enterprise.FeatureFlags.IntegrationTests.csproj` | Redirected shared tests project reference pointing to renamed `Ama.Enterprise.Project.Tests.Common`. |
| `$/Ama.Enterprise.FeatureFlags.IntegrationTests/Services/FeatureFlagDomainIntegrationTests.cs` | Integration tests verifying the single-node lifecycle, domain logic preservation, and CRDT bootstrapping capabilities of the Feature Flags module avoiding network sockets. |
| `$/Ama.Enterprise.FeatureFlags.ShowCase/Ama.Enterprise.FeatureFlags.ShowCase.csproj` | Appended an MSBuild target to automatically execute the local native Azurite startup batch script during the build process. Added `DisableFastUpToDateCheck` to ensure Visual Studio always evaluates the pre-build Azurite checks even when C# files haven't changed. |
| `$/Ama.Enterprise.FeatureFlags.ShowCase/Program.cs` | Updated application entry point substituting obsolete dummy declarations resolving Open Source configurations targeting mapped DI bindings. |
| `$/Ama.Enterprise.FeatureFlags.ShowCase/start-azurite.bat` | Windows batch script that automatically discovers and executes natively installed Azurite instances (via Visual Studio or NPM) bypassing Docker requirements, explicitly detached via PowerShell to prevent MSBuild hangs. |
| `$/Ama.Enterprise.FeatureFlags/Ama.Enterprise.FeatureFlags.csproj` | Updated to include and pack the solution-level README.md file as standard NuGet documentation resolving the `NU5046` package warning. |
| `$/Ama.Enterprise.FeatureFlags/Constants.cs` | Global constants for the Feature Flags module, defining common identifiers and document types. |
| `$/Ama.Enterprise.FeatureFlags/Extensions/ServiceCollectionExtensions.cs` | Extensions for registering feature flags components and background synchronization bootstrappers. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlag.cs` | Represents a single enterprise feature flag in the system incorporating audit and ownership structures. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagAudit.cs` | Audit trace structure tracking temporal modifications for a specific feature flag. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagMetadata.cs` | Metadata structure encapsulating enterprise multi-tenancy and product domain boundaries. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagOwnership.cs` | Ownership details specifying team contact routing for a feature flag. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagState.cs` | The root state dictionary representing active and tombstoned feature flags. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsCrdtAotContext.cs` | AOT reflection context for the Feature Flags CRDT models. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsJsonContext.cs` | AOT-friendly JSON context for Feature Flags serialization. |
| `$/Ama.Enterprise.FeatureFlags/Services/FeatureFlagBootstrapper.cs` | Background service initializing the singleton feature flags state across replica scopes. |
| `$/Ama.Enterprise.FeatureFlags/Services/FeatureFlagClusterManager.cs` | Implementation of the feature flag cluster manager interfacing with the orchestrator patching intent. |
| `$/Ama.Enterprise.FeatureFlags/Services/IFeatureFlagClusterManager.cs` | Interface for managing distributed feature toggles, evaluating state modifications and observing remote updates. |
| `$/Ama.Enterprise.Licensing.UnitTests/Ama.Enterprise.Licensing.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.Licensing.UnitTests/Services/CertificateLoaderTests.cs` | Unit tests verifying certificate loading capabilities from multiple inputs evaluating null constraints and cryptographic formats. |
| `$/Ama.Enterprise.Licensing.UnitTests/Services/HonorLicenseManagerTests.cs` | Evaluates generic honor checks mapping cryptography bounds. Updated to mock the internal property setter resolving internal bounds utilizing reflection circumventing the updated internal set boundaries. |
| `$/Ama.Enterprise.Licensing/Ama.Enterprise.Licensing.csproj` | Updated to include and pack the solution-level README.md file as standard NuGet documentation resolving the `NU5046` package warning. |
| `$/Ama.Enterprise.Licensing/Extensions/ServiceCollectionExtensions.cs` | Redesigned configuration pipeline splitting Open Source and Enterprise declarations into distinct APIs. Applied `[UnsupportedOSPlatform("browser")]` to the Enterprise configuration to enforce compile-time compiler warnings/errors on Blazor WebAssembly. |
| `$/Ama.Enterprise.Licensing/Models/DeclaredLicenseType.cs` | Enum defining user choices for license declaration (OpenSource, Enterprise, Unknown). |
| `$/Ama.Enterprise.Licensing/Models/LicenseOptions.cs` | Configuration structure holding license strings and extended options. Adjusted `DeclaredLicenseType` to utilize an internal set preventing external bypasses to the DI assignment bounds. |
| `$/Ama.Enterprise.Licensing/Models/LicensePayload.cs` | DTO representing the parsed JSON structure contained within an Enterprise license cryptographic payload defining boundaries. |
| `$/Ama.Enterprise.Licensing/Models/LicensingJsonContext.cs` | Source-generated AOT JSON serialization context ensuring parsing for licensing JSON payload structures. |
| `$/Ama.Enterprise.Licensing/Services/CertificateLoader.cs` | Updated certificate loading using `X509CertificateLoader` to fix obsolete constructor warnings. Added primary certificate extraction for PKCS12 collections, disposing of unused certificates to prevent memory leaks. |
| `$/Ama.Enterprise.Licensing/Services/HonorLicenseManager.cs` | Evaluates configured license parameters providing decoupled standalone logic. Integrated `OperatingSystem.IsBrowser()` runtime checks prohibiting the processing or declaration of Enterprise license materials within a client-side environment. |
| `$/Ama.Enterprise.Licensing/Services/ICertificateLoader.cs` | Interface for loading X.509 certificates from various origins. |
| `$/Ama.Enterprise.Licensing/Services/ILicenseManager.cs` | Interface separating generic honor-based logic capabilities avoiding tightly coupled bounds. |
| `$/Ama.Enterprise.Licensing/Services/LicenseStartupService.cs` | Starts up evaluations executing internal generic licensing checks. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Ama.Enterprise.P2p.AspNetCore.IntegrationTests.csproj` | Redirected shared tests project reference pointing to renamed `Ama.Enterprise.Project.Tests.Common`. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Services/AspNetCorePeerDiscoveryIntegrationTests.cs` | Updated integration test mapping evaluating `TargetHost`, `TargetPort`, and `TargetUseHttps` decoupling dynamically built `UriBuilder` HTTP routing, bypassing string concatenation vulnerabilities. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Services/AspNetCorePeerHandshakeIntegrationTests.cs` | Refactored integration tests verifying the decoupled ASP.NET Core Standalone peer handshaker discovering topologies. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Services/AspNetCoreTransportIntegrationTests.cs` | Upgraded end-to-end multi-mesh generic routing tests evaluating ASP.NET Core and Standalone structures. |
| `$/Ama.Enterprise.P2p.AspNetCore/Ama.Enterprise.P2p.AspNetCore.csproj` | Updated to include and pack the solution-level README.md file as standard NuGet documentation resolving the `NU5046` package warning. |
| `$/Ama.Enterprise.P2p.AspNetCore/Extensions/AspNetCoreDiscoveryServiceCollectionExtensions.cs` | Updated DI registrations to inject `IPeerRegistry` and `IFailureDetector` dependencies natively into the `AspNetCorePeerHandshaker` bounds. |
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
| `$/Ama.Enterprise.P2p.AspNetCore/Services/HttpInboundDispatcher.cs` | Removed rigid `ICrdtSerializer` injecting decoupled `IServiceProvider` extracting scoped keyed architectures executing internal formats. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/IHttpInboundDispatcher.cs` | Contract decoupling generic abstract HTTP routing frameworks. Consolidated from removed Http.Core package. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/CertificateNetworkIntegrationTests.cs` | Integration tests verifying network connectivity and drop routines mapping dynamically evaluated certificate authentication boundaries safely resolving decentralized topological rules. Appended E2E end-to-end multi-node tests verifying deduplication mechanics and dynamic adaptability when nodes join and drop using strict certificate authentication. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/Handlers/TestMessageHandler.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/Models/TestNode.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/P2pAdvancedIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/P2pNetworkIntegrationTests.cs` | Contains complex integration tests validating TCP binding, deduplications, and payload distributions. Removed obsolete direct `HttpClient` testing replacing it with `ITransportRouter`. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/P2pVersioningIntegrationTests.cs` | Integration tests verifying backwards compatibility and protocol versioning constraints. Upgraded to utilize TCP transports dropping obsolete HTTP bindings. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/PushPullGossipIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/QuicNetworkIntegrationTests.cs` | Added integration testing evaluating multiplexed encrypted QUIC TLS 1.3 behaviors validating standard networking flows. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/SessionRoutingIntegrationTests.cs` | Integration tests verifying end-to-end multi-mesh Zero-Trust routing policies tracking token validations, outbound drops, and inbound structural rejections implicitly. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/TcpNetworkIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/UdpNetworkIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/UdpPeerDiscoveryIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/WireEncryptionIntegrationTests.cs` | Integration tests validating End-to-End (E2E) AES-GCM data-in-transit wire encryption logic mapping dynamically without topology drops across shared network configurations. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Ama.Enterprise.P2p.IntegrationTests.csproj` | Redirected shared tests project reference pointing to renamed `Ama.Enterprise.Project.Tests.Common`. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Architecture/VersioningArchitectureTests.cs` | Architectural tests that parse the CI/CD deployment files ensuring specific deployed versions possess test coverage. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Discovery/DnsPeerDiscoveryIntegrationTests.cs` | Integration tests verifying DNS peer discovery resolves target domains and dispatches Phase 2 handshakes against discovered IPs. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Ama.Enterprise.P2p.Mqtt.IntegrationTests.csproj` | Redirected shared tests project reference pointing to renamed `Ama.Enterprise.Project.Tests.Common`. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Services/MqttPeerDiscoveryIntegrationTests.cs` | Integration tests verifying MQTT peer discovery mapping decoupled multi-mesh architectures. Migrated to use `TcpTransport` and `TcpPeerEndpoint` dropping obsolete HTTP transport bindings. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Services/MqttTransportIntegrationTests.cs` | Integration tests verifying end-to-end MQTT transport functionality evaluating isolated inbound subscriptions. |
| `$/Ama.Enterprise.P2p.Mqtt/Ama.Enterprise.P2p.Mqtt.csproj` | Updated to include and pack the solution-level README.md file as standard NuGet documentation resolving the `NU5046` package warning. |
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
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttTransport.cs` | Integrated unified format mapping encrypting outgoing structural streams properly gracefully structurally. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttTransportListener.cs` | Extracted the deserialization bridging directly via generic decoders securely handling incoming payloads safely structurally natively. |
| `$/Ama.Enterprise.P2p.Telemetry.Cli/Ama.Enterprise.P2p.Telemetry.Cli.csproj` | Updated analyzer generic tracking linking to the renamed `Ama.Enterprise.CRDT.MessagePack.SourceGenerators` bounds. |
| `$/Ama.Enterprise.P2p.Telemetry.Cli/Program.cs` | Refactored internal architecture replacing `Spectre.Console` bounds with `Terminal.Gui` structural mappings. Addressed UI framerate stuttering by shifting string building and data sorting to background threads avoiding blocking UI capabilities, while standardizing single-assignment `IList<string>` data sources circumventing layout recalculation storms present in `ObservableCollection`. Enforced standard coding practices removing prefixed fields, introducing null guard checks, and utilizing interfaces. |
| `$/Ama.Enterprise.P2p.Telemetry.IntegrationTests/Ama.Enterprise.P2p.Telemetry.IntegrationTests.csproj` | Redirected shared tests project reference pointing to renamed `Ama.Enterprise.Project.Tests.Common`. |
| `$/Ama.Enterprise.P2p.Telemetry.IntegrationTests/Services/TelemetryNetworkIntegrationTests.cs` | Updated to test custom meter boundaries by asserting on custom metric scopes configurations. |
| `$/Ama.Enterprise.P2p.Telemetry.UnitTests/Ama.Enterprise.P2p.Telemetry.UnitTests.csproj` | Unit tests project for validating P2P telemetry aggregations and metric extrapolation behaviors tracking .NET 10 time boundaries. |
| `$/Ama.Enterprise.P2p.Telemetry.UnitTests/Services/ClusterMetricsAggregatorTests.cs` | Unit tests validating the `ClusterMetricsAggregator` tracking time series histories calculating deltas avoiding logic errors. Appended verification bounds securing standard behavior mappings across Histograms, UpDownCounters, and monotonic metric bounds. |
| `$/Ama.Enterprise.P2p.Telemetry/Ama.Enterprise.P2p.Telemetry.csproj` | Updated to include and pack the solution-level README.md file as standard NuGet documentation resolving the `NU5046` package warning. |
| `$/Ama.Enterprise.P2p.Telemetry/Constants.cs` | Defines the core meter names evaluated by the telemetry network listeners isolating generic algorithms. |
| `$/Ama.Enterprise.P2p.Telemetry/Extensions/ServiceCollectionExtensions.cs` | Updated DI to map singletons for the `TelemetryPushProtocol` ensuring generic instance isolation matching background network hooks. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/ClusterMetricAggregation.cs` | Data structure representing the computed aggregated statistics for a specific metric across a cluster of nodes. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/MetricSnapshotDto.cs` | AOT friendly DTO holding scoped aggregated metrics structures decoupling logic. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/MetricTagDto.cs` | AOT friendly structure representing a metric dimension tracking generic mappings. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/TelemetryJsonContext.cs` | AOT generic bindings resolving metric serialization evaluating pure DTO constraints. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/TelemetryOptions.cs` | Updated to expose tracking configuration enabling generic meter dimension scopes via an `IEquatable` bounds implementation. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/TelemetryPayloadDto.cs` | Appended `MagicHeader` constant enabling fast-path binary stream identification prior to entering deserialization pipelines. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/ClusterMetricsAggregator.cs` | Thread-safe service responsible for computing rates, deltas, and multi-node aggregations over mapped telemetry boundaries. Fixed histogram and counter aggregation logic enforcing structural delta classifications, eliminating jumping sums and errant negative throughput derivatives. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/IClusterMetricsAggregator.cs` | Contract for aggregating cluster-wide telemetry metrics across active nodes tracking mathematical trends. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/ITelemetryAggregator.cs` | Interface for centrally aggregating and retrieving in-memory telemetry network states. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryAggregator.cs` | Thread-safe in-memory aggregator holding the latest telemetry network metrics. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryForwarderService.cs` | Prepended the fixed `MagicHeader` bytes to outgoing metric broadcasts ensuring compatibility with the fast-path telemetry boundary evaluation mechanism. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryPayloadHandler.cs` | Implemented binary header slicing, evaluating payloads for magic bytes and dropping unknown streams, bypassing heavy `MessagePackSerializationException` CPU spikes. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryPushAlgorithm.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Ama.Enterprise.P2p.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/Core/InMemoryPeerRegistryTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/Core/MessageDispatcherTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/Core/RandomPeerSelectorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/Core/TimeBasedFailureDetectorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore.IntegrationTests/Ama.Enterprise.P2p.WebRTC.AspNetCore.IntegrationTests.csproj` | Redirected shared tests project reference pointing to renamed `Ama.Enterprise.Project.Tests.Common`. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore.IntegrationTests/Services/WebRtcSignalingIntegrationTests.cs` | Extended integration coverage to assess strict ASP.NET Core WSS (Secure WebSockets) Standalone and Integrated HTTPS WebRTC out-of-band signaling negotiation tracking local X.509 definitions and avoiding handshake drops. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Ama.Enterprise.P2p.WebRTC.AspNetCore.csproj` | Updated to include and pack the solution-level README.md file as standard NuGet documentation resolving the `NU5046` package warning. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Extensions/EndpointRouteBuilderExtensions.cs` | Maps inbound WebRTC signaling endpoints managing WebSocket isolated handshakes negotiating SDP descriptors, reading route prefixes directly from injected settings. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Extensions/ServiceCollectionExtensions.cs` | Updated the dependency injection setup matching the newly integrated zero-trust dependencies (`IPeerAuthenticator`, `P2pNodeOptions`, `PeerEndpoint`, `IFailureDetector`) securing ASP.NET Core WebRTC components explicitly. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Models/WebRtcSignalingAction.cs` | Added `AuthRequest` and `AuthResponse` protocol actions orchestrating strict zero-trust explicit out-of-band WebSockets signaling. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Models/WebRtcSignalingOptions.cs` | Removed `IsEnabled` explicitly delegating standard activation directly through DI bounds ensuring cleanly decoupled tracking. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/IWebRtcHttpPeerDiscovery.cs` | Contract for a service that orchestrates the out-of-band WebRTC signaling workflow against a specific remote HTTP endpoint. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/IWebRtcSignalingClient.cs` | Updated negotiation signatures requiring local zero-trust generic authentication payloads. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/WebRtcHttpPeerDiscovery.cs` | Extended WebRTC HTTP peer discovery to construct mapped local payloads and extract `PeerId`, preventing connection-based amnesia bugs. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/WebRtcSignalingClient.cs` | Injected local `AuthRequest` mappings prior to signaling bounds, returning dynamic remote configurations. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/WebRtcSignalingServer.cs` | Bound zero-trust evaluations mapping incoming `AuthRequest`, tracking client `PeerId` and dropping disconnected `ConnectionId` references. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/WebRtcSignalingWsHelper.cs` | Internal helper wrapping WebSockets bounds isolating generic out-of-band envelope negotiations. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling.IntegrationTests/Ama.Enterprise.P2p.WebRTC.DistributedSignaling.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling.IntegrationTests/Services/WebRtcDistributedSignalingIntegrationTests.cs` | Refactored integration tests routing `replicaId` tracking explicit parameters instead of obsolete `meshId` mapping arrays. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Ama.Enterprise.P2p.WebRTC.DistributedSignaling.csproj` | Updated to include and pack the solution-level README.md file as standard NuGet documentation resolving the `NU5046` package warning. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Constants.cs` | Defines global constants such as document type aliases and default IDs for the WebRTC CRDT distributed signaling components. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Extensions/EndpointRouteBuilderExtensions.cs` | Refactored routes to require `replicaId` mapping distinct localized `ICrdtSignalingManager` from `DistributedCrdtScopeManager`, bypassing global mesh boundaries. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Extensions/ServiceCollectionExtensions.cs` | Updated WebRTC Distributed Signaling DI pipeline replacing global singleton tracking with `AddDistributedCrdtService`, scoping the manager to local CRDT replica bounds, bypassing multi-mesh overlap limits. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Models/CrdtSignalingState.cs` | Root CRDT document model representing the WebRTC out-of-band signaling state drop-box updating definitions reflecting dynamic target tracking properties matching presence scopes. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Models/DistributedSignalingAotContext.cs` | Appended standard CRDT payload structs (CrdtDocument, JournaledOperation, DottedVersionVector) exposing explicit AOT-friendly serialization mappings. Cleaned up duplicate Dictionary definitions and adjusted class to `public sealed`. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Models/DistributedSignalingCrdtAotContext.cs` | Native AOT mapping explicitly bounding WebRTC DTOs and multidimensional dictionary topologies securing the distributed serialization pipeline correctly. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Services/CrdtSignalingManager.cs` | Modified structural bounds enforcing mapped targeted dynamic keys handling specific intent behaviors across public logic pipelines. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Services/ICrdtSignalingManager.cs` | Contract managing the WebRTC out-of-band signaling drop-box updated tracking target presence mapping dictionary evaluation constraints. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Services/IWebRtcDistributedSignalingClient.cs` | Replaced `meshId` generic parameters with strict `replicaId` mappings matching isolated CRDT localized multi-replica bounds. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Services/IWebRtcSignalingOrchestrator.cs` | Appended `replicaId` tracking executing constraints alongside `meshId` tracking bounds matching local CRDT generic orchestrations. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Services/WebRtcDistributedSignalingClient.cs` | Updated HTTP signaling boundaries executing `replicaId` targeted bounds tracking multi-replica architectural routes avoiding unstructured multi-mesh amnesia limits. |
| `$/Ama.Enterprise.P2p.WebRTC.DistributedSignaling/Services/WebRtcSignalingOrchestrator.cs` | Updated `replicaId` injection scoping active background multi-party synchronization evaluations against multi-mesh tracking states targeting explicit bounds. |
| `$/Ama.Enterprise.P2p.WebRTC.IntegrationTests/Ama.Enterprise.P2p.WebRTC.IntegrationTests.csproj` | Redirected shared tests project reference pointing to renamed `Ama.Enterprise.Project.Tests.Common`. |
| `$/Ama.Enterprise.P2p.WebRTC.IntegrationTests/Services/WebRtcTransportIntegrationTests.cs` | Updated `TestMessage` implementing the newly enforced `ProtocolVersion` satisfying `IMeshMessage`. |
| `$/Ama.Enterprise.P2p.WebRTC/Ama.Enterprise.P2p.WebRTC.csproj` | Updated to include and pack the solution-level README.md file as standard NuGet documentation resolving the `NU5046` package warning. |
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
| `$/Ama.Enterprise.P2p/Ama.Enterprise.P2p.csproj` | Updated the NuGet description representing the core generic peer-to-peer mechanisms while extending metadata tag values to include 'networking'. |
| `$/Ama.Enterprise.P2p/Constants.cs` | Global constants for the P2P module, including protocol versions and payload size limits. |
| `$/Ama.Enterprise.P2p/Extensions/CertificateAuthenticatorServiceCollectionExtensions.cs` | Extension methods configuring the explicit dependency injection mapping for certificate-based peer authentication. |
| `$/Ama.Enterprise.P2p/Extensions/DnsDiscoveryServiceCollectionExtensions.cs` | Extension methods for registering DNS-based active peer discovery components isolated via Keyed dependencies to specific mesh profiles. |
| `$/Ama.Enterprise.P2p/Extensions/GossipNetworkServiceCollectionExtensions.cs` | Extension methods for registering generic Gossip algorithm components. |
| `$/Ama.Enterprise.P2p/Extensions/IP2pMeshBuilder.cs` | Interface for building and configuring specific Keyed DI mesh profiles. |
| `$/Ama.Enterprise.P2p/Extensions/P2pMeshBuilder.cs` | Implementation of `IP2pMeshBuilder` handling multi-mesh dependency injection tracking. |
| `$/Ama.Enterprise.P2p/Extensions/P2pMeshRegistrationTracker.cs` | Centralized tracking mechanism guaranteeing idempotent mesh registrations evaluating identical configurations, bypassing duplicates. |
| `$/Ama.Enterprise.P2p/Extensions/PushPullGossipNetworkServiceCollectionExtensions.cs` | Extension methods for registering generic Push-Pull Gossip algorithm components. |
| `$/Ama.Enterprise.P2p/Extensions/QuicTransportServiceCollectionExtensions.cs` | Dedicated P2P dependency injection extensions exposing decoupled QUIC transport configurations bounded seamlessly into the existing builder architectures. |
| `$/Ama.Enterprise.P2p/Extensions/ServiceCollectionExtensions.cs` | Extension methods for registering generic P2P meshes. Refactored to extract specific algorithm and transport registrations into separate distinct files. |
| `$/Ama.Enterprise.P2p/Extensions/SessionAuthenticationServiceCollectionExtensions.cs` | Dependency injection extensions to configure token-based session authentication for a P2P mesh. |
| `$/Ama.Enterprise.P2p/Extensions/TcpTransportServiceCollectionExtensions.cs` | Extension methods for registering TCP transports dynamically mapped to multi-mesh pipelines. |
| `$/Ama.Enterprise.P2p/Extensions/UdpDiscoveryServiceCollectionExtensions.cs` | Removed tightly coupled injected Handshaker Options isolating generic P2P mesh parameters decoupling configuration. |
| `$/Ama.Enterprise.P2p/Extensions/UdpTransportServiceCollectionExtensions.cs` | Extension methods for registering robust UDP datagram transports decoupled to multi-mesh pipelines. |
| `$/Ama.Enterprise.P2p/Extensions/WireEncoderServiceCollectionExtensions.cs` | Extension methods for registering structural wire encoders. Updated with explicit XML documentation warning developers about the security guarantees and transport prerequisites. |
| `$/Ama.Enterprise.P2p/Extensions/ZeroTrustRoutingServiceCollectionExtensions.cs` | Dependency injection extensions to configure zero-trust routing policies and decorators. |
| `$/Ama.Enterprise.P2p/Models/Algorithms/GossipMessage.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Algorithms/GossipMessageType.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Algorithms/GossipOptions.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Algorithms/PushPullGossipOptions.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/CertificateAuthenticatorOptions.cs` | Added `RevocationMode` property allowing configuration of X509 certificate revocation checks (e.g., NoCheck for air-gapped environments). |
| `$/Ama.Enterprise.P2p/Models/Core/FailureDetectorOptions.cs` | Configuration options for tuning generic protocol-agnostic failure detection components. |
| `$/Ama.Enterprise.P2p/Models/Core/IExtensibleDistributedPayload.cs` | Added `[JsonIgnore]` attribute to the `BinaryExtensionData` property explicitly avoiding System.Text.Json serializing empty or unmapped internal binary structures, correctly encapsulating STJ from MessagePack behaviors. |
| `$/Ama.Enterprise.P2p/Models/Core/IMeshMessage.cs` | Added required standardized `SenderId` bounding origin payloads decoupled traversing generic algorithms. |
| `$/Ama.Enterprise.P2p/Models/Core/P2pMeshMetadata.cs` | Metadata record registering a specific mesh identifier into the global dependency container for orchestration. |
| `$/Ama.Enterprise.P2p/Models/Core/P2pNodeOptions.cs` | Centralized generic configuration options holding the core node identity (ID and Endpoint) for the P2P Mesh. Updated to enforce a static, process-wide global peer identifier to satisfy repeatable idempotent tracker validations. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerEndpoint.cs` | Abstract base record for peer endpoints, configured with JSON polymorphic attributes mapping same-assembly derivatives to support standard AOT serialization. Updated tolerant reader payload properties to be nullable avoiding record equality reference failures. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerHandshakePayload.cs` | DTO encapsulating the peer identity and its specific authentication handshake payload natively during Phase 2 generic negotiations. |
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
| `$/Ama.Enterprise.P2p/Services/Algorithms/GossipAlgorithm.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Algorithms/PushPullGossipAlgorithm.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/ApplicationPayloadDispatcher.cs` | Composite orchestrator dispatching to abstract domain observers. |
| `$/Ama.Enterprise.P2p/Services/Core/CertificatePeerAuthenticator.cs` | Updated certificate chain validation to respect the configured `RevocationMode` from options, enabling offline support. |
| `$/Ama.Enterprise.P2p/Services/Core/DirectMessageSender.cs` | Implements localized targeted point-to-point generic delivery fetching active peering bindings avoiding overarching network broadcast storms. |
| `$/Ama.Enterprise.P2p/Services/Core/IApplicationPayloadDispatcher.cs` | Dispatches targeted application payloads. |
| `$/Ama.Enterprise.P2p/Services/Core/IApplicationPayloadHandler.cs` | Defines a domain-level consumer decoupling underlying distribution protocols. |
| `$/Ama.Enterprise.P2p/Services/Core/IDirectMessageSender.cs` | Defines a targeted point-to-point payload delivery contract decoupling anti-entropy processes from gossip epidemic broadcasts honoring the Single Responsibility Principle. |
| `$/Ama.Enterprise.P2p/Services/Core/IFailureDetector.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IInboundMessageQueue.cs` | Defines an internal queue for decoupling inbound network listeners from the protocol logic. |
| `$/Ama.Enterprise.P2p/Services/Core/IMeshRoutingPolicy.cs` | Interface for defining zero-trust routing policies that accept or reject messages based on session contexts. |
| `$/Ama.Enterprise.P2p/Services/Core/IMeshWireEncoder.cs` | Interface for encoding and decoding mesh messages. Updated with explicit XML documentation clarifying security limitations and HTTPS tunneling requirements. |
| `$/Ama.Enterprise.P2p/Services/Core/IP2pAlgorithm.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerAuthenticator.cs` | Expanded evaluating explicit outgoing local data arrays actively traversing generic topologies. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerDiscovery.cs` | Defines mechanisms for discovering other peers. Refactored to decouple active discovery probes and internal passive listeners from local background loops. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerHandshaker.cs` | Handshake interface bounds updated replacing generic identities mapping correctly utilizing explicit encapsulated payload DTO resolving generic explicit securely. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerRegistry.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerSelector.cs` | Interface for algorithms that select a generic subset of peers for communication. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerSessionRegistry.cs` | Contract mapping active Peer identifiers to their authenticated Session Context tracking live untrusted multi-mesh topologies. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerTopologyObserver.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/ISessionTokenValidator.cs` | Interface for validating session authorization tokens like JWTs during peer handshakes. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransport.cs` | Generic interface defining the outbound network transport capabilities, augmented with endpoint routing capabilities. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransportListener.cs` | Generic interface defining the inbound network listener capabilities for receiving protocol messages. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransportRouter.cs` | Interface for routing outgoing messages to the appropriate transport based on the endpoint type. |
| `$/Ama.Enterprise.P2p/Services/Core/InMemoryPeerRegistry.cs` | Implements an in-memory thread-safe registry tracking peering topology globally using a flat dictionary mapping. |
| `$/Ama.Enterprise.P2p/Services/Core/InMemoryPeerSessionRegistry.cs` | Thread-safe singleton mapping dynamically extracted session contexts per Peer across partitioned boundaries. |
| `$/Ama.Enterprise.P2p/Services/Core/InboundMessageQueue.cs` | Channel-backed implementation of the inbound message queue. |
| `$/Ama.Enterprise.P2p/Services/Core/MeshWireEncoder.cs` | Implements the wire encoding pipeline bridging raw serialization to optional AES-GCM encryption. Removed legacy fallback for unformatted payload and added strict unencrypted payload rejection when encryption is mandated, neutralizing downgrade attacks. |
| `$/Ama.Enterprise.P2p/Services/Core/PassThroughPeerAuthenticator.cs` | Implemented required generic local array boundaries resolving mapping. |
| `$/Ama.Enterprise.P2p/Services/Core/PolicyEnforcingPayloadDispatcher.cs` | Decorator implementation over the application dispatcher executing strict inbound policy boundaries preventing unauthorized data evaluation. |
| `$/Ama.Enterprise.P2p/Services/Core/PolicyEnforcingTransportRouter.cs` | Decorator implementation over the outbound transport router preventing malicious or unauthorized broadcasts to generic client sessions explicitly. |
| `$/Ama.Enterprise.P2p/Services/Core/RandomPeerSelector.cs` | Implementation of IPeerSelector utilizing random distribution selection. |
| `$/Ama.Enterprise.P2p/Services/Core/SessionPeerAuthenticator.cs` | Peer authenticator establishing and tracking zero-trust session-based boundaries via tokens instead of traditional mutual TLS certificates. |
| `$/Ama.Enterprise.P2p/Services/Core/TimeBasedFailureDetector.cs` | Implementation of IFailureDetector using abstract heartbeats decoupled from specific protocol options. |
| `$/Ama.Enterprise.P2p/Services/Core/TransportRouter.cs` | Composite transport router that delegates sending messages to the correct specific transport implementation. |
| `$/Ama.Enterprise.P2p/Services/Discovery/DnsPeerDiscovery.cs` | Encapsulated targeted structures reading arrays mapping dynamically. |
| `$/Ama.Enterprise.P2p/Services/Discovery/IDnsSrvResolver.cs` | Interface defining the contract for resolving DNS SRV records, allowing abstraction over third-party DNS packages. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpPeerDiscovery.cs` | Integrated Phase 2 negotiations executing `GetLocalHandshakeDataAsync` routing encapsulated structures mapping. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpPeerHandshaker.cs` | Updated UDP handshaker injecting registry and failure detector bounds. Evaluates inbound handshake requests dynamically establishing explicit bidirectional peer registration mimicking outbound mappings without relying on disconnected Phase 1 multicast discovery loops. |
| `$/Ama.Enterprise.P2p/Services/P2pHostedService.cs` | Orchestrating background service running Keyed P2P meshes globally. Refactored implementing direct execution control mapping discovery network polling loops isolating loops. |
| `$/Ama.Enterprise.P2p/Services/Transports/QuicTransport.cs` | High-performance natively multiplexed implementation of the ITransport explicitly mapping QUIC unidirectional streams avoiding standard head-of-line blocking inherently. |
| `$/Ama.Enterprise.P2p/Services/Transports/QuicTransportListener.cs` | Inbound decentralized listener evaluating distinct multiplexed QUIC streams enforcing native TLS 1.3 arrays naturally without third-party overrides. |
| `$/Ama.Enterprise.P2p/Services/Transports/TcpTransport.cs` | Replaced internal `ICrdtSerializer` serialization directly mapping via the secure generalized `IMeshWireEncoder` architecture. |
| `$/Ama.Enterprise.P2p/Services/Transports/TcpTransportListener.cs` | Upgraded generic stream extraction dynamically reading wire formatted encoded inputs. |
| `$/Ama.Enterprise.P2p/Services/Transports/UdpTransport.cs` | Bounded data-in-transit payloads traversing network streams through explicit generic structured formatting. |
| `$/Ama.Enterprise.P2p/Services/Transports/UdpTransportListener.cs` | Overhauled byte extraction evaluating mapped decryptions via injected structural `IMeshWireEncoder` dependencies. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/Ama.Enterprise.Project.Analyzers.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/DirectSerializationUsageAnalyzerTests.cs` | Unit tests for `DirectSerializationUsageAnalyzer` to ensure diagnostics are reported for `System.Text.Json` usages and ignored for correct generic interfaces. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/JsonSerializableExtensiblePayloadAnalyzerTests.cs` | Unit tests evaluating diagnostic scenarios verifying constraints validating generic enumerations, nested custom list structures, explicit system exclusions, and strictly tracked options bounds avoiding validation storms. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/PropertyInfoUsageAnalyzerTests.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/SystemConvertUsageAnalyzerTests.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/TaskDelayWithoutOptionsAnalyzerTests.cs` | Unit tests evaluating diagnostic evaluation scenarios for `TaskDelayWithoutOptionsAnalyzer`. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/ThreadSleepUsageAnalyzerTests.cs` | Unit tests evaluating diagnostic evaluation scenarios for `ThreadSleepUsageAnalyzer`. |
| `$/Ama.Enterprise.Project.Analyzers/Ama.Enterprise.Project.Analyzers.csproj` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/DirectSerializationUsageAnalyzer.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/JsonSerializableExtensiblePayloadAnalyzer.cs` | Roslyn diagnostic analyzer enforcing that all models injected into P2P `JsonSerializerContext` classes correctly implement the `IExtensibleDistributedPayload` interface, identifying options structures erroneously injected into AOT mapping arrays explicitly. |
| `$/Ama.Enterprise.Project.Analyzers/PropertyInfoUsageAnalyzer.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/SystemConvertUsageAnalyzer.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/TaskDelayWithoutOptionsAnalyzer.cs` | Roslyn diagnostic analyzer enforcing configurable options instead of hardcoded intervals within `Task.Delay` invocations. |
| `$/Ama.Enterprise.Project.Analyzers/ThreadSleepUsageAnalyzer.cs` | Roslyn diagnostic analyzer enforcing the prohibition of synchronous `Thread.Sleep` invocations to prevent thread starvation and poor asynchronous performance. |
| `$/Ama.Enterprise.Project.Tests.Common/Ama.Enterprise.Project.Tests.Common.csproj` | No description provided. |
| `$/Ama.Enterprise.Project.Tests.Common/Attributes/IntegrationFactAttribute.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Tests.Common/Attributes/TestedProtocolVersionAttribute.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Tests.Common/Extensions/XunitLoggingBuilderExtensions.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Tests.Common/Logging/XunitLogger.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Tests.Common/Logging/XunitLoggerProvider.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Tests.Common/Networking/NetworkResourceManager.cs` | No description provided. |
| `$/Ama.Enterprise.slnx` | Updated solution structure definitions tracking explicitly configured bounds explicitly modifying renamed structural instances naturally. |
| `$/CodingStandards.md` | No description provided. |
| `$/FilesDescription.md` | No description provided. |
| `$/LICENSE` | Dual-license agreement detailing the revenue-capped Community and Enterprise honor-based terms, updated to enforce strictly constrained GPLv3 or standard license bounds for forks and derivative works. |
| `$/README.md` | Primary introduction documentation explaining the core architecture, capabilities, getting started guide, and repository structure for the decentralized P2P toolkit. |
| `$/apps-todo.txt` | No description provided. |
| `$/p2p-mesh-architectures.md` | No description provided. |
| `$/solution.settings.json` | No description provided. |
