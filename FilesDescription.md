| File Path | Description |
| --- | --- |
| `$/.editorconfig` | No description provided. |
| `$/.github/workflows/ci.yml` | GitHub Actions workflow for building and testing the solution on PRs and non-master branch pushes. |
| `$/.github/workflows/publish-nuget-manual.yml` | GitHub Actions workflow for manually publishing stable releases to NuGet with explicit version inputs. |
| `$/.github/workflows/publish-nuget.yml` | GitHub Actions workflow for automatically publishing preview packages to NuGet upon pushing to the master branch. |
| `$/.gitignore` | No description provided. |
| `$/Ama.Enterprise.CRDT.Analyzers/Ama.Enterprise.CRDT.Analyzers.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/Ama.Enterprise.CRDT.Distributed.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/AntiEntropyStateSyncIntegrationTests.cs` | Integration tests verifying that state synchronization extracts unified missing operations. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/BackgroundAndStorageIntegrationTests.cs` | Handled breaking DI changes by invoking `IApplicationPayloadHandler` avoiding obsolete generic envelopes during manual fallback sync testing. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/DocumentOrchestratorIntegrationTests.cs` | Integration tests verifying the multi-document orchestration matrix, dynamic CRDT lifecycle creation, and tombstoning limits. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/EvictionEdgeCasesIntegrationTests.cs` | Integration tests explicitly demonstrating and replicating the four edge case vulnerabilities associated with tombstoning, eviction data amnesia, unbounded journals, and snapshot overwrites. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/JournalingAndSnapshottingIntegrationTests.cs` | Refactored tests to bypass obsolete document wrapper methods querying central synchronization services for missing journals. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/MainServicesHappyPathIntegrationTests.cs` | Updated integration test to use `IApplicationPayloadHandler` instead of obsolete message handlers and cleaned up excessive comments. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Ama.Enterprise.CRDT.Distributed.ShowCase.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Constants.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/DeviceStatus.cs` | Data structure representing the status of an IoT device. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/FleetState.cs` | Root CRDT document model representing fleet devices status, updated to inherit `IDistributedCrdtState`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/ShowCaseCrdtAotContext.cs` | CRDT AOT reflection context mapping types used by the showcase documents. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/ShowCaseJsonContext.cs` | AOT JSON context for the showcase multi-CRDT models. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/TaskItem.cs` | Data structure representing an individual task item. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/TaskListState.cs` | Root CRDT document model representing a task list, updated to inherit `IDistributedCrdtState`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Program.cs` | Refactored to natively leverage single strict implicit unified storage DI setups directly efficiently. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/FleetManager.cs` | Implementation handling intentions and queries for the fleet document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/IFleetManager.cs` | Interface for managing the distributed fleet status CRDT document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ITaskManager.cs` | Interface for managing the distributed task list CRDT document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ShowCaseCrdtStorage.cs` | Refactored to inject `DistributedCrdtReplicaRegistration` replacing obsolete options monitor and natively supporting static decoupled DI bounds. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/TaskManager.cs` | Implementation handling intentions and queries for the task list document. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Ama.Enterprise.CRDT.Distributed.TableStorage.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Extensions/ServiceCollectionExtensions.cs` | Simplified Table Storage DI extensions to strictly register a single unified scoped storage natively explicitly removing fragmented routing limits. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Models/CrdtTableEntity.cs` | Azure Table Storage entity model incorporating property chunking to persist payloads up to ~960KB. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Models/TableStorageCrdtOptions.cs` | Configuration structure holding Azure Table Storage endpoints and table bindings. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Services/TableStorageDistributedCrdtStorage.cs` | Azure Table Storage distributed backend implementing explicitly robust efficient native partition evaluations fetching constrained `GetJournalCountAsync` natively natively. |
| `$/Ama.Enterprise.CRDT.Distributed.UnitTests/Ama.Enterprise.CRDT.Distributed.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed/Ama.Enterprise.CRDT.Distributed.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed/Extensions/ServiceCollectionExtensions.cs` | Extension methods for bootstrapping CRDT dependencies. Updated to add dynamic ThresholdCompactionPolicyFactory resolutions utilizing `CompactionTtlSeconds`. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtEvictionRejectionMessage.cs` | Message broadcasted to forcefully reject and re-bootstrap nodes that have been tombstoned by the cluster, preventing amnesia edge cases. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMessageWrapper.cs` | Envelope wrapper mapping generic messages targeting specifically identified CRDT documents across the network topology. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtOperationsMessage.cs` | Transmission model conveying replicated CRDT intent patches targeted asynchronously across active nodes. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtRegistryEntry.cs` | Represents metadata about an active or tombstoned distributed CRDT document mapped via the global cluster registry. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtRegistryState.cs` | Global P2P synced directory state handling distributed multi-document topologies, ensuring active instantiation maps across nodes. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtSnapshotMessage.cs` | Message payload containing a complete materialized CRDT document snapshot, used as a fallback synchronization mechanism when log truncation gaps are detected. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtStateSyncMessage.cs` | Structure carrying generic synchronization states formatted across anti-entropy operations representing document DVV. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtOptions.cs` | Added `JournalTrimThreshold` property enforcing configurable aggressive journaling trimming bounds cleanly, alongside `CompactionTtlSeconds` enabling time-based garbage collection thresholds explicitly. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtP2pJsonContext.cs` | JSON serialization context mapping AOT bindings resolving eviction message constraints. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtReplicaRegistration.cs` | Represents a dynamically registered Replica ID enforcing discrete CRDT multi-mesh state architectures inherently. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtSystemAotContext.cs` | AOT contextual reflection mapping for internal orchestrator registry CRDT scopes, bridging models. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtSystemJsonContext.cs` | JSON serialization context guaranteeing AOT compatibility for internal orchestrator registry CRDT scopes. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ClusterStateTracker.cs` | Singleton thread-safe implementation capturing localized maps representing overarching remote state matrix limits. Modified `RemovePeerByNetworkId` preventing amnesia by intentionally preserving CRDT vectors, decoupling network routes and avoiding destructive structural gaps. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtCheckpointService.cs` | Background service responsible for periodic check-pointing. Updated to incorporate a configurable aggressive journal trim matching `JournalTrimThreshold` actively offloading unbounded limits natively utilizing snapshot synchronization. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtDocumentOrchestrator.cs` | Centralized generic orchestrator managing global localized active P2P CRDT document bindings. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtEvictionService.cs` | Implementation of `ICrdtEvictionService` extracting the eviction logic, avoiding duplication. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtInitializationService.cs` | Refactored to mutate the shared scope context in-place, closing the split-brain scope disconnect bug and avoiding destroyed instances. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DistributedCrdtDocument.cs` | Removed obsolete `GetMissingOperationsAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DistributedCrdtScopeFactory.cs` | Factory mapping internal ServiceProvider boundaries generating isolated persistent generic structural boundaries. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DistributedCrdtScopeManager.cs` | Centralized singleton tracker actively managing explicit long-lived background scopes per instantiated replica natively. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IClusterStateTracker.cs` | Tracks the last known synchronization bounds. Updated exposing logical tombstone identity mechanisms alongside `TombstonePeerByNetworkId` targeting topology exits. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ICrdtDocumentOrchestrator.cs` | Generic manager facilitating multi-document runtime allocations, resolving logical decentralized P2P creation and deletion payloads. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ICrdtEvictionService.cs` | Interface for a dedicated service that orchestrates replica eviction and local identity re-bootstrapping across all CRDT documents. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtDocument.cs` | Removed obsolete `GetMissingOperationsAsync` as journal resolutions are centralized. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtScope.cs` | Encapsulates the long-lived structural boundaries for a strictly identified generic localized CRDT replica. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtScopeFactory.cs` | Factory interface tracking explicit CRDT scope instantiations dynamically natively resolving bounds. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtStorage.cs` | Added `GetJournalCountAsync` dynamically allowing evaluation of bounds. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDocumentFactory.cs` | AOT-friendly generic factory interface for resolving mapped distributed CRDT instances. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/MemoryCrdtStorage.cs` | Ephemeral implementation natively evaluating `GetJournalCountAsync` quickly checking in-memory tracked objects cleanly securely. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtAntiEntropyService.cs` | Implemented network traffic smoothing jitter algorithms, preventing UDP/HTTP overflow "Thundering Herd" payload storms. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtP2pPayloadHandler.cs` | Updated extracting `IDirectMessageSender` strictly bounding Anti-Entropy replies via targeted pushes preventing "Thundering Herd" broadcast storms. Evaluates targeted CRDT snapshots dropping concurrent DVV matrices preventing fatal offline amnesia overwrites. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtTopologyObserver.cs` | Observes network connections and hooks into the core P2P protocols. Refactored resolving `Departed` topology states with instant tombstones freeing log restrictions, while protecting `Dead` topology traces for offline synchronization. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/StorageJournalForwarder.cs` | Injects localized implementations mapping back into underlying internal storage architectures, acting as the interface wrapper over globally active pipelines. |
| `$/Ama.Enterprise.CRDT.MemoryPack/Ama.Enterprise.CRDT.MemoryPack.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.TableStorage/Ama.Enterprise.CRDT.TableStorage.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.TableStorage/todo.txt` | No description provided. |
| `$/Ama.Enterprise.CRDT.Testing/Ama.Enterprise.CRDT.Testing.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Testing/todo.txt` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags.IntegrationTests/Ama.Enterprise.FeatureFlags.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags.ShowCase/Ama.Enterprise.FeatureFlags.ShowCase.csproj` | Showcase console application project displaying P2P feature flags integration, AOT readiness, and UDP cluster discovery. |
| `$/Ama.Enterprise.FeatureFlags.ShowCase/Program.cs` | Updated application entry point resolving `IFeatureFlagClusterManager` cleanly via isolated structural scopes avoiding root provider errors targeting mapped singletons securely. |
| `$/Ama.Enterprise.FeatureFlags.UnitTests/Ama.Enterprise.FeatureFlags.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags/Ama.Enterprise.FeatureFlags.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags/Extensions/ServiceCollectionExtensions.cs` | Refactored `AddFeatureFlags` signature enforcing explicit `replicaId` tracking dynamically configuring underlying singleton bounds identically across mesh architectures. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlag.cs` | Data structure representing a single feature flag. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagOptions.cs` | Configuration structure for feature flags. Removed hardcoded network abstractions, delegating topology management dynamically to the host. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagState.cs` | Inherits `IDistributedCrdtState` and maps generic constraints bridging properties. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsCrdtAotContext.cs` | AOT context for the feature flags models. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsJsonContext.cs` | JSON context for the feature flags models. |
| `$/Ama.Enterprise.FeatureFlags/Services/FeatureFlagBootstrapper.cs` | Refactored bootstrapping sequence evaluating multi-replica dynamic structural scopes efficiently overriding obsolete provider limits natively. |
| `$/Ama.Enterprise.FeatureFlags/Services/FeatureFlagClusterManager.cs` | Refactored internal DI boundaries utilizing mapped orchestrators ensuring generic dynamic models are cached, decoupling interface requirements. |
| `$/Ama.Enterprise.FeatureFlags/Services/IFeatureFlagClusterManager.cs` | Interface for the feature flag cluster manager. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Ama.Enterprise.P2p.AspNetCore.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Services/AspNetCorePeerHandshakeIntegrationTests.cs` | Refactored integration tests verifying the decoupled ASP.NET Core Standalone peer handshaker explicitly natively natively discovering topologies securely and reliably efficiently natively. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Services/AspNetCoreTransportIntegrationTests.cs` | Upgraded end-to-end multi-mesh generic routing tests testing ASP.NET Core explicitly evaluating distinct bounds properly actively evaluating Standalone structures efficiently natively natively securely explicitly structurally. |
| `$/Ama.Enterprise.P2p.AspNetCore/Ama.Enterprise.P2p.AspNetCore.csproj` | Consolidated package natively mapping localized endpoints alongside Kestrel transport implementations into a unified ASP.NET HTTP structure. |
| `$/Ama.Enterprise.P2p.AspNetCore/Extensions/AspNetCoreDiscoveryServiceCollectionExtensions.cs` | Extension methods for safely registering generic ASP.NET Core peer handshaker components securely tracking specific internal multi-mesh routing architectures natively dynamically. |
| `$/Ama.Enterprise.P2p.AspNetCore/Extensions/EndpointRouteBuilderExtensions.cs` | Refactored ASP.NET Core endpoint routing to use explicit `RequestDelegate` handlers extracting routing context explicitly, effectively resolving AOT serialization and trimming warnings. |
| `$/Ama.Enterprise.P2p.AspNetCore/Extensions/ServiceCollectionExtensions.cs` | Isolated configuration hooking handlers dynamically mapping AOT generic traits. Merged Kestrel and Http.Core extensions internally tracking singleton mappings natively. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCoreHandshakeOptions.cs` | Configuration structure for isolated ASP.NET Core handshaking parameters explicitly tracking decoupled Integrated and Standalone topological bounding natively. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCoreHostingMode.cs` | Determines whether the ASP.NET Core P2P transport relies on the host application's HTTP pipeline explicitly routed via MapP2pMeshEndpoints() or spins up an isolated Standalone Kestrel web server internally natively decoupled. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCoreJsonContext.cs` | Generates isolated interoperability processing structured models natively. Appended Kestrel bounds resolving AOT serialization bounds purely. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCorePeerEndpoint.cs` | Identifies and bridges standard inbound external mapping boundaries and explicitly routed ports. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCoreTransportOptions.cs` | Allows explicitly assigning network bounds decoupling public mappings distinctly isolating host constraints structurally. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/HttpPayloadProcessResult.cs` | Enumeration explicitly identifying deterministic HTTP processing outcomes. Consolidated from removed Http.Core package. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/AspNetCoreTransport.cs` | Adjusted fallback outbound routing path boundaries natively mapping explicit standardized prefixes isolating domains seamlessly securely. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/AspNetCoreTransportListener.cs` | Refactored standalone listener initialization actively formatting standard library path constraints guaranteeing distinct execution spaces globally gracefully natively. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/Discovery/AspNetCorePeerHandshaker.cs` | Updated Standalone HTTP topology probes falling back into standard `ama-enterprise` routing prefix scopes preventing implicit collisions gracefully effectively. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/HttpInboundDispatcher.cs` | Thread-safe generic centralized HTTP orchestrator extracting mapped inbound pipelines isolated exclusively. Consolidated from removed Http.Core package. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/IHttpInboundDispatcher.cs` | Contract decoupling generic abstract HTTP routing frameworks natively decoupling boundaries safely. Consolidated from removed Http.Core package. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/Handlers/TestMessageHandler.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/Models/TestNode.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/P2pAdvancedIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/P2pNetworkIntegrationTests.cs` | Contains complex integration tests validating actual TCP binding natively, deduplications, and payload distributions. Removed obsolete direct `HttpClient` testing replacing it with `ITransportRouter` explicitly correctly. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/P2pVersioningIntegrationTests.cs` | Integration tests verifying backwards compatibility and protocol versioning constraints. Upgraded to utilize strict TCP transports securely dropping obsolete HTTP explicit bindings. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/PushPullGossipIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/TcpNetworkIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/UdpNetworkIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Algorithms/UdpPeerDiscoveryIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Ama.Enterprise.P2p.IntegrationTests.csproj` | Integration tests project for validating P2P networking components via HTTP loopbacks. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Architecture/VersioningArchitectureTests.cs` | Architectural tests that parse the CI/CD deployment files ensuring specific deployed versions always possess explicit test coverage. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Discovery/DnsPeerDiscoveryIntegrationTests.cs` | Integration tests verifying DNS peer discovery natively resolves target domains and dispatches accurate Phase 2 handshakes against discovered IPs. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Services/Core/LicenseManagerIntegrationTests.cs` | Integration tests verifying cryptographic RSA signature validations for the generic honor-based license manager. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Ama.Enterprise.P2p.Mqtt.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Services/MqttPeerDiscoveryIntegrationTests.cs` | Integration tests verifying MQTT peer discovery mapping decoupled multi-mesh architectures natively. Migrated to use `TcpTransport` and `TcpPeerEndpoint` dynamically correctly correctly correctly dropping obsolete HTTP transport bindings. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Services/MqttTransportIntegrationTests.cs` | Integration tests verifying end-to-end MQTT transport functionality evaluating isolated inbound subscriptions. |
| `$/Ama.Enterprise.P2p.Mqtt/Ama.Enterprise.P2p.Mqtt.csproj` | Project definition for MQTT transport using MQTTnet compatible with AOT serialization constraints. |
| `$/Ama.Enterprise.P2p.Mqtt/Extensions/MqttDiscoveryServiceCollectionExtensions.cs` | Updated to guarantee Mqtt JSON and Discovery JSON AOT serialization bindings execute for decoupled discovery capabilities. |
| `$/Ama.Enterprise.P2p.Mqtt/Extensions/ServiceCollectionExtensions.cs` | Extracted MQTT JSON AOT polymorphic registrations into a reusable idempotent block tracking distinct bounds, ensuring robust generic network initializations. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttDiscoveryJsonContext.cs` | JSON serialization context for Phase 1 and Phase 2 generic discovery primitives in MQTT. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttDiscoveryMessage.cs` | Modified payload natively projecting active local Phase 2 routing pseudo-ports ensuring decoupled message isolation locally. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttDiscoveryOptions.cs` | Configuration options for tuning active MQTT peer discovery broadcast intervals and topic suffixes. Now decoupled from the generic MQTT transports, providing isolated broker connection settings for discovery architectures. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttHandshakeMessage.cs` | Data structure representing the Phase 2 MQTT network handshake payload exchange. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttHandshakeOptions.cs` | Appended explicit local `HandshakePort` parameter isolating active connection profiles avoiding nested IP overlapping bounds locally. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttJsonContext.cs` | Source-generated AOT JSON serialization context for the MQTT endpoint model structure. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttPeerEndpoint.cs` | Inherited PeerEndpoint model representing an isolated MQTT destination node defined by its internal client identity identifier. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttRoutingEndPoint.cs` | Custom end point representing an MQTT routing target identifying an explicit client. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttTransportOptions.cs` | Configuration record setting broker connection host endpoints credentials and specific topic routing bounds. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/Discovery/MqttPeerDiscovery.cs` | Implementation of MQTT peer discovery. Updated to actively handshake and register incoming discovery peers when receiving broadcasts, avoiding one-sided peer connections. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/Discovery/MqttPeerHandshaker.cs` | Refactored implementing interface bindings exposing configured mapped localized endpoints natively. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/IMqttClientManager.cs` | Interface establishing lifecycle controls for individual MQTT client subscriptions and active payloads publications. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttClientManager.cs` | Updated to strictly isolate topic subscriptions and client connection IDs by injecting the explicit `meshId`, preventing cross-mesh broker collisions. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttTransport.cs` | Outbound mapping evaluating strict decoupled MQTT client managers natively. Refactored input validatons explicitly. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttTransportListener.cs` | Inbound listener registering standard network hooks dynamically bound to mapped multi-mesh brokers securely. |
| `$/Ama.Enterprise.P2p.Telemetry.Cli.UnitTests/Ama.Enterprise.P2p.Telemetry.Cli.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.Telemetry.Cli/Ama.Enterprise.P2p.Telemetry.Cli.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.Telemetry.Cli/Program.cs` | Console application serving as a live in-place telemetry dashboard, connecting to a P2P mesh and displaying dynamically aggregated metrics. Replaced custom console dashboard logic with Terminal.Gui providing a native scrollable table, client list, and total active peer aggregations. Updated to intersect incoming telemetry endpoints with `IPeerRegistry` state, explicitly dropping inactive/dead nodes from UI aggregations seamlessly natively. |
| `$/Ama.Enterprise.P2p.Telemetry.IntegrationTests/Ama.Enterprise.P2p.Telemetry.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.Telemetry.IntegrationTests/Services/TelemetryNetworkIntegrationTests.cs` | Updated to test custom meter boundaries by asserting on custom metric scopes native configurations natively. |
| `$/Ama.Enterprise.P2p.Telemetry.UnitTests/Ama.Enterprise.P2p.Telemetry.UnitTests.csproj` | Unit tests project for validating P2P telemetry aggregations and metric extrapolation behaviors natively tracking .NET 10 time boundaries. |
| `$/Ama.Enterprise.P2p.Telemetry.UnitTests/Services/ClusterMetricsAggregatorTests.cs` | Unit tests validating the `ClusterMetricsAggregator` tracking time series histories natively calculating deltas dynamically correctly avoiding logic errors. Appended explicit verification bounds securing standard behavior mappings across Histograms, UpDownCounters, and strict monotonic metric bounds organically. |
| `$/Ama.Enterprise.P2p.Telemetry/Ama.Enterprise.P2p.Telemetry.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.Telemetry/Constants.cs` | Defines the core explicit meter names dynamically evaluated by the telemetry network listeners isolating generic algorithms. |
| `$/Ama.Enterprise.P2p.Telemetry/Extensions/ServiceCollectionExtensions.cs` | Updated DI to map singletons for the explicit `TelemetryPushProtocol` ensuring bounded generic instance isolation matching background network hooks. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/ClusterMetricAggregation.cs` | Data structure representing the computed aggregated statistics for a specific metric across a cluster of nodes. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/MetricSnapshotDto.cs` | AOT friendly pure DTO strictly holding explicitly scoped aggregated metrics structures decoupling logic safely. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/MetricTagDto.cs` | AOT friendly structure representing a metric precise dimension extracted tracking generic mappings actively. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/TelemetryJsonContext.cs` | Isolated AOT generic bindings resolving metric serialization safely evaluating pure DTO constraints natively. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/TelemetryOptions.cs` | Updated to expose explicit tracking configuration enabling native generic meter dimension scopes via a robust `IEquatable` bounds implementation. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/TelemetryPayloadDto.cs` | DTO defining the top-level explicitly network transmission payloads natively wrapping internally batched generic metrics safely mapping AOT configurations. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/ClusterMetricsAggregator.cs` | Thread-safe service responsible for computing rates, deltas, and multi-node aggregations over mapped telemetry boundaries natively. Fixed histogram and counter aggregation logic enforcing strict structural delta classifications, fundamentally eliminating jumping sums and errant negative throughput derivatives. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/IClusterMetricsAggregator.cs` | Contract for aggregating cluster-wide telemetry metrics across active nodes natively tracking mathematical trends. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/ITelemetryAggregator.cs` | Interface for centrally aggregating and retrieving in-memory telemetry network states. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryAggregator.cs` | Thread-safe in-memory aggregator holding the latest telemetry network metrics. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryForwarderService.cs` | Replaced hardcoded telemetry matching metrics with configurable scopes mapping native user boundaries explicitly. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryPayloadHandler.cs` | Application payload handler dedicated to processing incoming telemetry network payloads and routing them into the centralized aggregator. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryPushAlgorithm.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Ama.Enterprise.P2p.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/Core/InMemoryPeerRegistryTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/Core/MessageDispatcherTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/Core/RandomPeerSelectorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/Core/TimeBasedFailureDetectorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore.IntegrationTests/Ama.Enterprise.P2p.WebRTC.AspNetCore.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore.IntegrationTests/Services/WebRtcSignalingIntegrationTests.cs` | Added explicit integration tests for generic out-of-band discovery scenarios spanning across Standalone, Integrated, and offline disconnected peers. Adapted the builder initialization to map the newly decoupled discovery registration. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Ama.Enterprise.P2p.WebRTC.AspNetCore.csproj` | Fixed the `PackageId` property which was identical to `Ama.Enterprise.P2p.WebRTC`, causing a circular dependency error during build. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Extensions/EndpointRouteBuilderExtensions.cs` | Refactored WebRTC signaling endpoints to use explicit `RequestDelegate` configurations parsing routing constraints directly, eliminating runtime reflection constraints ensuring strict AOT compatibility. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Extensions/ServiceCollectionExtensions.cs` | Split the WebRTC ASP.NET Core dependency injection extension into two distinct isolated registrations, enabling granular decoupled flexibility between inbound server topologies and outbound HTTP discovery peers natively. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Models/WebRtcSignalingOptions.cs` | Configuration structure for isolated ASP.NET Core WebRTC signaling HTTP parameters explicitly tracking decoupled Integrated and Standalone bounds. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/IWebRtcHttpPeerDiscovery.cs` | Contract for a service that orchestrates the out-of-band WebRTC signaling workflow against a specific remote HTTP endpoint natively. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/IWebRtcSignalingClient.cs` | Interface for interacting with native peer HTTP API endpoints initiating decoupled WebRTC out-of-band SDP negotiations cleanly. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/WebRtcHttpPeerDiscovery.cs` | Updated to inject `IPeerRegistry`, `IPeerAuthenticator`, and `IFailureDetector` enforcing immediate structural mapping of explicitly discovered out-of-band WebRTC connections mirroring the UDP discovery flow securely. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/WebRtcSignalingClient.cs` | Implements `IWebRtcSignalingClient` using robust `IHttpClientFactory` abstractions safely targeting AOT serialization mapping SDP offers and answers globally. |
| `$/Ama.Enterprise.P2p.WebRTC.AspNetCore/Services/WebRtcSignalingServer.cs` | Implementation managing isolated ASP.NET Core HTTP out-of-band WebRTC signaling streams evaluating Integrated and Standalone modes. |
| `$/Ama.Enterprise.P2p.WebRTC.IntegrationTests/Ama.Enterprise.P2p.WebRTC.IntegrationTests.csproj` | Added project references mapping MQTT signaling capabilities alongside copying explicit local settings explicitly. |
| `$/Ama.Enterprise.P2p.WebRTC.IntegrationTests/Services/WebRtcTransportIntegrationTests.cs` | Updated `TestMessage` implementing the newly enforced `ProtocolVersion` satisfying `IMeshMessage`. |
| `$/Ama.Enterprise.P2p.WebRTC/Ama.Enterprise.P2p.WebRTC.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.WebRTC/Extensions/ServiceCollectionExtensions.cs` | WebRTC dependency injection pipeline configuring underlying STUN models mapping transports alongside base generic Gossip meshes, updated to inject dynamic cross-assembly JSON polymorphism resolvers. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcHandshakeMessage.cs` | In-band signaling structure notifying local identity topologies through initialized WebRTC channels. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcInvitationAnswer.cs` | DTO representing a WebRTC invitation answer containing the connection identifier and the SDP answer string. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcInvitationOffer.cs` | DTO representing a WebRTC invitation offer containing the connection identifier and the SDP offer string. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcJsonContext.cs` | Baseline STJ context generation tracking WebRTC handshake protocols. Updated to include `WebRtcInvitationOffer` and `WebRtcInvitationAnswer` DTOs for AOT serialization. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcOptions.cs` | Configuration structure holding explicit ICE servers natively bound to AOT-friendly serialization mappings. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcPeerEndpoint.cs` | WebRTC data channel connection endpoint identifying uniquely scoped connection states. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/IWebRtcConnectionManager.cs` | Interface isolating abstract signaling scopes exposing Data Channel inbound references. Updated to expose WebRTC connection state changes targeting UI subscriptions. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/IWebRtcInvitationService.cs` | Generic mechanism exchanging SDP structures. Updated to use DTOs instead of tuples for SDP exchange. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcConnectionManager.cs` | Implements the management of WebRTC connections and out-of-band signaling. Added native System.Diagnostics.Metrics tracking for connection lifecycles. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcTransport.cs` | WebRTC specific implementation handling outbound gossip structures wrapping targeted connection payloads. Integrated standard metrics telemetry exposing outbound throughput payload boundaries natively. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcTransportListener.cs` | WebRTC specific listener registering asynchronous bindings targeting decentralized peer streams. Integrated robust metrics monitoring tracking inbound payloads and message volumes. |
| `$/Ama.Enterprise.P2p/Ama.Enterprise.P2p.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p/Constants.cs` | Global constants for the P2P module, including protocol versions and payload size limits. |
| `$/Ama.Enterprise.P2p/Extensions/DnsDiscoveryServiceCollectionExtensions.cs` | Extension methods for registering DNS-based active peer discovery components isolated via Keyed dependencies to specific mesh profiles. |
| `$/Ama.Enterprise.P2p/Extensions/IP2pMeshBuilder.cs` | Interface for building and configuring specific Keyed DI mesh profiles. |
| `$/Ama.Enterprise.P2p/Extensions/P2pMeshBuilder.cs` | Implementation of `IP2pMeshBuilder` handling multi-mesh dependency injection tracking. |
| `$/Ama.Enterprise.P2p/Extensions/P2pMeshRegistrationTracker.cs` | Centralized tracking mechanism guaranteeing idempotent mesh registrations evaluating structurally identical configurations, bypassing duplicates. |
| `$/Ama.Enterprise.P2p/Extensions/ServiceCollectionExtensions.cs` | Extension methods updated with non-intrusive honor-based global licensing hooks natively tracking meshes explicitly. |
| `$/Ama.Enterprise.P2p/Extensions/UdpDiscoveryServiceCollectionExtensions.cs` | Removed tightly coupled injected Handshaker Options isolating generic P2P mesh parameters decoupling explicitly. |
| `$/Ama.Enterprise.P2p/Models/Core/FailureDetectorOptions.cs` | Configuration options for tuning generic protocol-agnostic failure detection components. |
| `$/Ama.Enterprise.P2p/Models/Core/IMeshMessage.cs` | Added required standardized `SenderId` bounding origin payloads explicitly decoupled traversing generic algorithms identically. |
| `$/Ama.Enterprise.P2p/Models/Core/LicenseOptions.cs` | Configuration options for tracking the honor-based license setup explicitly, extended with public key cryptography configurations. |
| `$/Ama.Enterprise.P2p/Models/Core/P2pMeshMetadata.cs` | Metadata record registering a specific mesh identifier into the global dependency container for orchestration. |
| `$/Ama.Enterprise.P2p/Models/Core/P2pNodeOptions.cs` | Centralized generic configuration options holding the core node identity (ID and Endpoint) for the P2P Mesh. Updated to enforce a static, process-wide global peer identifier to satisfy repeatable idempotent tracker validations. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerEndpoint.cs` | Abstract base record for peer endpoints, configured with JSON polymorphic attributes mapping same-assembly derivatives to support standard AOT serialization. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerId.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerNode.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerStatus.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Discovery/DnsDiscoveryOptions.cs` | Configuration options for DNS-based peer discovery, extended to support SRV record resolution flags. |
| `$/Ama.Enterprise.P2p/Models/Discovery/SrvRecordTarget.cs` | Data structure representing a resolved target hostname and port from a DNS SRV query. |
| `$/Ama.Enterprise.P2p/Models/Discovery/UdpDiscoveryMessage.cs` | Introduced `HandshakePort` property natively mapping dynamically assigned Phase 2 protocol sockets. |
| `$/Ama.Enterprise.P2p/Models/Discovery/UdpDiscoveryOptions.cs` | Removed the AdvertisedHandshakePort, standardizing decoupled generic mesh boundaries. |
| `$/Ama.Enterprise.P2p/Models/Discovery/UdpHandshakeOptions.cs` | Removed obsolete `TargetPort` decoupling configurations enabling generic mapped payload allocations dynamically discovering inbound target bounds. |
| `$/Ama.Enterprise.P2p/Models/Gossip/GossipMessage.cs` | Incorporated `ProtocolVersion` maintaining fallback compatibility. |
| `$/Ama.Enterprise.P2p/Models/Gossip/GossipMessageType.cs` | Defines explicit message typings enabling push-pull sync interactions, distinguishing broadcasts. |
| `$/Ama.Enterprise.P2p/Models/Gossip/GossipOptions.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Gossip/PushPullGossipOptions.cs` | Dedicated configuration options decoupling the Push-Pull mechanisms without polluting the pure baseline primitives. |
| `$/Ama.Enterprise.P2p/Models/P2pJsonSerializerContext.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Transports/TcpPeerEndpoint.cs` | Represents a TCP network address endpoint. |
| `$/Ama.Enterprise.P2p/Models/Transports/TcpTransportOptions.cs` | Configuration options explicitly bound for configuring active TCP transport connectivity. Implemented IEquatable to comply with the standard bounding. |
| `$/Ama.Enterprise.P2p/Models/Transports/UdpPeerEndpoint.cs` | Represents a UDP network address endpoint. |
| `$/Ama.Enterprise.P2p/Models/Transports/UdpTransportOptions.cs` | Configuration options explicitly bound for configuring active UDP datagram connectivity. Implemented IEquatable to comply with the standard bounding. |
| `$/Ama.Enterprise.P2p/Services/Algorithms/GossipAlgorithm.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Algorithms/PushPullGossipAlgorithm.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/ApplicationPayloadDispatcher.cs` | Composite orchestrator dispatching to abstract domain observers. |
| `$/Ama.Enterprise.P2p/Services/Core/DirectMessageSender.cs` | Implements localized targeted point-to-point generic delivery dynamically fetching active peering bindings avoiding overarching network broadcast storms. |
| `$/Ama.Enterprise.P2p/Services/Core/HonorLicenseManager.cs` | Implementation tracking generic honor-based checks evaluating provided bounds explicitly natively via RSA cryptographic signatures. |
| `$/Ama.Enterprise.P2p/Services/Core/IApplicationPayloadDispatcher.cs` | Dispatches targeted application payloads. |
| `$/Ama.Enterprise.P2p/Services/Core/IApplicationPayloadHandler.cs` | Defines a domain-level consumer decoupling underlying distribution protocols. |
| `$/Ama.Enterprise.P2p/Services/Core/IDirectMessageSender.cs` | Defines a targeted point-to-point payload delivery contract decoupling anti-entropy processes from gossip epidemic broadcasts explicitly honoring the Single Responsibility Principle. |
| `$/Ama.Enterprise.P2p/Services/Core/IFailureDetector.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IInboundMessageQueue.cs` | Defines an internal queue for decoupling inbound network listeners from the protocol logic. |
| `$/Ama.Enterprise.P2p/Services/Core/ILicenseManager.cs` | Contract isolating the validation logic for the honor-based licensing system. |
| `$/Ama.Enterprise.P2p/Services/Core/IP2pAlgorithm.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerAuthenticator.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerDiscovery.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerHandshaker.cs` | Added explicit `LocalHandshakePort` property natively exposing Phase 2 configurations natively decoupled without enforcing tightly bound internal options dependencies. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerRegistry.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerSelector.cs` | Interface for algorithms that select a generic subset of peers for communication. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerTopologyObserver.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransport.cs` | Generic interface defining the outbound network transport capabilities, augmented with endpoint routing capabilities. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransportListener.cs` | Generic interface defining the inbound network listener capabilities for receiving protocol messages. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransportRouter.cs` | Interface for routing outgoing messages to the appropriate transport based on the endpoint type. |
| `$/Ama.Enterprise.P2p/Services/Core/InMemoryPeerRegistry.cs` | Implements an in-memory thread-safe registry tracking peering topology globally using a flat dictionary mapping. |
| `$/Ama.Enterprise.P2p/Services/Core/InboundMessageQueue.cs` | Channel-backed implementation of the inbound message queue. |
| `$/Ama.Enterprise.P2p/Services/Core/LicenseStartupService.cs` | Background startup service tracking the single explicit license validation step cleanly without overlapping boundaries. |
| `$/Ama.Enterprise.P2p/Services/Core/PassThroughPeerAuthenticator.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/RandomPeerSelector.cs` | Implementation of IPeerSelector utilizing random distribution selection. |
| `$/Ama.Enterprise.P2p/Services/Core/TimeBasedFailureDetector.cs` | Implementation of IFailureDetector using abstract heartbeats decoupled from specific protocol options. |
| `$/Ama.Enterprise.P2p/Services/Core/TransportRouter.cs` | Composite transport router that delegates sending messages to the correct specific transport implementation. |
| `$/Ama.Enterprise.P2p/Services/Discovery/DnsPeerDiscovery.cs` | Extended DNS peer discovery to dynamically handle optional SRV record resolutions bridging isolated port configurations. |
| `$/Ama.Enterprise.P2p/Services/Discovery/IDnsSrvResolver.cs` | Interface defining the contract for resolving DNS SRV records, allowing abstraction over third-party DNS packages natively. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpDiscoveryJsonContext.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpPeerDiscovery.cs` | Implementation of UDP peer discovery. Updated to actively handshake and register incoming discovery peers when getting discovered, avoiding one-sided peer topologies. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpPeerHandshaker.cs` | Added `LocalHandshakePort` property natively extracting localized internal constraints standardizing interface. |
| `$/Ama.Enterprise.P2p/Services/P2pHostedService.cs` | Refactored encompassing overarching multi-mesh core states evaluating dynamic incoming deduplications and isolated health evaluations efficiently avoiding duplication. |
| `$/Ama.Enterprise.P2p/Services/Transports/TcpTransport.cs` | Implements isolated outbound transport using robust TCP streams explicitly mapped against bounded architectures. |
| `$/Ama.Enterprise.P2p/Services/Transports/TcpTransportListener.cs` | Implements inbound network listener extracting localized pure TCP streams mapping native robust pipelines. |
| `$/Ama.Enterprise.P2p/Services/Transports/UdpTransport.cs` | Implements completely decoupled, natively mapped lightweight UDP datagram delivery mechanisms efficiently safely. |
| `$/Ama.Enterprise.P2p/Services/Transports/UdpTransportListener.cs` | Implements decoupled native UDP inbound multiplexing tracking locally registered decentralized architectures smoothly. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/Ama.Enterprise.Project.Analyzers.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/DirectSerializationUsageAnalyzerTests.cs` | Unit tests for `DirectSerializationUsageAnalyzer` to ensure diagnostics are reported for `System.Text.Json` usages and ignored for correct generic interfaces. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/PropertyInfoUsageAnalyzerTests.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/SystemConvertUsageAnalyzerTests.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/Ama.Enterprise.Project.Analyzers.csproj` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/DirectSerializationUsageAnalyzer.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/PropertyInfoUsageAnalyzer.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/SystemConvertUsageAnalyzer.cs` | No description provided. |
| `$/Ama.Enterprise.UnitTests/Ama.Enterprise.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.UnitTests/Attributes/IntegrationFactAttribute.cs` | Custom xUnit `FactAttribute` providing a centralized toggle to enable or disable all integration tests. Shared testing utility. |
| `$/Ama.Enterprise.UnitTests/Attributes/TestedProtocolVersionAttribute.cs` | Custom attribute utilized by structural reflection tests to declare protocol versions explicitly covered by a method. Shared testing utility. |
| `$/Ama.Enterprise.UnitTests/Extensions/XunitLoggingBuilderExtensions.cs` | Extension methods to register xUnit logger in ILoggingBuilder. Shared testing utility. |
| `$/Ama.Enterprise.UnitTests/Logging/XunitLogger.cs` | Custom ILogger implementation for routing logs to xUnit's ITestOutputHelper. Shared testing utility. |
| `$/Ama.Enterprise.UnitTests/Logging/XunitLoggerProvider.cs` | Provider for creating XunitLogger instances. Shared testing utility. |
| `$/Ama.Enterprise.UnitTests/Networking/NetworkResourceManager.cs` | A shared resource manager providing unique wait-free network TCP and UDP ports reliably across explicitly isolated executing integration tests natively avoiding parallel port exhaustion collisions. |
| `$/Ama.Enterprise.slnx` | Purged unreferenced obsolete entries bridging merged internal bounds (`Ama.Enterprise.P2p.Http.Core` and `Ama.Enterprise.P2p.Kestrel`). |
| `$/CodingStandards.md` | No description provided. |
| `$/FilesDescription.md` | No description provided. |
| `$/LICENCE` | No description provided. |
| `$/apps-todo.txt` | No description provided. |
| `$/p2p-mesh-architectures.md` | No description provided. |
| `$/solution.settings.json` | No description provided. |
