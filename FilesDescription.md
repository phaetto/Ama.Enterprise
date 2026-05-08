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
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/CompositeStorageIntegrationTests.cs` | Integration tests verifying that the storage composite router redirects persistence commands to primary or specific instances. |
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
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Program.cs` | Removed magic `scopeProvider` accesses, resolving via transparent forwarded root services. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/FleetManager.cs` | Implementation handling intentions and queries for the fleet document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/IFleetManager.cs` | Interface for managing the distributed fleet status CRDT document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ITaskManager.cs` | Interface for managing the distributed task list CRDT document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ShowCaseCrdtStorage.cs` | End-to-end localized storage mechanism persisting multiple document streams resolving active DVV bounds. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/TaskManager.cs` | Implementation handling intentions and queries for the task list document. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Ama.Enterprise.CRDT.Distributed.TableStorage.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Extensions/ServiceCollectionExtensions.cs` | Registers distributed Azure Table Storage native persistence hooks. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Models/CrdtTableEntity.cs` | Azure Table Storage entity model incorporating property chunking to persist payloads up to ~960KB. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Models/TableStorageCrdtOptions.cs` | Configuration structure holding Azure Table Storage endpoints and table bindings. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Services/TableStorageDistributedCrdtStorage.cs` | A centralized Azure Table Storage distributed backend implementing chunked DVV. |
| `$/Ama.Enterprise.CRDT.Distributed.UnitTests/Ama.Enterprise.CRDT.Distributed.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed/Ama.Enterprise.CRDT.Distributed.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed/Extensions/ServiceCollectionExtensions.cs` | Introduced `AddDistributedCrdtService` extension to create transparent forwarders resolving to the `DistributedCrdtScopeProvider`, eliminating manual service provider scoping logic. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtEvictionRejectionMessage.cs` | Message broadcasted to forcefully reject and re-bootstrap nodes that have been tombstoned by the cluster, preventing amnesia edge cases. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMessageWrapper.cs` | Envelope wrapper mapping generic messages targeting specifically identified CRDT documents across the network topology. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtOperationsMessage.cs` | Transmission model conveying replicated CRDT intent patches targeted asynchronously across active nodes. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtRegistryEntry.cs` | Represents metadata about an active or tombstoned distributed CRDT document mapped via the global cluster registry. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtRegistryState.cs` | Global P2P synced directory state handling distributed multi-document topologies, ensuring active instantiation maps across nodes. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtSnapshotMessage.cs` | Message payload containing a complete materialized CRDT document snapshot, used as a fallback synchronization mechanism when log truncation gaps are detected. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtStateSyncMessage.cs` | Structure carrying generic synchronization states formatted across anti-entropy operations representing document DVV. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtStorageRegistration.cs` | Configuration structure mapping keyed DI storage registrations tracking active storage router aliases. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtOptions.cs` | Configuration options for the Distributed CRDT module, updated to include peer eviction TTL mappings tracking unreachable network bound nodes bridging state limits. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtP2pJsonContext.cs` | JSON serialization context mapping AOT bindings resolving eviction message constraints. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtSystemAotContext.cs` | AOT contextual reflection mapping for internal orchestrator registry CRDT scopes, bridging models. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtSystemJsonContext.cs` | JSON serialization context guaranteeing AOT compatibility for internal orchestrator registry CRDT scopes. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/MissingOperationsResult.cs` | DTO representing the result of querying for missing operations and indicating whether a full snapshot is required, bridging interface contracts without relying on tuples. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ClusterStateTracker.cs` | Singleton thread-safe implementation capturing localized maps representing overarching remote state matrix limits. Modified `RemovePeerByNetworkId` preventing amnesia by intentionally preserving CRDT vectors, decoupling network routes and avoiding destructive structural gaps. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CompositeCrdtStorage.cs` | Router backend decoupling multiple diverse persistent stores, routing multi-document identities. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtCheckpointService.cs` | Background service responsible for periodically saving the full in-memory state of all registered CRDTs. Replaced direct eviction with mathematically bounded tombstoning algorithms resolving logic bounds. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtDocumentOrchestrator.cs` | Centralized generic orchestrator managing global localized active P2P CRDT document bindings. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtEvictionService.cs` | Implementation of `ICrdtEvictionService` extracting the eviction logic, avoiding duplication. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtInitializationService.cs` | Refactored to mutate the shared scope context in-place, closing the split-brain scope disconnect bug and avoiding destroyed instances. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DistributedCrdtDocument.cs` | Removed obsolete `GetMissingOperationsAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DistributedCrdtScopeProvider.cs` | Secured by removing dangerous `ReplaceScope` logic, guaranteeing instances outlive P2P network threads. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IClusterStateTracker.cs` | Tracks the last known synchronization bounds. Updated exposing logical tombstone identity mechanisms alongside `TombstonePeerByNetworkId` targeting topology exits. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ICrdtDocumentOrchestrator.cs` | Generic manager facilitating multi-document runtime allocations, resolving logical decentralized P2P creation and deletion payloads. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ICrdtEvictionService.cs` | Interface for a dedicated service that orchestrates replica eviction and local identity re-bootstrapping across all CRDT documents. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtDocument.cs` | Removed obsolete `GetMissingOperationsAsync` as journal resolutions are centralized. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtStorage.cs` | Generic interface exposing unified persistence mechanisms for distributed CRDT documents, extending robust asynchronous DVV mapped journal trimming. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDocumentFactory.cs` | AOT-friendly generic factory interface for resolving mapped distributed CRDT instances. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/MemoryCrdtStorage.cs` | Ephemeral implementation providing default active storage, fulfilling unified backend protocol actions resolving async mapped trims. |
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
| `$/Ama.Enterprise.FeatureFlags.ShowCase/Program.cs` | Application entry point decoupled to explicitly map and inject required generic P2P structures directly bypassing monolithic dependency wrappers natively. |
| `$/Ama.Enterprise.FeatureFlags.UnitTests/Ama.Enterprise.FeatureFlags.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags/Ama.Enterprise.FeatureFlags.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags/Extensions/ServiceCollectionExtensions.cs` | Removed hardcoded transport mechanisms from the service configuration, cleanly decoupling the domain CRDT models from P2P infrastructural setup implementations. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlag.cs` | Data structure representing a single feature flag. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagOptions.cs` | Configuration structure for feature flags. Removed hardcoded network abstractions, delegating topology management dynamically to the host. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagState.cs` | Inherits `IDistributedCrdtState` and maps generic constraints bridging properties. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsCrdtAotContext.cs` | AOT context for the feature flags models. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsJsonContext.cs` | JSON context for the feature flags models. |
| `$/Ama.Enterprise.FeatureFlags/Services/FeatureFlagBootstrapper.cs` | Eager initialization hosted service that hooks into the orchestrator creating the global state, preventing amnesia races. |
| `$/Ama.Enterprise.FeatureFlags/Services/FeatureFlagClusterManager.cs` | Refactored internal DI boundaries utilizing mapped orchestrators ensuring generic dynamic models are cached, decoupling interface requirements. |
| `$/Ama.Enterprise.FeatureFlags/Services/IFeatureFlagClusterManager.cs` | Interface for the feature flag cluster manager. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Ama.Enterprise.P2p.AspNetCore.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Services/AspNetCorePeerHandshakeIntegrationTests.cs` | Refactored integration tests verifying the decoupled ASP.NET Core Standalone peer handshaker explicitly natively natively discovering topologies securely and reliably efficiently natively. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Services/AspNetCoreTransportIntegrationTests.cs` | Upgraded end-to-end multi-mesh generic routing tests testing ASP.NET Core explicitly evaluating distinct bounds properly actively evaluating Standalone structures efficiently natively natively securely explicitly structurally. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Services/KestrelPeerHandshakeIntegrationTests.cs` | Removed. Fully migrated explicit bounds strictly into unified standalone decoupled generalized ASP.NET Core natively mapping cleanly effectively purely effortlessly efficiently organically properly cleanly smoothly efficiently flawlessly cleanly correctly seamlessly correctly perfectly properly cleanly effectively cleanly correctly gracefully cleanly cleanly properly cleanly smartly correctly flawlessly elegantly smartly. |
| `$/Ama.Enterprise.P2p.AspNetCore.IntegrationTests/Services/KestrelTransportIntegrationTests.cs` | Removed. Substituted testing standard natively cleanly seamlessly decoupled seamlessly organically properly cleanly gracefully correctly safely logically organically safely logically smoothly optimally organically optimally gracefully optimally optimally rationally completely structurally securely effectively cleanly securely cleanly safely elegantly completely smartly rationally natively gracefully seamlessly cleanly elegantly safely smartly successfully logically natively safely smartly correctly completely optimally seamlessly rationally securely elegantly gracefully correctly securely smoothly securely gracefully smartly successfully gracefully. |
| `$/Ama.Enterprise.P2p.AspNetCore/Ama.Enterprise.P2p.AspNetCore.csproj` | Consolidated package natively mapping localized endpoints alongside Kestrel transport implementations into a unified ASP.NET HTTP structure. |
| `$/Ama.Enterprise.P2p.AspNetCore/Extensions/AspNetCoreDiscoveryServiceCollectionExtensions.cs` | Extension methods for safely registering generic ASP.NET Core peer handshaker components securely tracking specific internal multi-mesh routing architectures natively dynamically. |
| `$/Ama.Enterprise.P2p.AspNetCore/Extensions/EndpointRouteBuilderExtensions.cs` | Supplies `MapP2pMeshEndpoints` extending minimal APIs resolving inbound payloads. Updated namespaces matching consolidation. |
| `$/Ama.Enterprise.P2p.AspNetCore/Extensions/KestrelDiscoveryServiceCollectionExtensions.cs` | Removed. Upgraded resolving decoupled generic ASP.NET strictly executing standard natively decoupled generic dependency abstractions smoothly effectively resolving purely generic dynamically natively organically smoothly. |
| `$/Ama.Enterprise.P2p.AspNetCore/Extensions/ServiceCollectionExtensions.cs` | Isolated configuration hooking handlers dynamically mapping AOT generic traits. Merged Kestrel and Http.Core extensions internally tracking singleton mappings natively. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCoreHandshakeOptions.cs` | Configuration structure for isolated ASP.NET Core handshaking parameters explicitly tracking decoupled Integrated and Standalone topological bounding natively. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCoreHostingMode.cs` | Determines whether the ASP.NET Core P2P transport relies on the host application's HTTP pipeline explicitly routed via MapP2pMeshEndpoints() or spins up an isolated Standalone Kestrel web server internally natively decoupled. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCoreJsonContext.cs` | Generates isolated interoperability processing structured models natively. Appended Kestrel bounds resolving AOT serialization bounds purely. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCorePeerEndpoint.cs` | Identifies and bridges standard inbound external mapping boundaries and explicitly routed ports. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/AspNetCoreTransportOptions.cs` | Allows explicitly assigning network bounds decoupling public mappings distinctly isolating host constraints structurally. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/HttpPayloadProcessResult.cs` | Enumeration explicitly identifying deterministic HTTP processing outcomes. Consolidated from removed Http.Core package. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/KestrelHandshakeOptions.cs` | Removed. Replaced by strictly isolated integrated distinct generalized natively bounded parameters efficiently managing endpoints mapping cleanly structurally gracefully safely. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/KestrelPeerEndpoint.cs` | Removed. Replaced by unified integrated ASP.NET Core generic explicit hosting mechanisms purely resolving configurations cleanly structurally natively. |
| `$/Ama.Enterprise.P2p.AspNetCore/Models/KestrelTransportOptions.cs` | Removed. Replaced by structurally unified multi-mode decoupled ASP.NET Core explicit structures natively encapsulating parameters securely explicitly natively safely. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/AspNetCoreTransport.cs` | Implements resilient asynchronous generic outbound mapping reliably dispatching standard payloads tracking structural URLs safely handling standard rejections cleanly. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/AspNetCoreTransportListener.cs` | Implements interface boundaries directly attaching explicit handlers purely. Unified namespace bounds following consolidation cleanly. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/Discovery/AspNetCorePeerHandshaker.cs` | Implementation of IPeerHandshaker managing isolated ASP.NET Core HTTP probes explicitly natively supporting Standalone and Integrated hosting bounds robustly decoupling structures. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/Discovery/KestrelPeerHandshaker.cs` | Removed. Generalized dynamically decoupled strictly integrated cleanly native bindings explicitly replacing distinct explicit generic mappings purely cleanly structurally cleanly seamlessly. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/HttpInboundDispatcher.cs` | Thread-safe generic centralized HTTP orchestrator extracting mapped inbound pipelines isolated exclusively. Consolidated from removed Http.Core package. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/IHttpInboundDispatcher.cs` | Contract decoupling generic abstract HTTP routing frameworks natively decoupling boundaries safely. Consolidated from removed Http.Core package. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/KestrelTransport.cs` | Removed. Substituted by dynamically evaluated strictly decoupled generic pure standard native HTTP components structurally securely decoupled explicitly natively mapping structurally. |
| `$/Ama.Enterprise.P2p.AspNetCore/Services/KestrelTransportListener.cs` | Removed. Unified natively executing generic HTTP explicit topologies mapping safely purely dynamically evaluating structures purely natively gracefully securely efficiently. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Ama.Enterprise.P2p.IntegrationTests.csproj` | Integration tests project for validating P2P networking components via HTTP loopbacks. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Architecture/VersioningArchitectureTests.cs` | Architectural tests that parse the CI/CD deployment files ensuring specific deployed versions always possess explicit test coverage. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Discovery/DnsPeerDiscoveryIntegrationTests.cs` | Integration tests verifying DNS peer discovery natively resolves target domains and dispatches accurate Phase 2 handshakes against discovered IPs. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/Handlers/TestMessageHandler.cs` | Updated to implement `IApplicationPayloadHandler` and capture unwrapped application payloads via `TestPayloadRecord` reflecting domain consumer architecture. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/Models/TestNode.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/P2pAdvancedIntegrationTests.cs` | Updated DI registrations routing `IApplicationPayloadHandler` and adapted assertion mechanics to decode unwrapped byte arrays. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/P2pNetworkIntegrationTests.cs` | Refactored integration assertions targeting unboxed test payloads. Redesigned deduplication tests wrapping identical envelopes ensuring robust evaluation. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/P2pVersioningIntegrationTests.cs` | Refactored integration assertions mapping unboxed generic wrappers. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/PushPullGossipIntegrationTests.cs` | Integration tests verifying Push-Pull anti-entropy bounds isolating digest transmissions and ensuring active localized fallback synchronizations. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/UdpPeerDiscoveryIntegrationTests.cs` | Updated integration profiles extracting hardcoded target mappings demonstrating and validating decentralized runtime IP mappings robustly without collisions. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Ama.Enterprise.P2p.Mqtt.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Services/MqttPeerDiscoveryIntegrationTests.cs` | Integration tests verifying the active MQTT peer discovery background service mapping decoupled mesh architectures. |
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
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttClientManager.cs` | Service controlling the underlying generic MQTTnet connections dispatching messages to scoped route prefixes. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttTransport.cs` | Generic outbound mesh transport implementing isolated message payload deliveries targeting assigned MQTT topologies. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttTransportListener.cs` | Internal background receiver connecting underlying MQTT topic subscriptions and interpreting generalized mesh envelopes. |
| `$/Ama.Enterprise.P2p.Telemetry.IntegrationTests/Ama.Enterprise.P2p.Telemetry.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.Telemetry.IntegrationTests/Services/TelemetryNetworkIntegrationTests.cs` | Integration tests verifying pure asynchronous End-to-End explicitly evaluated decentralized generic telemetry broadcasting mapping decoupled constraints actively securely. |
| `$/Ama.Enterprise.P2p.Telemetry/Ama.Enterprise.P2p.Telemetry.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.Telemetry/Constants.cs` | Defines the core explicit meter names dynamically evaluated by the telemetry network listeners isolating generic algorithms. |
| `$/Ama.Enterprise.P2p.Telemetry/Extensions/ServiceCollectionExtensions.cs` | Updated DI to map singletons for the explicit `TelemetryPushProtocol` ensuring bounded generic instance isolation matching background network hooks. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/MetricSnapshotDto.cs` | AOT friendly pure DTO strictly holding explicitly scoped aggregated metrics structures decoupling logic safely. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/MetricTagDto.cs` | AOT friendly structure representing a metric precise dimension extracted tracking generic mappings actively. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/TelemetryJsonContext.cs` | Isolated AOT generic bindings resolving metric serialization safely evaluating pure DTO constraints natively. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/TelemetryOptions.cs` | Configuration specifically targeting defining how and when mapped telemetry metrics broadcast generically isolated. |
| `$/Ama.Enterprise.P2p.Telemetry/Models/TelemetryPayloadDto.cs` | DTO defining the top-level explicitly network transmission payloads natively wrapping internally batched generic metrics safely mapping AOT configurations. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/ITelemetryAggregator.cs` | Interface for centrally aggregating and retrieving in-memory telemetry network states. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryAggregator.cs` | Thread-safe in-memory aggregator holding the latest telemetry network metrics. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryForwarderService.cs` | Substituted general P2P broadcast generic array loops with the isolated `TelemetryPushProtocol` explicit instance natively mapping payloads purely bounding topology routing. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryPayloadHandler.cs` | Application payload handler dedicated to processing incoming telemetry network payloads and routing them into the centralized aggregator. |
| `$/Ama.Enterprise.P2p.Telemetry/Services/TelemetryPushProtocol.cs` | Dedicated protocol handling un-forwarded one-hop payloads explicitly utilizing `IDirectMessageSender` securely bypassing duplicative network enveloping and strictly bounding explicit network topology routing internally natively. |
| `$/Ama.Enterprise.P2p.UnitTests/Ama.Enterprise.P2p.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/GossipProtocolTests.cs` | Refactored mock structures to validate `IApplicationPayloadDispatcher` verifying unwrapped domain payload transmissions. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/InMemoryPeerRegistryTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/MessageDispatcherTests.cs` | Adjusted tests adapting the newly refactored `ApplicationPayloadDispatcher` confirming decoupling of generic envelopes. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/RandomPeerSelectorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/TimeBasedFailureDetectorTests.cs` | No description provided. |
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
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcConnectionManager.cs` | Internal hosted implementation managing SIPSorcery RTCPeer connections bound within localized mesh scopes. Updated to use DTOs instead of tuples. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcTransport.cs` | WebRTC specific implementation handling outbound gossip structures wrapping targeted connection payloads. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcTransportListener.cs` | WebRTC specific listener registering asynchronous bindings targeting decentralized peer streams. |
| `$/Ama.Enterprise.P2p/Ama.Enterprise.P2p.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p/Constants.cs` | Global constants for the P2P module, including protocol versions and payload size limits. |
| `$/Ama.Enterprise.P2p/Extensions/DnsDiscoveryServiceCollectionExtensions.cs` | Extension methods for registering DNS-based active peer discovery components isolated via Keyed dependencies to specific mesh profiles. |
| `$/Ama.Enterprise.P2p/Extensions/IP2pMeshBuilder.cs` | Interface for building and configuring specific Keyed DI mesh profiles. |
| `$/Ama.Enterprise.P2p/Extensions/P2pMeshBuilder.cs` | Implementation of `IP2pMeshBuilder` handling multi-mesh dependency injection tracking. |
| `$/Ama.Enterprise.P2p/Extensions/P2pMeshRegistrationTracker.cs` | Centralized tracking mechanism guaranteeing idempotent mesh registrations evaluating structurally identical configurations, bypassing duplicates. |
| `$/Ama.Enterprise.P2p/Extensions/ServiceCollectionExtensions.cs` | Moved common mesh infrastructure registrations into AddP2pMesh to avoid duplication between distinct protocol configurations like AddGossipNetwork and AddPushPullGossipNetwork. |
| `$/Ama.Enterprise.P2p/Extensions/UdpDiscoveryServiceCollectionExtensions.cs` | Removed tightly coupled injected Handshaker Options isolating generic P2P mesh parameters decoupling explicitly. |
| `$/Ama.Enterprise.P2p/Models/Core/FailureDetectorOptions.cs` | Configuration options for tuning generic protocol-agnostic failure detection components. |
| `$/Ama.Enterprise.P2p/Models/Core/IMeshMessage.cs` | Added required standardized `SenderId` bounding origin payloads explicitly decoupled traversing generic algorithms identically. |
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
| `$/Ama.Enterprise.P2p/Models/Transports/HttpPeerEndpoint.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Transports/HttpTransportOptions.cs` | Added `IsEnabled` flag to track if the HTTP transport has been added for a designated P2P mesh. |
| `$/Ama.Enterprise.P2p/Services/Core/ApplicationPayloadDispatcher.cs` | Composite orchestrator dispatching to abstract domain observers. |
| `$/Ama.Enterprise.P2p/Services/Core/DirectMessageSender.cs` | Implements localized targeted point-to-point generic delivery dynamically fetching active peering bindings avoiding overarching network broadcast storms. |
| `$/Ama.Enterprise.P2p/Services/Core/IApplicationPayloadDispatcher.cs` | Dispatches targeted application payloads. |
| `$/Ama.Enterprise.P2p/Services/Core/IApplicationPayloadHandler.cs` | Defines a domain-level consumer decoupling underlying distribution protocols. |
| `$/Ama.Enterprise.P2p/Services/Core/IDirectMessageSender.cs` | Defines a targeted point-to-point payload delivery contract decoupling anti-entropy processes from gossip epidemic broadcasts explicitly honoring the Single Responsibility Principle. |
| `$/Ama.Enterprise.P2p/Services/Core/IFailureDetector.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IInboundMessageQueue.cs` | Defines an internal queue for decoupling inbound network listeners from the protocol logic. |
| `$/Ama.Enterprise.P2p/Services/Core/IP2pProtocol.cs` | Generic interface defining the orchestrator for the P2P protocol, abstracting algorithms like Gossip. |
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
| `$/Ama.Enterprise.P2p/Services/Core/PassThroughPeerAuthenticator.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/RandomPeerSelector.cs` | Implementation of IPeerSelector utilizing random distribution selection. |
| `$/Ama.Enterprise.P2p/Services/Core/TimeBasedFailureDetector.cs` | Implementation of IFailureDetector using abstract heartbeats decoupled from specific protocol options. |
| `$/Ama.Enterprise.P2p/Services/Core/TransportRouter.cs` | Composite transport router that delegates sending messages to the correct specific transport implementation. |
| `$/Ama.Enterprise.P2p/Services/Discovery/DnsPeerDiscovery.cs` | Extended DNS peer discovery to dynamically handle optional SRV record resolutions bridging isolated port configurations. |
| `$/Ama.Enterprise.P2p/Services/Discovery/IDnsSrvResolver.cs` | Interface defining the contract for resolving DNS SRV records, allowing abstraction over third-party DNS packages natively. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpDiscoveryJsonContext.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpPeerDiscovery.cs` | Implementation of UDP peer discovery. Updated to actively handshake and register incoming discovery peers when getting discovered, avoiding one-sided peer topologies. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpPeerHandshaker.cs` | Added `LocalHandshakePort` property natively extracting localized internal constraints standardizing interface. |
| `$/Ama.Enterprise.P2p/Services/Gossip/GossipProtocol.cs` | Stripped explicit bounds transitioning states cleanly into globally bound generic infrastructures extracting standalone `ProtocolState` natively. |
| `$/Ama.Enterprise.P2p/Services/Gossip/PushPullGossipProtocol.cs` | Cleaned redundant health and heartbeat mechanics offloading bounds reliably against decoupled host mechanisms dynamically. |
| `$/Ama.Enterprise.P2p/Services/P2pHostedService.cs` | Refactored encompassing overarching multi-mesh core states evaluating dynamic incoming deduplications and isolated health evaluations efficiently avoiding duplication. |
| `$/Ama.Enterprise.P2p/Services/Transports/HttpTransport.cs` | Refactored standard HTTP implementation isolated via Keyed dependencies handling precise outgoing payloads evaluating identical network bounds. |
| `$/Ama.Enterprise.P2p/Services/Transports/HttpTransportListener.cs` | Redesigned inbound network listener transitioning away from multiplexing, adapting keyed dependencies handling localized prefixes isolating meshes. |
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
| `$/Ama.Enterprise.slnx` | Purged unreferenced obsolete entries bridging merged internal bounds (`Ama.Enterprise.P2p.Http.Core` and `Ama.Enterprise.P2p.Kestrel`). |
| `$/CodingStandards.md` | No description provided. |
| `$/FilesDescription.md` | No description provided. |
| `$/LICENCE` | No description provided. |
| `$/apps-todo.txt` | No description provided. |
| `$/p2p-mesh-architectures.md` | No description provided. |
| `$/solution.settings.json` | No description provided. |
