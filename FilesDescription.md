| File Path | Description |
| --- | --- |
| `$/.editorconfig` | No description provided. |
| `$/.github/workflows/ci.yml` | GitHub Actions workflow for building and testing the solution on PRs and non-master branch pushes. |
| `$/.github/workflows/publish-nuget-manual.yml` | GitHub Actions workflow for manually publishing stable releases to NuGet with explicit version inputs. |
| `$/.github/workflows/publish-nuget.yml` | GitHub Actions workflow for automatically publishing preview packages to NuGet upon pushing to the master branch. |
| `$/.gitignore` | No description provided. |
| `$/Ama.Enterprise.CRDT.Analyzers/Ama.Enterprise.CRDT.Analyzers.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/Ama.Enterprise.CRDT.BlazorApp.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/App.razor` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/Layout/MainLayout.razor` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/Layout/MainLayout.razor.css` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/Layout/NavMenu.razor` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/Layout/NavMenu.razor.css` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/Pages/Counter.razor` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/Pages/Home.razor` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/Pages/NotFound.razor` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/Pages/Weather.razor` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/Program.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/_Imports.razor` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/wwwroot/css/app.css` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/wwwroot/favicon.png` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/wwwroot/icon-192.png` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/wwwroot/icon-512.png` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/wwwroot/index.html` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/wwwroot/manifest.webmanifest` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/wwwroot/sample-data/weather.json` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/wwwroot/service-worker.js` | No description provided. |
| `$/Ama.Enterprise.CRDT.BlazorApp/wwwroot/service-worker.published.js` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/Ama.Enterprise.CRDT.Distributed.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Ama.Enterprise.CRDT.Distributed.ShowCase.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/DeviceStatus.cs` | Data structure representing the status of an IoT device. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/FleetState.cs` | Root CRDT document model representing fleet devices status, updated to inherit `IDistributedCrdtState`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/ShowCaseCrdtAotContext.cs` | CRDT AOT reflection context mapping types used by the showcase documents. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/ShowCaseJsonContext.cs` | AOT JSON context for the showcase multi-CRDT models. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/TaskItem.cs` | Data structure representing an individual task item. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/TaskListState.cs` | Root CRDT document model representing a task list, updated to inherit `IDistributedCrdtState`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Program.cs` | Refactored internal DI boundaries correctly utilizing explicitly mapped Document IDs safely decoupled from mapping logic. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/FleetManager.cs` | Implementation handling intentions and queries for the fleet document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/IFleetManager.cs` | Interface for managing the distributed fleet status CRDT document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ITaskManager.cs` | Interface for managing the distributed task list CRDT document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ShowCaseCrdtStorage.cs` | End-to-end localized storage mechanism inherently persisting multiple document streams directly resolving active DVV bounds completely. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ShowCaseDocumentStorage.cs` | Local JSON file-based document storage implementation capturing root application state uniquely via DI. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ShowCaseGlobalStorage.cs` | Local JSON file-based storage implementation correctly persisting the Global DVV mapped dynamically for the node locally. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/TaskManager.cs` | Implementation handling intentions and queries for the task list document. |
| `$/Ama.Enterprise.CRDT.Distributed.UnitTests/Ama.Enterprise.CRDT.Distributed.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed/Ama.Enterprise.CRDT.Distributed.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed/Extensions/ServiceCollectionExtensions.cs` | Updated `AddDistributedDocument` DI registration to securely enforce `IDistributedCrdtState` boundaries implicitly mapped through parameters strictly. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMessageWrapper.cs` | Envelope wrapper mapping generic messages targeting specifically identified CRDT documents across the network topology. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtOperationsMessage.cs` | Transmission model conveying replicated CRDT intent patches targeted asynchronously natively across active nodes. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtSnapshotMessage.cs` | Message payload containing a complete materialized CRDT document snapshot, used as a fallback synchronization mechanism when log truncation gaps are detected. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtSnapshotRequestMessage.cs` | Request message broadcasted by a new replica to obtain a full document snapshot from active peers. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtSnapshotResponseMessage.cs` | Response message containing a full serialized CRDT document snapshot to bootstrap an empty replica. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtStateSyncMessage.cs` | Structure carrying generic synchronization states explicitly formatted across anti-entropy operations representing document DVV. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtOptions.cs` | Standardized internal options configurations outlining shared active sync behavior maps inherently integrated into any document type. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtP2pJsonContext.cs` | Global baseline AOT-centric System.Text.Json generation models resolving native P2P exchanges without resorting to reflection explicitly. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/IDistributedCrdtState.cs` | Imposes a centralized generic constraint on root CRDT state models to inherently map their own synchronization identifiers. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ClusterStateTracker.cs` | Thread-safe implementation storing exactly mapped global synchronization states for active P2P connections. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtInitializationService.cs` | Hosted service responsible for aggressively initializing all registered distributed CRDT documents upon application startup before anti-entropy algorithms activate. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DistributedCrdtDocument.cs` | Updated to extract the DocumentId securely from the explicitly injected generic IDistributedCrdtState constraint correctly without constructors. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DistributedCrdtScopeProvider.cs` | Scoped singleton holding global reference handles bridging inbound P2P traffic strictly alongside active internal event listeners gracefully. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IClusterStateTracker.cs` | Tracks the last known synchronization bounds (Global Dotted Version Vectors) for all connected peers to enable mathematically safe journal trimming. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtDocument.cs` | Updated with generic constraints requiring TState to implement IDistributedCrdtState to remove magic strings. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtGlobalStorage.cs` | Generic interface establishing bounds for optional persistence stores strictly hooking onto the global Replica Version Vector lifecycle. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtStorage.cs` | Generic interface exposing unified persistence mechanisms for distributed CRDT documents securely extending robust asynchronous DVV mapped journal trimming. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/MemoryCrdtStorage.cs` | Ephemeral implementation effectively providing default active storage correctly fulfilling unified backend protocol actions directly resolving async mapped trims natively. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/MemoryJournal.cs` | Thread-safe, abstract operation buffer logging uncommitted or recently committed CRDT patches for immediate anti-entropy retrieval. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtAntiEntropyService.cs` | Hosted background orchestrator cyclically synchronizing each registered document pipeline actively throughout connected meshes implicitly. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtGossipHandler.cs` | Intercepts universally generated P2P traffic and translates payload envelopes to corresponding localized document routing handlers. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtJournalTrimmingService.cs` | Background service that aggregates network-wide synchronization bounds and safely executes distributed log truncations using the Global Minimum Version Vector. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtTopologyObserver.cs` | Observes internal transport listeners and triggers aggressive DVV baseline synchronization maps dynamically against freshly bound peers securely. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/StorageJournalForwarder.cs` | Injects localized implementations mapping directly back into underlying internal storage architectures acting strictly as the interface wrapper over globally active pipelines. |
| `$/Ama.Enterprise.CRDT.MessagePack/Ama.Enterprise.CRDT.MessagePack.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.OpenTelemetry/Ama.Enterprise.CRDT.OpenTelemetry.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.TableStorage/Ama.Enterprise.CRDT.TableStorage.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.TableStorage/todo.txt` | No description provided. |
| `$/Ama.Enterprise.CRDT.Testing/Ama.Enterprise.CRDT.Testing.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.UI/Ama.Enterprise.CRDT.UI.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT/Ama.Enterprise.CRDT.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags.IntegrationTests/Ama.Enterprise.FeatureFlags.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags.ShowCase/Ama.Enterprise.FeatureFlags.ShowCase.csproj` | Showcase console application project displaying P2P feature flags integration, AOT readiness, and UDP cluster discovery. |
| `$/Ama.Enterprise.FeatureFlags.ShowCase/Program.cs` | Main entry point containing an interactive UI for observing and interacting with distributed feature flags via gossip synchronization. |
| `$/Ama.Enterprise.FeatureFlags.UnitTests/Ama.Enterprise.FeatureFlags.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags/Ama.Enterprise.FeatureFlags.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags/Extensions/ServiceCollectionExtensions.cs` | Refactored parameterless DI registration securely matching updated interface bindings natively. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlag.cs` | Data structure representing a single feature flag. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagOptions.cs` | Configuration options for the feature flags module, including the CRDT ReplicaId mapping. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagState.cs` | Explicitly inherits `IDistributedCrdtState` and maps generic constraints bridging properties dynamically. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsCrdtAotContext.cs` | AOT context for the feature flags models. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsJsonContext.cs` | JSON context for the feature flags models. |
| `$/Ama.Enterprise.FeatureFlags/Services/FeatureFlagClusterManager.cs` | Implementation of the feature flag cluster manager using DVV sync. |
| `$/Ama.Enterprise.FeatureFlags/Services/IFeatureFlagClusterManager.cs` | Interface for the feature flag cluster manager. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Ama.Enterprise.P2p.IntegrationTests.csproj` | Integration tests project for validating P2P networking components via HTTP loopbacks. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Architecture/VersioningArchitectureTests.cs` | Architectural tests that parse the CI/CD deployment files ensuring specific deployed versions always possess explicit test coverage. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/Handlers/TestMessageHandler.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/Models/TestNode.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/P2pAdvancedIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/P2pNetworkIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/P2pVersioningIntegrationTests.cs` | Integration tests verifying backwards compatibility and explicit deployment protocol versioning constraints. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/UdpPeerDiscoveryIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.TableStorage/Ama.Enterprise.P2p.TableStorage.csproj` | Serverless-focused Azure Table Storage integration for P2P state management. |
| `$/Ama.Enterprise.P2p.TableStorage/Extensions/ServiceCollectionExtensions.cs` | DI extension methods for registering the Table Storage peer registry. |
| `$/Ama.Enterprise.P2p.TableStorage/Models/TableStorageRegistryOptions.cs` | Configuration options for the Table Storage peer registry. |
| `$/Ama.Enterprise.P2p.TableStorage/Services/TableStoragePeerRegistry.cs` | Implementation of `IPeerRegistry` utilizing Azure Table Storage, optimized for ephemeral/serverless compute nodes. |
| `$/Ama.Enterprise.P2p.UnitTests/Ama.Enterprise.P2p.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Extensions/ServiceCollectionExtensionsTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/GossipProtocolTests.cs` | Unit tests for GossipProtocol, updated to verify decoupled message queue and routing behaviors. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/HttpTransportListenerTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/HttpTransportTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/InMemoryPeerRegistryTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/MessageDispatcherTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/P2pHostedServiceTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/PassThroughPeerAuthenticatorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/RandomPeerSelectorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/SystemTextJsonGossipSerializerTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/TimeBasedFailureDetectorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.WebRTC.IntegrationTests/Ama.Enterprise.P2p.WebRTC.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.WebRTC.IntegrationTests/Extensions/ServiceCollectionExtensionsTests.cs` | Structural tests verifying proper dependency injection registration across the P2P mesh logic utilizing WebRTC specifically. |
| `$/Ama.Enterprise.P2p.WebRTC.IntegrationTests/Services/WebRtcTransportIntegrationTests.cs` | End-to-end integration tests verifying functional WebRTC SDP handshake synchronization and STUN connection mechanisms safely. Skipped by default. |
| `$/Ama.Enterprise.P2p.WebRTC.TableStorage/Ama.Enterprise.P2p.WebRTC.TableStorage.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.WebRTC/Ama.Enterprise.P2p.WebRTC.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.WebRTC/Extensions/ServiceCollectionExtensions.cs` | WebRTC dependency injection pipeline configuring underlying STUN models mapping transports securely alongside base generic Gossip meshes natively, heavily updated to inject dynamic cross-assembly JSON polymorphism resolvers. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcHandshakeMessage.cs` | In-band signaling structure notifying explicitly local identity topologies securely through initialized WebRTC channels safely. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcJsonContext.cs` | Baseline STJ context generation tracking dynamically WebRTC handshake protocols completely. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcOptions.cs` | Configuration structure holding explicit ICE servers natively bound to AOT-friendly serialization mappings. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcPeerEndpoint.cs` | WebRTC data channel connection endpoint identifying uniquely scoped connection states securely. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/IWebRtcConnectionManager.cs` | Interface isolating abstract signaling scopes natively exposing Data Channel inbound references. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/IWebRtcInvitationService.cs` | Generic mechanism exchanging SDP structures seamlessly representing localized peer invitations dynamically cross boundaries appropriately. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcConnectionManager.cs` | Internal hosted implementation managing explicitly SIPSorcery RTCPeer connections directly bound within localized mesh scopes correctly. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcTransport.cs` | WebRTC specific implementation handling outbound gossip structures safely wrapping targeted connection payloads. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcTransportListener.cs` | WebRTC specific listener registering seamlessly asynchronous bindings effectively targeting decentralized peer streams globally. |
| `$/Ama.Enterprise.P2p/Ama.Enterprise.P2p.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p/Constants.cs` | Global constants for the P2P module, including protocol versions and payload size limits. |
| `$/Ama.Enterprise.P2p/Extensions/IP2pMeshBuilder.cs` | Interface for building and configuring specific Keyed DI mesh profiles. |
| `$/Ama.Enterprise.P2p/Extensions/P2pMeshBuilder.cs` | Implementation of `IP2pMeshBuilder` handling multi-mesh dependency injection tracking. |
| `$/Ama.Enterprise.P2p/Extensions/ServiceCollectionExtensions.cs` | DI extension methods configuring P2P Gossip protocol components. Updated `HttpTransport` registration to inject `IPeerRegistry` dependency. |
| `$/Ama.Enterprise.P2p/Extensions/UdpDiscoveryServiceCollectionExtensions.cs` | Registration logic configuring Dependency Injection specifically targeting the UDP peer discovery sub-components and background services. |
| `$/Ama.Enterprise.P2p/Models/Core/FailureDetectorOptions.cs` | Configuration options for tuning generic protocol-agnostic failure detection components. |
| `$/Ama.Enterprise.P2p/Models/Core/P2pMeshMetadata.cs` | Metadata record registering a specific mesh identifier into the global dependency container for orchestration. |
| `$/Ama.Enterprise.P2p/Models/Core/P2pNodeOptions.cs` | Centralized generic configuration options holding the core node identity (ID and Endpoint) for the P2P Mesh. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerEndpoint.cs` | Abstract base record for peer endpoints, explicitly configured with JSON polymorphic attributes natively mapping same-assembly derivatives to support standard AOT serialization. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerId.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerNode.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerStatus.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Discovery/UdpDiscoveryOptions.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Gossip/GossipMessage.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Gossip/GossipOptions.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/P2pJsonSerializerContext.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Transports/HttpPeerEndpoint.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Transports/HttpTransportOptions.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IFailureDetector.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IInboundMessageQueue.cs` | Defines an internal queue for decoupling inbound network listeners from the protocol logic. |
| `$/Ama.Enterprise.P2p/Services/Core/IMessageDispatcher.cs` | Generic interface routing incoming protocol messages to registered handlers. |
| `$/Ama.Enterprise.P2p/Services/Core/IMessageHandler.cs` | Generic interface defining a domain-level consumer for P2P messages. |
| `$/Ama.Enterprise.P2p/Services/Core/IP2pProtocol.cs` | Generic interface defining the orchestrator for the P2P protocol, abstracting algorithms like Gossip. |
| `$/Ama.Enterprise.P2p/Services/Core/IP2pTelemetry.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerAuthenticator.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerDiscovery.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerRegistry.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerSelector.cs` | Interface for algorithms that select a generic subset of peers for communication. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerTopologyObserver.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransport.cs` | Generic interface defining the outbound network transport capabilities, augmented with endpoint routing capabilities. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransportListener.cs` | Generic interface defining the inbound network listener capabilities for receiving protocol messages. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransportRouter.cs` | Interface for routing outgoing messages to the appropriate transport based on the endpoint type. |
| `$/Ama.Enterprise.P2p/Services/Core/InMemoryPeerRegistry.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/InboundMessageQueue.cs` | Channel-backed implementation of the inbound message queue. |
| `$/Ama.Enterprise.P2p/Services/Core/MessageDispatcher.cs` | Implements the generic message dispatcher for routing parsed P2P messages. |
| `$/Ama.Enterprise.P2p/Services/Core/PassThroughPeerAuthenticator.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/RandomPeerSelector.cs` | Implementation of IPeerSelector utilizing random distribution selection. |
| `$/Ama.Enterprise.P2p/Services/Core/TimeBasedFailureDetector.cs` | Implementation of IFailureDetector using abstract heartbeats decoupled from specific protocol options. |
| `$/Ama.Enterprise.P2p/Services/Core/TransportRouter.cs` | Composite transport router that delegates sending messages to the correct specific transport implementation. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpDiscoveryJsonContext.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpPeerDiscovery.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Gossip/GossipProtocol.cs` | Gossip orchestrator decoupled from network listeners, communicating via inbound queues and outbound routers. |
| `$/Ama.Enterprise.P2p/Services/P2pHostedService.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Transports/HttpTransport.cs` | Outbound HTTP-based transport implementation that dynamically resolves targets. Updated to automatically remove dead peers from the registry upon persistent connection or communication timeouts. |
| `$/Ama.Enterprise.P2p/Services/Transports/HttpTransportListener.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/Ama.Enterprise.Project.Analyzers.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/PropertyInfoUsageAnalyzerTests.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/SystemConvertUsageAnalyzerTests.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/Ama.Enterprise.Project.Analyzers.csproj` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/PropertyInfoUsageAnalyzer.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/SystemConvertUsageAnalyzer.cs` | No description provided. |
| `$/Ama.Enterprise.UnitTests/Ama.Enterprise.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.UnitTests/Attributes/IntegrationFactAttribute.cs` | Custom xUnit `FactAttribute` providing a centralized toggle to enable or disable all integration tests. Shared testing utility. |
| `$/Ama.Enterprise.UnitTests/Attributes/TestedProtocolVersionAttribute.cs` | Custom attribute utilized by structural reflection tests to declare protocol versions explicitly covered by a method. Shared testing utility. |
| `$/Ama.Enterprise.UnitTests/Extensions/XunitLoggingBuilderExtensions.cs` | Extension methods to register xUnit logger in ILoggingBuilder. Shared testing utility. |
| `$/Ama.Enterprise.UnitTests/Logging/XunitLogger.cs` | Custom ILogger implementation for routing logs to xUnit's ITestOutputHelper. Shared testing utility. |
| `$/Ama.Enterprise.UnitTests/Logging/XunitLoggerProvider.cs` | Provider for creating XunitLogger instances. Shared testing utility. |
| `$/Ama.Enterprise.slnx` | No description provided. |
| `$/CodingStandards.md` | No description provided. |
| `$/FilesDescription.md` | No description provided. |
| `$/LICENCE` | No description provided. |
| `$/solution.settings.json` | No description provided. |
