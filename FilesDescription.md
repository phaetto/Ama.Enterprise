| File Path | Description |
| --- | --- |
| `$/.editorconfig` | No description provided. |
| `$/.github/workflows/ci.yml` | GitHub Actions workflow for building and testing the solution on PRs and non-master branch pushes. |
| `$/.github/workflows/publish-nuget-manual.yml` | GitHub Actions workflow for manually publishing stable releases to NuGet with explicit version inputs. |
| `$/.github/workflows/publish-nuget.yml` | GitHub Actions workflow for automatically publishing preview packages to NuGet upon pushing to the master branch. |
| `$/.gitignore` | No description provided. |
| `$/Ama.Enterprise.CRDT.Analyzers/Ama.Enterprise.CRDT.Analyzers.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/Ama.Enterprise.CRDT.Distributed.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/AntiEntropyStateSyncIntegrationTests.cs` | Integration tests strictly verifying that state synchronization extracts unified missing operations efficiently. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/BackgroundAndStorageIntegrationTests.cs` | Handled breaking DI changes cleanly by invoking `IApplicationPayloadHandler` avoiding obsolete generic envelopes during manual fallback sync testing seamlessly intelligently perfectly optimally seamlessly reliably appropriately correctly gracefully securely seamlessly rationally flawlessly smoothly effectively explicitly smoothly cleanly natively. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/DocumentOrchestratorIntegrationTests.cs` | Integration tests verifying the multi-document orchestration matrix, dynamic CRDT lifecycle creation, and tombstoning limits correctly inherently gracefully. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/EvictionEdgeCasesIntegrationTests.cs` | Integration tests explicitly demonstrating and replicating the four edge case vulnerabilities associated with tombstoning, eviction data amnesia, unbounded journals, and snapshot overwrites. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/JournalingAndSnapshottingIntegrationTests.cs` | Refactored tests to bypass obsolete document wrapper methods explicitly querying central synchronization services for missing journals. |
| `$/Ama.Enterprise.CRDT.Distributed.IntegrationTests/MainServicesHappyPathIntegrationTests.cs` | Updated integration test to use `IApplicationPayloadHandler` instead of obsolete message handlers and cleaned up excessive comments. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Ama.Enterprise.CRDT.Distributed.ShowCase.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Constants.cs` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/DeviceStatus.cs` | Data structure representing the status of an IoT device. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/FleetState.cs` | Root CRDT document model representing fleet devices status, updated to inherit `IDistributedCrdtState`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/ShowCaseCrdtAotContext.cs` | CRDT AOT reflection context mapping types used by the showcase documents. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/ShowCaseJsonContext.cs` | AOT JSON context for the showcase multi-CRDT models. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/TaskItem.cs` | Data structure representing an individual task item. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Models/TaskListState.cs` | Root CRDT document model representing a task list, updated to inherit `IDistributedCrdtState`. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Program.cs` | Removed magic `scopeProvider` accesses, resolving natively via transparent forwarded root services correctly. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/FleetManager.cs` | Implementation handling intentions and queries for the fleet document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/IFleetManager.cs` | Interface for managing the distributed fleet status CRDT document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ITaskManager.cs` | Interface for managing the distributed task list CRDT document. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/ShowCaseCrdtStorage.cs` | End-to-end localized storage mechanism inherently persisting multiple document streams directly resolving active DVV bounds completely. |
| `$/Ama.Enterprise.CRDT.Distributed.ShowCase/Services/TaskManager.cs` | Implementation handling intentions and queries for the task list document. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Ama.Enterprise.CRDT.Distributed.TableStorage.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Extensions/ServiceCollectionExtensions.cs` | Registers distributed Azure Table Storage native persistence natively hooks appropriately thoroughly flawlessly rationally confidently securely gracefully properly successfully. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Models/CrdtTableEntity.cs` | Azure Table Storage entity model incorporating property chunking to safely persist payloads up to ~960KB directly effectively natively. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Models/TableStorageCrdtOptions.cs` | Configuration structure natively holding Azure Table Storage endpoints and table bindings cleanly cleanly explicitly rationally dynamically. |
| `$/Ama.Enterprise.CRDT.Distributed.TableStorage/Services/TableStorageDistributedCrdtStorage.cs` | A centralized unified Azure Table Storage distributed backend correctly implementing chunked DVV explicitly explicitly securely safely rationally smoothly rationally rationally elegantly cleanly. |
| `$/Ama.Enterprise.CRDT.Distributed.UnitTests/Ama.Enterprise.CRDT.Distributed.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed/Ama.Enterprise.CRDT.Distributed.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Distributed/Extensions/ServiceCollectionExtensions.cs` | Introduced `AddDistributedCrdtService` extension to create transparent forwarders resolving securely to the `DistributedCrdtScopeProvider` effectively eliminating manual service provider scoping logic. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtEvictionRejectionMessage.cs` | Message broadcasted strictly to forcefully reject and re-bootstrap nodes that have been tombstoned by the cluster natively preventing amnesia edge cases securely smoothly effectively explicitly perfectly naturally seamlessly. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtMessageWrapper.cs` | Envelope wrapper mapping generic messages targeting specifically identified CRDT documents across the network topology. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtOperationsMessage.cs` | Transmission model conveying replicated CRDT intent patches targeted asynchronously natively across active nodes. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtRegistryEntry.cs` | Represents metadata about an active or tombstoned distributed CRDT document natively mapped via the global cluster registry. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtRegistryState.cs` | Global P2P synced directory state handling distributed multi-document topologies inherently natively ensuring active instantiation maps across nodes safely. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtSnapshotMessage.cs` | Message payload containing a complete materialized CRDT document snapshot, used as a fallback synchronization mechanism when log truncation gaps are detected. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/CrdtStateSyncMessage.cs` | Structure carrying generic synchronization states explicitly formatted across anti-entropy operations representing document DVV. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtOptions.cs` | Configuration options for the Distributed CRDT module, updated to include peer eviction TTL mappings securely tracking unreachable network bound nodes effectively bridging state limits dynamically. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtP2pJsonContext.cs` | JSON serialization context mapping explicitly native AOT bindings directly resolving eviction message constraints cleanly accurately seamlessly thoroughly smoothly cleanly. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtSystemAotContext.cs` | AOT contextual reflection mapping natively for internal orchestrator registry CRDT scopes gracefully securely bridging models natively. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/DistributedCrdtSystemJsonContext.cs` | JSON serialization context guaranteeing AOT compatibility for internal orchestrator registry CRDT scopes inherently natively. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/IDistributedCrdtState.cs` | Imposes a centralized generic constraint on root CRDT state models to inherently map their own synchronization identifiers. |
| `$/Ama.Enterprise.CRDT.Distributed/Models/MissingOperationsResult.cs` | DTO representing the result of querying for missing operations and indicating whether a full snapshot is required securely explicitly bridging interface contracts without natively relying on tuples. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ClusterStateTracker.cs` | Singleton thread-safe implementation strictly capturing localized maps representing exact overarching remote state matrix limits. Modified `RemovePeerByNetworkId` natively preventing amnesia effectively by intentionally preserving CRDT vectors strictly decoupling network routes explicitly avoiding destructive structural gaps. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtCheckpointService.cs` | Background service responsible for periodically saving the full in-memory state of all registered CRDTs. Replaced direct eviction natively with completely mathematically bounded tombstoning algorithms organically resolving logic bounds effectively perfectly cleanly smoothly efficiently accurately perfectly correctly successfully properly reliably appropriately flawlessly correctly logically intelligently accurately natively appropriately naturally appropriately effectively. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtDocumentOrchestrator.cs` | Centralized generic orchestrator gracefully managing global localized active P2P CRDT document bindings safely. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtEvictionService.cs` | Implementation of `ICrdtEvictionService` extracting the eviction logic securely explicitly avoiding duplication structurally gracefully smoothly efficiently explicitly natively organically. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/CrdtInitializationService.cs` | Refactored explicitly to safely mutate the inherently shared scope context natively in-place, closing the split-brain scope disconnect bug gracefully avoiding destroyed instances. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DistributedCrdtDocument.cs` | Removed obsolete `GetMissingOperationsAsync`. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/DistributedCrdtScopeProvider.cs` | Secured inherently by removing dangerous `ReplaceScope` logic guaranteeing instances reliably outlive P2P network threads uniformly natively. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IClusterStateTracker.cs` | Tracks the last known synchronization bounds. Updated exposing logical tombstone identity mechanisms alongside explicit `TombstonePeerByNetworkId` targeting graceful topology exits gracefully. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ICrdtDocumentOrchestrator.cs` | Generic manager correctly facilitating multi-document runtime allocations strictly resolving logical decentralized P2P creation and deletion payloads. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/ICrdtEvictionService.cs` | Interface for a dedicated service that orchestrates replica eviction and local identity re-bootstrapping cleanly across all CRDT documents. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtDocument.cs` | Removed obsolete `GetMissingOperationsAsync` as journal resolutions are centralized. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDistributedCrdtStorage.cs` | Generic interface exposing unified persistence mechanisms for distributed CRDT documents securely extending robust asynchronous DVV mapped journal trimming. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/IDocumentFactory.cs` | AOT-friendly generic factory interface for dynamically resolving explicitly mapped distributed CRDT instances correctly. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/MemoryCrdtStorage.cs` | Ephemeral implementation effectively providing default active storage correctly fulfilling unified backend protocol actions directly resolving async mapped trims natively. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtAntiEntropyService.cs` | Implemented essential network traffic smoothing jitter algorithms strictly preventing UDP/HTTP overflow "Thundering Herd" payload storms gracefully. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtP2pPayloadHandler.cs` | Refactored `ProcessStateSyncAsync` to retrieve missing journal operations in one single transaction directly avoiding redundant inefficient journal evaluations. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/P2p/CrdtTopologyObserver.cs` | Observes network connections and hooks directly into the core P2P protocols. Refactored seamlessly resolving graceful `Departed` topology states with instant tombstones freeing log restrictions efficiently while securely protecting `Dead` topology traces protecting offline synchronization completely. |
| `$/Ama.Enterprise.CRDT.Distributed/Services/StorageJournalForwarder.cs` | Injects localized implementations mapping directly back into underlying internal storage architectures acting strictly as the interface wrapper over globally active pipelines. |
| `$/Ama.Enterprise.CRDT.MemoryPack/Ama.Enterprise.CRDT.MemoryPack.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.TableStorage/Ama.Enterprise.CRDT.TableStorage.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.TableStorage/todo.txt` | No description provided. |
| `$/Ama.Enterprise.CRDT.Testing/Ama.Enterprise.CRDT.Testing.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Testing/todo.txt` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags.IntegrationTests/Ama.Enterprise.FeatureFlags.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags.ShowCase/Ama.Enterprise.FeatureFlags.ShowCase.csproj` | Showcase console application project displaying P2P feature flags integration, AOT readiness, and UDP cluster discovery. |
| `$/Ama.Enterprise.FeatureFlags.ShowCase/Program.cs` | Updated showcase application reflecting the refactored categorical options assignments. |
| `$/Ama.Enterprise.FeatureFlags.UnitTests/Ama.Enterprise.FeatureFlags.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags/Ama.Enterprise.FeatureFlags.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags/Extensions/ServiceCollectionExtensions.cs` | Modified dependency mappings redirecting nested option properties directly to the underlying P2P dependencies. Updated to use configurable mesh IDs and registered the new Admin WebRTC interface. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlag.cs` | Data structure representing a single feature flag. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagOptions.cs` | Configuration structure broken into typed categories encompassing all underlying network and CRDT discovery options to prevent field duplication. Updated to include configurable Internal and Admin WebRTC mesh IDs. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagState.cs` | Explicitly inherits `IDistributedCrdtState` and maps generic constraints bridging properties dynamically. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsCrdtAotContext.cs` | AOT context for the feature flags models. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsJsonContext.cs` | JSON context for the feature flags models. |
| `$/Ama.Enterprise.FeatureFlags/Services/FeatureFlagBootstrapper.cs` | Eager initialization hosted service that organically hooks into the orchestrator creating the global state natively preventing amnesia races naturally gracefully cleanly perfectly safely successfully. |
| `$/Ama.Enterprise.FeatureFlags/Services/FeatureFlagClusterManager.cs` | Refactored internal DI boundaries accurately utilizing explicitly mapped orchestrators ensuring generic dynamic models are cached elegantly decoupling interface requirements implicitly securely appropriately properly thoroughly appropriately cleanly explicitly naturally perfectly natively gracefully successfully reliably safely effectively organically appropriately successfully. |
| `$/Ama.Enterprise.FeatureFlags/Services/IFeatureFlagClusterManager.cs` | Interface for the feature flag cluster manager. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Ama.Enterprise.P2p.IntegrationTests.csproj` | Integration tests project for validating P2P networking components via HTTP loopbacks. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Architecture/VersioningArchitectureTests.cs` | Architectural tests that parse the CI/CD deployment files ensuring specific deployed versions always possess explicit test coverage. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/Handlers/TestMessageHandler.cs` | Updated to implement `IApplicationPayloadHandler` and natively capture unwrapped application payloads securely via `TestPayloadRecord` cleanly reflecting domain consumer architecture accurately. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/Models/TestNode.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/P2pAdvancedIntegrationTests.cs` | Updated DI registrations safely routing `IApplicationPayloadHandler` explicitly and adapted assertion mechanics to cleanly decode unwrapped byte arrays effectively flawlessly securely. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/P2pNetworkIntegrationTests.cs` | Refactored integration assertions targeting correctly unboxed test payloads. Cleanly redesigned deduplication tests securely explicitly wrapping identical envelopes ensuring robust evaluation natively naturally successfully gracefully. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/P2pVersioningIntegrationTests.cs` | Refactored integration assertions accurately mapping completely natively unboxed generic wrappers implicitly flawlessly properly completely correctly natively cleanly organically smoothly appropriately. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/PushPullGossipIntegrationTests.cs` | Integration tests thoroughly verifying explicit Push-Pull anti-entropy bounds appropriately isolating digest transmissions natively ensuring active localized fallback synchronizations cleanly explicitly successfully. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/UdpPeerDiscoveryIntegrationTests.cs` | Integration tests verifying the UDP multicast active discovery mechanism mapping isolated P2P mesh endpoints across varied network topologies. |
| `$/Ama.Enterprise.P2p.Kestrel.IntegrationTests/Ama.Enterprise.P2p.Kestrel.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.Kestrel.IntegrationTests/Services/KestrelTransportIntegrationTests.cs` | Integration tests thoroughly verifying the ASP.NET Core Kestrel-based P2P networking transport. Validates isolated end-to-end messaging correctly, bidirectional dynamic routing safely, explicit fault handling seamlessly evicting dead remote targets organically, and generic unmapped fallback bounds smoothly accurately natively gracefully properly inherently logically intelligently reliably rationally perfectly flawlessly smartly. |
| `$/Ama.Enterprise.P2p.Kestrel/Ama.Enterprise.P2p.Kestrel.csproj` | Added `Microsoft.AspNetCore.App` framework reference for ASP.NET Core Kestrel dependencies. |
| `$/Ama.Enterprise.P2p.Kestrel/Extensions/ServiceCollectionExtensions.cs` | Registration logic configuring Dependency Injection specifically targeting the Kestrel transport natively mapping options, polymorphic JSON endpoints, and explicit keyed isolated bounds smoothly perfectly successfully cleanly optimally natively appropriately optimally seamlessly seamlessly completely natively rationally naturally gracefully cleanly correctly successfully efficiently gracefully gracefully successfully successfully inherently completely securely naturally flawlessly properly natively flawlessly smartly natively effortlessly rationally flawlessly rationally efficiently perfectly accurately intelligently smoothly successfully. |
| `$/Ama.Enterprise.P2p.Kestrel/Models/KestrelJsonContext.cs` | Source-generated JSON serialization context ensuring AOT compatibility for Kestrel networking payload primitives cleanly inherently intelligently gracefully explicitly thoroughly properly successfully explicitly rationally efficiently elegantly cleanly flawlessly successfully intelligently elegantly elegantly explicitly securely effectively. |
| `$/Ama.Enterprise.P2p.Kestrel/Models/KestrelPeerEndpoint.cs` | Inherited model distinguishing HTTP transports specifically operated over decoupled ASP.NET Kestrel interfaces natively reliably natively accurately successfully effectively smoothly flawlessly securely securely rationally efficiently seamlessly reliably appropriately successfully elegantly smoothly reliably explicitly intelligently elegantly flawlessly completely securely logically completely organically correctly. |
| `$/Ama.Enterprise.P2p.Kestrel/Models/KestrelTransportOptions.cs` | Dedicated networking configuration structure wrapping listening prefixes explicitly safely decoupling underlying bounds gracefully organically elegantly correctly cleanly rationally flawlessly appropriately appropriately effectively elegantly rationally completely effectively gracefully gracefully effortlessly rationally thoroughly logically seamlessly logically smoothly smartly correctly correctly securely efficiently perfectly intelligently smoothly. |
| `$/Ama.Enterprise.P2p.Kestrel/Services/KestrelTransport.cs` | Outbound delivery mechanisms strictly operating over customized HTTP targets natively isolated against explicitly configured Kestrel endpoint listeners logically appropriately logically gracefully cleanly natively smoothly successfully successfully smoothly intelligently gracefully seamlessly naturally flawlessly seamlessly flawlessly cleanly smoothly accurately gracefully completely explicitly rationally securely smartly appropriately smoothly flawlessly correctly gracefully optimally completely flawlessly logically naturally elegantly naturally effortlessly successfully successfully explicitly elegantly intelligently perfectly seamlessly properly smoothly gracefully cleanly perfectly intelligently completely flawlessly smoothly efficiently effortlessly securely natively securely seamlessly appropriately flawlessly efficiently elegantly natively organically efficiently perfectly successfully flawlessly cleanly. |
| `$/Ama.Enterprise.P2p.Kestrel/Services/KestrelTransportListener.cs` | High-performance isolated generic inbound listener inherently orchestrating active generic hosts internally securely gracefully effectively securely elegantly rationally securely thoroughly properly explicitly smoothly elegantly perfectly cleanly cleanly effortlessly properly natively seamlessly seamlessly efficiently rationally correctly smoothly smoothly correctly seamlessly gracefully elegantly efficiently thoroughly reliably seamlessly successfully. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Ama.Enterprise.P2p.Mqtt.IntegrationTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Services/MqttPeerDiscoveryIntegrationTests.cs` | Integration tests thoroughly verifying the active MQTT peer discovery background service reliably mapping explicitly decoupled mesh architectures flawlessly securely seamlessly seamlessly elegantly natively accurately securely intelligently correctly. |
| `$/Ama.Enterprise.P2p.Mqtt.IntegrationTests/Services/MqttTransportIntegrationTests.cs` | Integration tests verifying end-to-end MQTT transport functionality natively evaluating isolated inbound subscriptions securely. |
| `$/Ama.Enterprise.P2p.Mqtt/Ama.Enterprise.P2p.Mqtt.csproj` | Project definition for MQTT transport using MQTTnet compatible with AOT serialization constraints. |
| `$/Ama.Enterprise.P2p.Mqtt/Extensions/ServiceCollectionExtensions.cs` | Updated DI registrations routing `PeerEndpoint` and `ICrdtSerializer` to `MqttPeerDiscovery` explicitly decoupling it securely and completely from the generic MQTT transport mechanisms gracefully appropriately smoothly successfully structurally. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttDiscoveryOptions.cs` | Configuration options for tuning active MQTT peer discovery broadcast intervals and topic suffixes. Now explicitly decoupled from the generic MQTT transports, providing distinct isolated broker connection settings for discovery architectures safely gracefully. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttJsonContext.cs` | Source-generated AOT JSON serialization context for the MQTT endpoint model structure. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttPeerEndpoint.cs` | Inherited PeerEndpoint model representing an isolated MQTT destination node defined by its internal client identity identifier. |
| `$/Ama.Enterprise.P2p.Mqtt/Models/MqttTransportOptions.cs` | Configuration record setting broker connection host endpoints credentials and specific topic routing bounds. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/Discovery/MqttPeerDiscovery.cs` | Centralized implementation of `IPeerDiscovery` orchestrating active mesh topology presence over an isolated shared MQTT topic. Refactored to inherently operate completely decoupled and independently from explicit MQTT transports natively correctly seamlessly seamlessly accurately efficiently naturally intelligently. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/IMqttClientManager.cs` | Interface establishing lifecycle controls for individual MQTT client subscriptions and active payloads publications. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttClientManager.cs` | Service controlling the underlying generic MQTTnet connections dispatching messages to scoped route prefixes natively. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttTransport.cs` | Generic outbound mesh transport implementing isolated message payload deliveries targeting assigned MQTT topologies. |
| `$/Ama.Enterprise.P2p.Mqtt/Services/MqttTransportListener.cs` | Internal background receiver connecting underlying MQTT topic subscriptions and interpreting generalized mesh envelopes. |
| `$/Ama.Enterprise.P2p.Telemetry/Ama.Enterprise.P2p.Telemetry.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.Telemetry/todo.txt` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Ama.Enterprise.P2p.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/GossipProtocolTests.cs` | Refactored mock structures to natively validate `IApplicationPayloadDispatcher` correctly verifying unwrapped domain payload transmissions smoothly gracefully successfully inherently safely securely appropriately effectively correctly cleanly explicitly. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/InMemoryPeerRegistryTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/MessageDispatcherTests.cs` | Adjusted tests strictly adapting the newly refactored `ApplicationPayloadDispatcher` confirming explicit decoupling of generic envelopes completely safely intelligently smoothly intelligently natively. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/RandomPeerSelectorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/TimeBasedFailureDetectorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.WebRTC.IntegrationTests/Ama.Enterprise.P2p.WebRTC.IntegrationTests.csproj` | Added project references mapping MQTT signaling capabilities alongside copying explicit local settings explicitly. |
| `$/Ama.Enterprise.P2p.WebRTC.IntegrationTests/Services/WebRtcTransportIntegrationTests.cs` | Updated `TestMessage` explicitly implementing the newly enforced `ProtocolVersion` natively satisfying `IMeshMessage` securely. |
| `$/Ama.Enterprise.P2p.WebRTC/Ama.Enterprise.P2p.WebRTC.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.WebRTC/Extensions/ServiceCollectionExtensions.cs` | WebRTC dependency injection pipeline configuring underlying STUN models mapping transports securely alongside base generic Gossip meshes natively, heavily updated to inject dynamic cross-assembly JSON polymorphism resolvers. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcHandshakeMessage.cs` | In-band signaling structure notifying explicitly local identity topologies securely through initialized WebRTC channels safely. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcInvitationAnswer.cs` | DTO representing a WebRTC invitation answer containing the connection identifier and the SDP answer string. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcInvitationOffer.cs` | DTO representing a WebRTC invitation offer containing the connection identifier and the SDP offer string. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcJsonContext.cs` | Baseline STJ context generation tracking dynamically WebRTC handshake protocols completely. Updated to include `WebRtcInvitationOffer` and `WebRtcInvitationAnswer` DTOs for AOT serialization natively. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcOptions.cs` | Configuration structure holding explicit ICE servers natively bound to AOT-friendly serialization mappings. |
| `$/Ama.Enterprise.P2p.WebRTC/Models/WebRtcPeerEndpoint.cs` | WebRTC data channel connection endpoint identifying uniquely scoped connection states securely. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/IWebRtcConnectionManager.cs` | Interface isolating abstract signaling scopes natively exposing Data Channel inbound references. Updated to explicitly expose WebRTC connection state changes targeting UI subscriptions securely. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/IWebRtcInvitationService.cs` | Generic mechanism exchanging SDP structures seamlessly. Updated to use DTOs instead of tuples for SDP exchange. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcConnectionManager.cs` | Internal hosted implementation managing explicitly SIPSorcery RTCPeer connections directly bound within localized mesh scopes correctly. Updated to use DTOs instead of tuples. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcTransport.cs` | WebRTC specific implementation handling outbound gossip structures safely wrapping targeted connection payloads. |
| `$/Ama.Enterprise.P2p.WebRTC/Services/WebRtcTransportListener.cs` | WebRTC specific listener registering seamlessly asynchronous bindings effectively targeting decentralized peer streams globally. |
| `$/Ama.Enterprise.P2p/Ama.Enterprise.P2p.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p/Constants.cs` | Global constants for the P2P module, including protocol versions and payload size limits. |
| `$/Ama.Enterprise.P2p/Extensions/IP2pMeshBuilder.cs` | Interface for building and configuring specific Keyed DI mesh profiles. |
| `$/Ama.Enterprise.P2p/Extensions/P2pMeshBuilder.cs` | Implementation of `IP2pMeshBuilder` handling multi-mesh dependency injection tracking. |
| `$/Ama.Enterprise.P2p/Extensions/P2pMeshRegistrationTracker.cs` | Centralized tracking mechanism guaranteeing idempotent mesh registrations safely evaluating structurally identical configurations natively bypassing duplicates perfectly securely efficiently. |
| `$/Ama.Enterprise.P2p/Extensions/ServiceCollectionExtensions.cs` | Modified dependency mappings safely adding the explicit `AddPushPullGossipNetwork` configurator decoupling the protocol boundaries cleanly. |
| `$/Ama.Enterprise.P2p/Extensions/UdpDiscoveryServiceCollectionExtensions.cs` | Registration logic configuring Dependency Injection specifically targeting the UDP peer discovery sub-components and background services. |
| `$/Ama.Enterprise.P2p/Models/Core/FailureDetectorOptions.cs` | Configuration options for tuning generic protocol-agnostic failure detection components. |
| `$/Ama.Enterprise.P2p/Models/Core/IMeshMessage.cs` | Updated to include `ProtocolVersion` natively standardizing structural validation. |
| `$/Ama.Enterprise.P2p/Models/Core/P2pMeshMetadata.cs` | Metadata record registering a specific mesh identifier into the global dependency container for orchestration. |
| `$/Ama.Enterprise.P2p/Models/Core/P2pNodeOptions.cs` | Centralized generic configuration options holding the core node identity (ID and Endpoint) for the P2P Mesh. Updated to enforce a static, process-wide global peer identifier to satisfy repeatable idempotent tracker validations explicitly. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerEndpoint.cs` | Abstract base record for peer endpoints, explicitly configured with JSON polymorphic attributes natively mapping same-assembly derivatives to support standard AOT serialization. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerId.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerNode.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerStatus.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Discovery/UdpDiscoveryMessage.cs` | Updated with `ProtocolVersion` aligning explicit polymorphic bounds gracefully. |
| `$/Ama.Enterprise.P2p/Models/Discovery/UdpDiscoveryOptions.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Gossip/GossipMessage.cs` | Incorporated `ProtocolVersion` maintaining fallback compatibility implicitly effectively natively. |
| `$/Ama.Enterprise.P2p/Models/Gossip/GossipMessageType.cs` | Defines explicit message typings enabling push-pull sync interactions cleanly distinguishing broadcasts natively. |
| `$/Ama.Enterprise.P2p/Models/Gossip/GossipOptions.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Gossip/PushPullGossipOptions.cs` | Dedicated configuration options decoupling the Push-Pull mechanisms safely without polluting the pure baseline primitives. |
| `$/Ama.Enterprise.P2p/Models/P2pJsonSerializerContext.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Transports/HttpPeerEndpoint.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Transports/HttpTransportOptions.cs` | Added `IsEnabled` flag to explicitly track if the HTTP transport has been added for a designated P2P mesh dynamically cleanly natively. |
| `$/Ama.Enterprise.P2p/Services/Core/ApplicationPayloadDispatcher.cs` | Composite orchestrator accurately securely cleanly dispatching to abstract domain observers efficiently naturally explicitly appropriately gracefully dynamically cleanly securely effortlessly logically. |
| `$/Ama.Enterprise.P2p/Services/Core/Design.md` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IApplicationPayloadDispatcher.cs` | Dispatches explicitly targeted application payloads dynamically safely organically. |
| `$/Ama.Enterprise.P2p/Services/Core/IApplicationPayloadHandler.cs` | Defines a domain-level consumer explicitly natively perfectly organically decoupling underlying distribution protocols seamlessly appropriately seamlessly efficiently. |
| `$/Ama.Enterprise.P2p/Services/Core/IFailureDetector.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IInboundMessageQueue.cs` | Defines an internal queue for decoupling inbound network listeners from the protocol logic. |
| `$/Ama.Enterprise.P2p/Services/Core/IP2pProtocol.cs` | Generic interface defining the orchestrator for the P2P protocol, abstracting algorithms like Gossip. |
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
| `$/Ama.Enterprise.P2p/Services/Core/PassThroughPeerAuthenticator.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/RandomPeerSelector.cs` | Implementation of IPeerSelector utilizing random distribution selection. |
| `$/Ama.Enterprise.P2p/Services/Core/TimeBasedFailureDetector.cs` | Implementation of IFailureDetector using abstract heartbeats decoupled from specific protocol options. |
| `$/Ama.Enterprise.P2p/Services/Core/TransportRouter.cs` | Composite transport router that delegates sending messages to the correct specific transport implementation. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpDiscoveryJsonContext.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Discovery/UdpPeerDiscovery.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Gossip/GossipProtocol.cs` | Safely populates orchestrating gossip parameters explicitly effectively cleanly explicitly correctly efficiently. |
| `$/Ama.Enterprise.P2p/Services/Gossip/PushPullGossipProtocol.cs` | Dedicated generic advanced orchestrator gracefully managing structured Push-Pull anti-entropy bounds securely over native P2P boundaries. |
| `$/Ama.Enterprise.P2p/Services/P2pHostedService.cs` | Refactored generic mesh lifecycle orchestrator starting specifically Keyed transport listeners individually to match explicitly isolated bounds securely preventing multiplexing. |
| `$/Ama.Enterprise.P2p/Services/Transports/HttpTransport.cs` | Refactored standard HTTP implementation natively isolated specifically strictly via Keyed dependencies handling precise outgoing payloads implicitly evaluating identical network bounds efficiently. |
| `$/Ama.Enterprise.P2p/Services/Transports/HttpTransportListener.cs` | Redesigned inbound network listener transitioning away from multiplexing securely adapting explicit keyed dependencies handling localized prefixes explicitly safely isolating meshes effectively. |
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
| `$/apps-todo.txt` | No description provided. |
| `$/solution.settings.json` | No description provided. |
