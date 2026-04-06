| File Path | Description |
| --- | --- |
| `$/.editorconfig` | No description provided. |
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
| `$/Ama.Enterprise.CRDT.OpenTelemetry/Ama.Enterprise.CRDT.OpenTelemetry.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.TableStorage/Ama.Enterprise.CRDT.TableStorage.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Testing/Ama.Enterprise.CRDT.Testing.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.UI/Ama.Enterprise.CRDT.UI.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT/Ama.Enterprise.CRDT.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags.UnitTests/Ama.Enterprise.FeatureFlags.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags/Ama.Enterprise.FeatureFlags.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Ama.Enterprise.P2p.IntegrationTests.csproj` | Integration tests project for validating P2P networking components via HTTP loopbacks. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Attributes/IntegrationFactAttribute.cs` | Custom xUnit `FactAttribute` providing a centralized toggle to enable or disable all integration tests. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Handlers/TestMessageHandler.cs` | Test handler implementation that captures received gossip messages in memory for assertions. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Models/TestNode.cs` | Data-transfer class orchestrating individual configured test nodes managing service scopes and lifecycles. |
| `$/Ama.Enterprise.P2p.IntegrationTests/P2pAdvancedIntegrationTests.cs` | Contains advanced integration tests validating high concurrency, multi-hop linear topologies, and TTL expiration behavior within the P2P network. |
| `$/Ama.Enterprise.P2p.IntegrationTests/P2pNetworkIntegrationTests.cs` | End-to-end integration tests validating real-world message propagation, network lifecycles, and deduplication capabilities. |
| `$/Ama.Enterprise.P2p.UnitTests/Ama.Enterprise.P2p.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Extensions/ServiceCollectionExtensionsTests.cs` | Unit tests for DI setup logic validating proper registration of core gossip services. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/GossipProtocolTests.cs` | Unit tests for GossipProtocol covering the background loop, message deduplication, and local message dispatch. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/HttpTransportListenerTests.cs` | Unit tests for HttpTransportListener checking constructor constraints and lifecycle. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/HttpTransportTests.cs` | Unit tests for HttpTransport ensuring proper HttpClient requests to peer endpoints are constructed. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/InMemoryPeerRegistryTests.cs` | Unit tests for the thread-safe `InMemoryPeerRegistry` validating peer state changes and observer notifications. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/MessageDispatcherTests.cs` | Unit tests for MessageDispatcher verifying message routing to multiple handlers and robust exception handling. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/P2pHostedServiceTests.cs` | Unit tests covering start/stop lifecycle management tied to the generic host. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/PassThroughPeerAuthenticatorTests.cs` | Unit tests for the `PassThroughPeerAuthenticator` ensuring all valid peers pass without complex checks. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/RandomPeerSelectorTests.cs` | Unit tests validating edge cases and list shuffling in the `RandomPeerSelector`. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/SystemTextJsonGossipSerializerTests.cs` | Unit tests assuring payload serialization and fault-tolerance when parsing broken JSON. |
| `$/Ama.Enterprise.P2p.UnitTests/Services/TimeBasedFailureDetectorTests.cs` | Unit tests managing interval-based evaluations verifying nodes transition to Suspect and Dead accurately. |
| `$/Ama.Enterprise.P2p/Ama.Enterprise.P2p.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p/Constants.cs` | Global constants for the P2P module, including protocol versions and payload size limits. |
| `$/Ama.Enterprise.P2p/Extensions/ServiceCollectionExtensions.cs` | Extension methods for setting up P2P DI configuration and options registration. |
| `$/Ama.Enterprise.P2p/Models/GossipMessage.cs` | Core message DTO enveloping the payload, sender ID, message ID, and TTL for P2P transport. |
| `$/Ama.Enterprise.P2p/Models/GossipOptions.cs` | Class providing configuration for the gossip protocol (interval, fanout, TTL) using the Options pattern. |
| `$/Ama.Enterprise.P2p/Models/PeerEndpoint.cs` | DTO mapping an IP/hostname and a port for peer network reachability. |
| `$/Ama.Enterprise.P2p/Models/PeerId.cs` | DTO representing a unique peer identifier using an underlying GUID. |
| `$/Ama.Enterprise.P2p/Models/PeerNode.cs` | DTO combining a peer's identity (`PeerId`) and network reachability (`PeerEndpoint`). |
| `$/Ama.Enterprise.P2p/Models/PeerStatus.cs` | Enum defining the lifecycle states of a peer (Active, Suspect, Dead). |
| `$/Ama.Enterprise.P2p/Services/GossipProtocol.cs` | Implementation of IGossipProtocol handling the periodic background loop, state tracking, deduplication, and routing of P2P messages. |
| `$/Ama.Enterprise.P2p/Services/HttpTransport.cs` | Implementation of ITransport utilizing IHttpClientFactory to send outbound serialized gossip messages using HTTP POST. |
| `$/Ama.Enterprise.P2p/Services/HttpTransportListener.cs` | Implementation of ITransportListener utilizing an HttpListener for capturing and deserializing inbound network gossip traffic over HTTP. |
| `$/Ama.Enterprise.P2p/Services/IFailureDetector.cs` | Interface providing logic for health evaluation and heartbeats to accurately determine node death/suspicion. |
| `$/Ama.Enterprise.P2p/Services/IGossipProtocol.cs` | Interface orchestrating the core logic, starting/stopping the P2P loop, and handling broadcast intent. |
| `$/Ama.Enterprise.P2p/Services/IGossipSerializer.cs` | Interface isolating the AOT-friendly System.Text.Json serialization logic for P2P messages. |
| `$/Ama.Enterprise.P2p/Services/IMessageDispatcher.cs` | Interface defining a bus/dispatcher to decouple the network protocol from the application handlers (CRDTs). |
| `$/Ama.Enterprise.P2p/Services/IMessageHandler.cs` | Interface implemented by the domain (like the CRDT engine) to react to parsed incoming gossip data. |
| `$/Ama.Enterprise.P2p/Services/IP2pTelemetry.cs` | Interface abstracting network observability, ready to be implemented by the OpenTelemetry project. |
| `$/Ama.Enterprise.P2p/Services/IPeerAuthenticator.cs` | Interface acting as the security boundary to validate and authenticate peers attempting to connect. |
| `$/Ama.Enterprise.P2p/Services/IPeerDiscovery.cs` | Interface defining mechanisms for discovering existing peers within the network context. |
| `$/Ama.Enterprise.P2p/Services/IPeerRegistry.cs` | Interface for managing the active routing table, storing and removing known peers. |
| `$/Ama.Enterprise.P2p/Services/IPeerSelector.cs` | Interface for algorithms that select a subset of peers to target during a gossip tick (e.g., random selection). |
| `$/Ama.Enterprise.P2p/Services/IPeerTopologyObserver.cs` | Interface for subscribing to peer network topology changes (joins, departures, status changes). |
| `$/Ama.Enterprise.P2p/Services/ITransport.cs` | Interface abstracting the outbound network calls for sending gossip messages. |
| `$/Ama.Enterprise.P2p/Services/ITransportListener.cs` | Interface abstracting the inbound network listeners and message callbacks. |
| `$/Ama.Enterprise.P2p/Services/InMemoryPeerRegistry.cs` | Implementation of IPeerRegistry storing active peers in a thread-safe in-memory dictionary and notifying topology observers. |
| `$/Ama.Enterprise.P2p/Services/MessageDispatcher.cs` | Implementation of IMessageDispatcher that routes incoming gossip messages to all registered IMessageHandler instances, ensuring errors in one handler do not crash the dispatch process. |
| `$/Ama.Enterprise.P2p/Services/P2pHostedService.cs` | Standard .NET `IHostedService` implementation to manage the background lifecycle of the Gossip protocol. |
| `$/Ama.Enterprise.P2p/Services/PassThroughPeerAuthenticator.cs` | Default implementation of IPeerAuthenticator that accepts all incoming peer connections. |
| `$/Ama.Enterprise.P2p/Services/RandomPeerSelector.cs` | Implementation of IPeerSelector that randomizes available active peers for gossip distribution. |
| `$/Ama.Enterprise.P2p/Services/SystemTextJsonGossipSerializer.cs` | Implementation of IGossipSerializer using AOT-friendly System.Text.Json source generators. |
| `$/Ama.Enterprise.P2p/Services/TimeBasedFailureDetector.cs` | Implementation of IFailureDetector using heartbeats and time intervals to determine peer health. |
| `$/Ama.Enterprise.slnx` | No description provided. |
| `$/CodingStandards.md` | No description provided. |
| `$/FilesDescription.md` | No description provided. |
| `$/LICENCE` | No description provided. |
| `$/solution.settings.json` | No description provided. |
