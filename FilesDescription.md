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
| `$/Ama.Enterprise.CRDT.OpenTelemetry/Ama.Enterprise.CRDT.OpenTelemetry.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.TableStorage/Ama.Enterprise.CRDT.TableStorage.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.Testing/Ama.Enterprise.CRDT.Testing.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT.UI/Ama.Enterprise.CRDT.UI.csproj` | No description provided. |
| `$/Ama.Enterprise.CRDT/Ama.Enterprise.CRDT.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags.UnitTests/Ama.Enterprise.FeatureFlags.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags/Ama.Enterprise.FeatureFlags.csproj` | No description provided. |
| `$/Ama.Enterprise.FeatureFlags/Extensions/ServiceCollectionExtensions.cs` | DI extension methods for registering the feature flags module. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlag.cs` | Data structure representing a single feature flag. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagState.cs` | Root CRDT document model containing the state of all feature flags. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsCrdtAotContext.cs` | AOT context for the feature flags models. |
| `$/Ama.Enterprise.FeatureFlags/Models/FeatureFlagsJsonContext.cs` | JSON context for the feature flags models. |
| `$/Ama.Enterprise.FeatureFlags/Models/P2p/FeatureFlagMessageWrapper.cs` | Envelope wrapper for feature flag messages sent over the P2P network. |
| `$/Ama.Enterprise.FeatureFlags/Models/P2p/FeatureFlagOperationsMessage.cs` | DTO containing missing CRDT operations transmitted in response to an anti-entropy state sync. |
| `$/Ama.Enterprise.FeatureFlags/Models/P2p/FeatureFlagP2pJsonContext.cs` | System.Text.Json AOT serialization context for P2P feature flag data transmission models. |
| `$/Ama.Enterprise.FeatureFlags/Models/P2p/FeatureFlagStateSyncMessage.cs` | DTO carrying the Dotted Version Vector for anti-entropy synchronization over P2P. |
| `$/Ama.Enterprise.FeatureFlags/Services/FeatureFlagClusterManager.cs` | Implementation of the feature flag cluster manager using DVV sync. |
| `$/Ama.Enterprise.FeatureFlags/Services/IFeatureFlagClusterManager.cs` | Interface for the feature flag cluster manager. |
| `$/Ama.Enterprise.FeatureFlags/Services/MemoryJournal.cs` | Thread-safe memory journal for CRDT operations in the feature flags module. |
| `$/Ama.Enterprise.FeatureFlags/Services/P2p/FeatureFlagAntiEntropyService.cs` | Background service that intermittently broadcasts the native replica state over Gossip to trigger feature flag sync across peers. |
| `$/Ama.Enterprise.FeatureFlags/Services/P2p/FeatureFlagGossipHandler.cs` | Message handler implementing P2P logic to parse payload wrappers and execute CRDT anti-entropy sync locally. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Ama.Enterprise.P2p.IntegrationTests.csproj` | Integration tests project for validating P2P networking components via HTTP loopbacks. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Architecture/VersioningArchitectureTests.cs` | Architectural tests that parse the CI/CD deployment files ensuring specific deployed versions always possess explicit test coverage. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Attributes/IntegrationFactAttribute.cs` | Custom xUnit `FactAttribute` providing a centralized toggle to enable or disable all integration tests. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Attributes/TestedProtocolVersionAttribute.cs` | Custom attribute utilized by structural reflection tests to declare protocol versions explicitly covered by a method. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Extensions/XunitLoggingBuilderExtensions.cs` | Extension methods to register xUnit logger in ILoggingBuilder. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/Handlers/TestMessageHandler.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/Models/TestNode.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/P2pAdvancedIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/P2pNetworkIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/P2pVersioningIntegrationTests.cs` | Integration tests verifying backwards compatibility and explicit deployment protocol versioning constraints. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Gossip/UdpPeerDiscoveryIntegrationTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Logging/XunitLogger.cs` | Custom ILogger implementation for routing logs to xUnit's ITestOutputHelper. |
| `$/Ama.Enterprise.P2p.IntegrationTests/Logging/XunitLoggerProvider.cs` | Provider for creating XunitLogger instances. |
| `$/Ama.Enterprise.P2p.TableStorage/Ama.Enterprise.P2p.TableStorage.csproj` | Serverless-focused Azure Table Storage integration for P2P state management. |
| `$/Ama.Enterprise.P2p.TableStorage/Extensions/ServiceCollectionExtensions.cs` | DI extension methods for registering the Table Storage peer registry. |
| `$/Ama.Enterprise.P2p.TableStorage/Models/TableStorageRegistryOptions.cs` | Configuration options for the Table Storage peer registry. |
| `$/Ama.Enterprise.P2p.TableStorage/Services/TableStoragePeerRegistry.cs` | Implementation of `IPeerRegistry` utilizing Azure Table Storage, optimized for ephemeral/serverless compute nodes. |
| `$/Ama.Enterprise.P2p.UnitTests/Ama.Enterprise.P2p.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Extensions/ServiceCollectionExtensionsTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/GossipProtocolTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/HttpTransportListenerTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/HttpTransportTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/InMemoryPeerRegistryTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/MessageDispatcherTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/P2pHostedServiceTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/PassThroughPeerAuthenticatorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/RandomPeerSelectorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/SystemTextJsonGossipSerializerTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p.UnitTests/Gossip/Services/TimeBasedFailureDetectorTests.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Ama.Enterprise.P2p.csproj` | No description provided. |
| `$/Ama.Enterprise.P2p/Constants.cs` | Global constants for the P2P module, including protocol versions and payload size limits. |
| `$/Ama.Enterprise.P2p/Extensions/ServiceCollectionExtensions.cs` | Extension methods for setting up P2P DI configuration and options registration. |
| `$/Ama.Enterprise.P2p/Extensions/UdpDiscoveryServiceCollectionExtensions.cs` | Registration logic configuring Dependency Injection specifically targeting the UDP peer discovery sub-components and background services. |
| `$/Ama.Enterprise.P2p/Models/Core/FailureDetectorOptions.cs` | Configuration options for tuning generic protocol-agnostic failure detection components. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerEndpoint.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerId.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerNode.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/PeerStatus.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Core/UdpDiscoveryOptions.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Gossip/GossipMessage.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Gossip/GossipOptions.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Models/Gossip/P2pJsonSerializerContext.cs` | AOT-friendly JSON context for P2P models, automatically loaded by ICrdtSerializer. |
| `$/Ama.Enterprise.P2p/Services/Core/IFailureDetector.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IMessageDispatcher.cs` | Generic interface routing incoming protocol messages to registered handlers. |
| `$/Ama.Enterprise.P2p/Services/Core/IMessageHandler.cs` | Generic interface defining a domain-level consumer for P2P messages. |
| `$/Ama.Enterprise.P2p/Services/Core/IMessageSerializer.cs` | Obsolete generic message serializer interface, superseded by ICrdtSerializer. |
| `$/Ama.Enterprise.P2p/Services/Core/IP2pTelemetry.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerAuthenticator.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerDiscovery.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerRegistry.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerSelector.cs` | Interface for algorithms that select a generic subset of peers for communication. |
| `$/Ama.Enterprise.P2p/Services/Core/IPeerTopologyObserver.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransport.cs` | Generic interface defining the outbound network transport capabilities for sending generic messages to peers. |
| `$/Ama.Enterprise.P2p/Services/Core/ITransportListener.cs` | Generic interface defining the inbound network listener capabilities for receiving protocol messages. |
| `$/Ama.Enterprise.P2p/Services/Core/InMemoryPeerRegistry.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/MessageDispatcher.cs` | Implements the generic message dispatcher for routing parsed P2P messages. |
| `$/Ama.Enterprise.P2p/Services/Core/PassThroughPeerAuthenticator.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/RandomPeerSelector.cs` | Implementation of IPeerSelector utilizing random distribution selection. |
| `$/Ama.Enterprise.P2p/Services/Core/TimeBasedFailureDetector.cs` | Implementation of IFailureDetector using abstract heartbeats decoupled from specific protocol options. |
| `$/Ama.Enterprise.P2p/Services/Core/UdpDiscoveryJsonContext.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Core/UdpPeerDiscovery.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Gossip/GossipProtocol.cs` | Re-architected implementation of IGossipProtocol targeting generic transport and dispatch interfaces. |
| `$/Ama.Enterprise.P2p/Services/Gossip/HttpTransport.cs` | Implements outbound gossip transport via HTTP, now leveraging ICrdtSerializer. |
| `$/Ama.Enterprise.P2p/Services/Gossip/HttpTransportListener.cs` | Implements inbound gossip listener via HttpListener, utilizing ICrdtSerializer. |
| `$/Ama.Enterprise.P2p/Services/Gossip/IGossipProtocol.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Gossip/P2pHostedService.cs` | No description provided. |
| `$/Ama.Enterprise.P2p/Services/Gossip/SystemTextJsonGossipSerializer.cs` | Obsolete SystemTextJson implementation, superseded by ICrdtSerializer. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/Ama.Enterprise.Project.Analyzers.UnitTests.csproj` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/PropertyInfoUsageAnalyzerTests.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers.UnitTests/SystemConvertUsageAnalyzerTests.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/Ama.Enterprise.Project.Analyzers.csproj` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/PropertyInfoUsageAnalyzer.cs` | No description provided. |
| `$/Ama.Enterprise.Project.Analyzers/SystemConvertUsageAnalyzer.cs` | No description provided. |
| `$/Ama.Enterprise.slnx` | No description provided. |
| `$/CodingStandards.md` | No description provided. |
| `$/FilesDescription.md` | No description provided. |
| `$/LICENCE` | No description provided. |
| `$/solution.settings.json` | No description provided. |
