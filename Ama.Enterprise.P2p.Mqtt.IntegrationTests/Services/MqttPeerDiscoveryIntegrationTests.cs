namespace Ama.Enterprise.P2p.Mqtt.IntegrationTests.Services;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Mqtt.Extensions;
using Ama.Enterprise.P2p.Mqtt.Models;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

public sealed class MqttPeerDiscoveryIntegrationTests
{
    private readonly ITestOutputHelper testOutputHelper;

    public MqttPeerDiscoveryIntegrationTests(ITestOutputHelper testOutputHelper)
    {
        this.testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    }

    [IntegrationFact]
    public async Task MqttPeerDiscovery_TwoNodes_DiscoverEachOther_Succeeds()
    {
        // Arrange
        var meshId = $"mqtt-disc-{Guid.NewGuid():N}";
        var topicPrefix = $"integration-test/disc/{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        testOutputHelper.WriteLine("Initializing Nodes for Discovery...");
        await using var nodeA = CreateDiscoveryTestNode(meshId, peerAId, topicPrefix);
        await using var nodeB = CreateDiscoveryTestNode(meshId, peerBId, topicPrefix);

        // Act
        testOutputHelper.WriteLine("Starting MQTT Peer Discovery background services...");
        await nodeA.StartDiscoveryAsync(cts.Token);
        await nodeB.StartDiscoveryAsync(cts.Token);

        // Assert - Use a polling loop to cleanly wait for public broker subscriptions and broadcasts to propagate natively
        testOutputHelper.WriteLine("Waiting for discovery broadcasts to synchronize across the broker...");
        
        bool discovered = false;
        var timeoutTime = DateTime.UtcNow.AddSeconds(20);

        while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
        {
            var peersA = await nodeA.Registry.GetAllPeersAsync(meshId, cts.Token);
            var peersB = await nodeB.Registry.GetAllPeersAsync(meshId, cts.Token);

            var aDiscoveredB = peersA.FirstOrDefault(p => p.Id.Equals(peerBId));
            var bDiscoveredA = peersB.FirstOrDefault(p => p.Id.Equals(peerAId));

            if (aDiscoveredB.Endpoint != null && bDiscoveredA.Endpoint != null)
            {
                // Verify the dynamically distributed generic endpoints correctly match standard MQTT parameters naturally
                aDiscoveredB.Endpoint.ShouldBeOfType<MqttPeerEndpoint>();
                bDiscoveredA.Endpoint.ShouldBeOfType<MqttPeerEndpoint>();

                discovered = true;
                
                testOutputHelper.WriteLine($"Node A discovered {peersA.Count()} peers.");
                testOutputHelper.WriteLine($"Node B discovered {peersB.Count()} peers.");
                break;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);
        }

        discovered.ShouldBeTrue("Nodes failed to discover each other within the expected timeout limit.");

        testOutputHelper.WriteLine("Stopping discovery loops...");
        await nodeA.StopDiscoveryAsync(cts.Token);
        await nodeB.StopDiscoveryAsync(cts.Token);
    }

    [IntegrationFact]
    public async Task MqttPeerDiscovery_ExplicitManualDiscovery_PopulatesRecentPeers_Succeeds()
    {
        // Arrange
        var meshId = $"mqtt-manual-disc-{Guid.NewGuid():N}";
        var topicPrefix = $"integration-test/manual/{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        await using var nodeA = CreateDiscoveryTestNode(meshId, peerAId, topicPrefix);
        await using var nodeB = CreateDiscoveryTestNode(meshId, peerBId, topicPrefix);

        // We only start Node B's background listener so it can respond to Node A's manual ping natively
        await nodeB.StartDiscoveryAsync(cts.Token);

        // Node A just starts its hosted service to establish the baseline MQTT connection without waiting for the loop
        if (nodeA.Discovery is IHostedService hostedA)
        {
            await hostedA.StartAsync(cts.Token);
        }

        // Delay sufficiently to guarantee both nodes have connected their MQTT subscriptions successfully before pinging
        await Task.Delay(TimeSpan.FromSeconds(30), cts.Token);

        // Act & Assert - Explicitly loop manual pings resolving initial broker subscription latencies effectively
        testOutputHelper.WriteLine("Firing explicit DiscoverPeersAsync roundtrips from Node A...");
        
        bool manuallyDiscovered = false;
        var timeoutTime = DateTime.UtcNow.AddSeconds(20);
        
        while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
        {
            var discoveredByA = await nodeA.Discovery.DiscoverPeersAsync(cts.Token);
            
            // Due to public broker latencies, the response might fall slightly outside the DiscoverPeersAsync internal timeout
            // but the background subscriber will catch it and push it into the registry natively.
            var registeredPeers = await nodeA.Registry.GetAllPeersAsync(meshId, cts.Token);
            
            if (discoveredByA.Any(n => n.Id.Equals(peerBId)) || registeredPeers.Any(p => p.Id.Equals(peerBId)))
            {
                manuallyDiscovered = true;
                break;
            }

            // Await briefly before retrying to prevent network spam
            await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
        }

        manuallyDiscovered.ShouldBeTrue("Node A failed to manually discover Node B within the timeout bound natively.");

        // Cleanup
        if (nodeA.Discovery is IHostedService cleanupHostedA)
        {
            await cleanupHostedA.StopAsync(cts.Token);
        }
        await nodeB.StopDiscoveryAsync(cts.Token);
    }

    [IntegrationFact]
    public async Task MqttPeerDiscovery_WithHttpTransport_DiscoversHttpEndpoints_Succeeds()
    {
        // Arrange
        var meshId = $"mqtt-http-disc-{Guid.NewGuid():N}";
        var topicPrefix = $"integration-test/http-disc/{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());
        
        var portA = 8401;
        var portB = 8402;

        testOutputHelper.WriteLine("Initializing HTTP Nodes for MQTT Discovery...");
        await using var nodeA = CreateDiscoveryTestNode(meshId, peerAId, topicPrefix, useHttpTransport: true, httpPort: portA);
        await using var nodeB = CreateDiscoveryTestNode(meshId, peerBId, topicPrefix, useHttpTransport: true, httpPort: portB);

        // Act
        testOutputHelper.WriteLine("Starting MQTT Peer Discovery background services for HTTP endpoints...");
        await nodeA.StartDiscoveryAsync(cts.Token);
        await nodeB.StartDiscoveryAsync(cts.Token);

        // Assert
        testOutputHelper.WriteLine("Waiting for discovery broadcasts to synchronize across the broker...");
        
        bool discovered = false;
        var timeoutTime = DateTime.UtcNow.AddSeconds(20);

        while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
        {
            var peersA = await nodeA.Registry.GetAllPeersAsync(meshId, cts.Token);
            var peersB = await nodeB.Registry.GetAllPeersAsync(meshId, cts.Token);

            if (!peersA.Any(p => p.Id.Equals(peerBId)) || !peersB.Any(p => p.Id.Equals(peerAId)))
            {
                await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);
                continue;
            }

            var aDiscoveredB = peersA.FirstOrDefault(p => p.Id.Equals(peerBId));
            var bDiscoveredA = peersB.FirstOrDefault(p => p.Id.Equals(peerAId));

            // Verify correctly that the polymorphic JSON serializer maintained the HTTP endpoints over the MQTT stream natively
            aDiscoveredB.Endpoint.ShouldBeOfType<HttpPeerEndpoint>();
            bDiscoveredA.Endpoint.ShouldBeOfType<HttpPeerEndpoint>();

            var bHttpEndpoint = (HttpPeerEndpoint)aDiscoveredB.Endpoint;
            bHttpEndpoint.Host.ShouldBe("localhost");
            bHttpEndpoint.Port.ShouldBe(portB);

            discovered = true;
            break;
        }

        discovered.ShouldBeTrue("Nodes failed to discover each other's HTTP endpoints within the expected timeout limit natively.");

        testOutputHelper.WriteLine("Stopping discovery loops...");
        await nodeA.StopDiscoveryAsync(cts.Token);
        await nodeB.StopDiscoveryAsync(cts.Token);
    }

    [IntegrationFact]
    public async Task MqttPeerDiscovery_ShouldMapEndpointsCorrectly_WhenUsingMultipleMeshes()
    {
        // Arrange
        var mesh1Id = $"mqtt-multi-1-{Guid.NewGuid():N}";
        var mesh2Id = $"mqtt-multi-2-{Guid.NewGuid():N}";
        
        var topicPrefix1 = $"integration-test/multi-1/{Guid.NewGuid():N}";
        var topicPrefix2 = $"integration-test/multi-2/{Guid.NewGuid():N}";
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());
        
        var portA = 8601;
        var portB = 8602;

        testOutputHelper.WriteLine("Initializing Multi-Mesh Nodes for MQTT Discovery...");
        await using var nodeA = CreateMultiMeshNode(peerAId, mesh1Id, mesh2Id, portA, topicPrefix1, topicPrefix2);
        await using var nodeB = CreateMultiMeshNode(peerBId, mesh1Id, mesh2Id, portB, topicPrefix1, topicPrefix2);

        // Act
        testOutputHelper.WriteLine("Starting background services...");
        await nodeA.StartAsync(cts.Token);
        await nodeB.StartAsync(cts.Token);

        // Assert
        testOutputHelper.WriteLine("Waiting for discovery broadcasts to synchronize across both meshes...");
        
        bool mesh1Discovered = false;
        bool mesh2Discovered = false;
        var timeoutTime = DateTime.UtcNow.AddSeconds(30);

        while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
        {
            if (!mesh1Discovered)
            {
                var peers1A = await nodeA.Registry.GetAllPeersAsync(mesh1Id, cts.Token);
                var peers1B = await nodeB.Registry.GetAllPeersAsync(mesh1Id, cts.Token);
                
                var aDiscoveredB1 = peers1A.FirstOrDefault(p => p.Id.Equals(peerBId));
                var bDiscoveredA1 = peers1B.FirstOrDefault(p => p.Id.Equals(peerAId));

                if (aDiscoveredB1.Endpoint != null && bDiscoveredA1.Endpoint != null)
                {
                    aDiscoveredB1.Endpoint.ShouldBeOfType<HttpPeerEndpoint>();
                    bDiscoveredA1.Endpoint.ShouldBeOfType<HttpPeerEndpoint>();
                    
                    var aHttpEndpoint = (HttpPeerEndpoint)aDiscoveredB1.Endpoint;
                    var bHttpEndpoint = (HttpPeerEndpoint)bDiscoveredA1.Endpoint;
                    
                    aHttpEndpoint.Port.ShouldBe(portB);
                    bHttpEndpoint.Port.ShouldBe(portA);
                    
                    mesh1Discovered = true;
                    testOutputHelper.WriteLine("Mesh 1 (HTTP) synchronized.");
                }
            }

            if (!mesh2Discovered)
            {
                var peers2A = await nodeA.Registry.GetAllPeersAsync(mesh2Id, cts.Token);
                var peers2B = await nodeB.Registry.GetAllPeersAsync(mesh2Id, cts.Token);
                
                var aDiscoveredB2 = peers2A.FirstOrDefault(p => p.Id.Equals(peerBId));
                var bDiscoveredA2 = peers2B.FirstOrDefault(p => p.Id.Equals(peerAId));

                if (aDiscoveredB2.Endpoint != null && bDiscoveredA2.Endpoint != null)
                {
                    aDiscoveredB2.Endpoint.ShouldBeOfType<MqttPeerEndpoint>();
                    bDiscoveredA2.Endpoint.ShouldBeOfType<MqttPeerEndpoint>();
                    
                    mesh2Discovered = true;
                    testOutputHelper.WriteLine("Mesh 2 (MQTT) synchronized.");
                }
            }

            if (mesh1Discovered && mesh2Discovered)
            {
                break;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);
        }

        mesh1Discovered.ShouldBeTrue("Nodes failed to discover each other on Mesh 1 (HTTP).");
        mesh2Discovered.ShouldBeTrue("Nodes failed to discover each other on Mesh 2 (MQTT).");

        testOutputHelper.WriteLine("Stopping discovery loops...");
        await nodeA.StopAsync(cts.Token);
        await nodeB.StopAsync(cts.Token);
    }

    private MqttDiscoveryTestNode CreateDiscoveryTestNode(string meshId, PeerId peerId, string topicPrefix, bool useHttpTransport = false, int httpPort = 0)
    {
        var services = new ServiceCollection();

        // Register core CRDT capabilities natively
        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });
        
        // Core P2P dependencies inherently required by discovery
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        var meshBuilder = services.AddP2pMesh(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        })
        .AddGossipNetwork();

        if (useHttpTransport)
        {
            // Explicitly evaluate injecting HTTP endpoints through MQTT discovery streams securely
            meshBuilder.AddHttpTransport(options =>
            {
                options.ListenHost = "localhost";
                options.ListenPort = httpPort;
                options.PathPrefix = "/p2p/gossip/";
            });
        }
        else
        {
            // Standard localized MQTT endpoint integration natively
            meshBuilder.AddMqttTransport(options =>
            {
                options.Host = "test.mosquitto.org";
                options.Port = 1883;
                options.TopicPrefix = topicPrefix;
            });
        }

        meshBuilder.AddMqttPeerDiscovery(options =>
        {
            options.Host = "test.mosquitto.org";
            options.Port = 1883;
            options.TopicPrefix = topicPrefix;
            options.DiscoveryInterval = TimeSpan.FromSeconds(5);
            options.DiscoveryTimeout = TimeSpan.FromSeconds(10);
            options.DiscoveryTopicSuffix = "discovery";
        });

        var provider = services.BuildServiceProvider();

        return new MqttDiscoveryTestNode(
            provider,
            peerId,
            provider.GetRequiredKeyedService<IPeerDiscovery>(meshId),
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private MultiMeshMqttTestNode CreateMultiMeshNode(
        PeerId peerId,
        string mesh1Id,
        string mesh2Id,
        int mesh1HttpPort,
        string topicPrefix1,
        string topicPrefix2)
    {
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });
        
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        services.AddP2pMesh(mesh1Id, options =>
        {
            options.LocalPeerId = peerId.Value;
        })
        .AddGossipNetwork()
        .AddHttpTransport(options =>
        {
            options.ListenHost = "localhost";
            options.ListenPort = mesh1HttpPort;
            options.PathPrefix = "/p2p/mesh1/";
        })
        .AddMqttPeerDiscovery(options =>
        {
            options.Host = "test.mosquitto.org";
            options.Port = 1883;
            options.TopicPrefix = topicPrefix1;
            options.DiscoveryInterval = TimeSpan.FromSeconds(5);
            options.DiscoveryTimeout = TimeSpan.FromSeconds(10);
            options.DiscoveryTopicSuffix = "discovery";
        });

        services.AddP2pMesh(mesh2Id, options =>
        {
            options.LocalPeerId = peerId.Value;
        })
        .AddPushPullGossipNetwork()
        .AddMqttTransport(options =>
        {
            options.Host = "test.mosquitto.org";
            options.Port = 1883;
            options.TopicPrefix = topicPrefix2;
        })
        .AddMqttPeerDiscovery(options =>
        {
            options.Host = "test.mosquitto.org";
            options.Port = 1883;
            options.TopicPrefix = topicPrefix2;
            options.DiscoveryInterval = TimeSpan.FromSeconds(5);
            options.DiscoveryTimeout = TimeSpan.FromSeconds(10);
            options.DiscoveryTopicSuffix = "discovery";
        });

        var provider = services.BuildServiceProvider();

        return new MultiMeshMqttTestNode(
            provider,
            peerId,
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private sealed record MqttDiscoveryTestNode(
        ServiceProvider Provider,
        PeerId Id,
        IPeerDiscovery Discovery,
        IPeerRegistry Registry) : IAsyncDisposable
    {
        public async Task StartDiscoveryAsync(CancellationToken cancellationToken)
        {
            if (Discovery is IHostedService hosted)
            {
                await hosted.StartAsync(cancellationToken);
            }
        }

        public async Task StopDiscoveryAsync(CancellationToken cancellationToken)
        {
            if (Discovery is IHostedService hosted)
            {
                await hosted.StopAsync(cancellationToken);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await StopDiscoveryAsync(CancellationToken.None);
            await Provider.DisposeAsync();
        }
    }

    private sealed record MultiMeshMqttTestNode(
        ServiceProvider Provider,
        PeerId Id,
        IPeerRegistry Registry) : IAsyncDisposable
    {
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            var hostedServices = Provider.GetServices<IHostedService>();
            foreach (var hostedService in hostedServices)
            {
                await hostedService.StartAsync(cancellationToken);
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            var hostedServices = Provider.GetServices<IHostedService>();
            foreach (var hostedService in hostedServices.Reverse())
            {
                await hostedService.StopAsync(cancellationToken);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await StopAsync(CancellationToken.None);
            await Provider.DisposeAsync();
        }
    }
}