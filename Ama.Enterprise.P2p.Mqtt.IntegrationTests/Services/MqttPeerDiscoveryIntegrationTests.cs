namespace Ama.Enterprise.P2p.Mqtt.IntegrationTests.Services;

using System;
using System.Collections.Generic;
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
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        using var topologySemaphore = new SemaphoreSlim(0);
        Action onTopologyChanged = () => topologySemaphore.Release();

        testOutputHelper.WriteLine("Initializing Nodes for Discovery...");
        await using var nodeA = CreateDiscoveryTestNode(meshId, peerAId, topicPrefix, onTopologyChanged, handshakePort: 9001);
        await using var nodeB = CreateDiscoveryTestNode(meshId, peerBId, topicPrefix, onTopologyChanged, handshakePort: 9002);

        // Act
        testOutputHelper.WriteLine("Starting MQTT Peer Discovery background services...");
        await nodeA.StartDiscoveryAsync(cts.Token);
        await nodeB.StartDiscoveryAsync(cts.Token);

        // Assert - Use a polling loop augmented with observer notifications to wait for public broker subscriptions and broadcasts to propagate
        testOutputHelper.WriteLine("Waiting for discovery broadcasts to synchronize across the broker...");
        
        bool discovered = false;
        var timeoutTime = DateTime.UtcNow.AddSeconds(60);

        while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
        {
            var peersA = await nodeA.Registry.GetAllPeersAsync(meshId, cts.Token);
            var peersB = await nodeB.Registry.GetAllPeersAsync(meshId, cts.Token);

            var aDiscoveredB = peersA.FirstOrDefault(p => p.Id.Equals(peerBId));
            var bDiscoveredA = peersB.FirstOrDefault(p => p.Id.Equals(peerAId));

            if (aDiscoveredB.Endpoint != null && bDiscoveredA.Endpoint != null)
            {
                aDiscoveredB.Endpoint.ShouldBeOfType<MqttPeerEndpoint>();
                bDiscoveredA.Endpoint.ShouldBeOfType<MqttPeerEndpoint>();

                discovered = true;
                
                testOutputHelper.WriteLine($"Node A discovered {peersA.Count()} peers.");
                testOutputHelper.WriteLine($"Node B discovered {peersB.Count()} peers.");
                break;
            }

            await topologySemaphore.WaitAsync(TimeSpan.FromMilliseconds(500), cts.Token);
        }

        discovered.ShouldBeTrue("Nodes failed to discover each other within the expected timeout limit.");

        testOutputHelper.WriteLine("Stopping discovery loops...");
        await nodeA.StopDiscoveryAsync(cts.Token);
        await nodeB.StopDiscoveryAsync(cts.Token);
    }
    
    [IntegrationFact]
    public async Task MqttPeerDiscovery_FiveNodes_ShouldDiscoverEachOther_Succeeds()
    {
        // Arrange
        var meshId = $"mqtt-five-disc-{Guid.NewGuid():N}";
        var topicPrefix = $"integration-test/five-disc/{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120)); // Generous timeout for multiple nodes hitting public broker
        
        var peers = Enumerable.Range(0, 5).Select(_ => new PeerId(Guid.NewGuid())).ToList();
        var nodes = new List<MqttDiscoveryTestNode>();

        using var topologySemaphore = new SemaphoreSlim(0);
        Action onTopologyChanged = () => topologySemaphore.Release();

        testOutputHelper.WriteLine("Initializing 5 Nodes for MQTT Discovery...");
        int basePort = 9020;
        foreach (var peerId in peers)
        {
            nodes.Add(CreateDiscoveryTestNode(meshId, peerId, topicPrefix, onTopologyChanged, handshakePort: ++basePort));
        }

        try
        {
            // Act
            testOutputHelper.WriteLine("Starting MQTT Peer Discovery background services for 5 nodes concurrently...");
            var startTasks = nodes.Select(n => n.StartDiscoveryAsync(cts.Token));
            await Task.WhenAll(startTasks);

            // Assert
            testOutputHelper.WriteLine("Waiting for discovery broadcasts to synchronize across the broker for all 5 nodes...");
            
            bool allSynchronized = false;
            var timeoutTime = DateTime.UtcNow.AddSeconds(90);

            while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
            {
                allSynchronized = true;

                foreach (var node in nodes)
                {
                    var registryPeers = await node.Registry.GetAllPeersAsync(meshId, cts.Token);
                    var missing = peers.Where(p => p != node.Id && !registryPeers.Any(rp => rp.Id.Equals(p) && rp.Endpoint != null)).ToList();
                    
                    if (missing.Any())
                    {
                        allSynchronized = false;
                        break;
                    }
                }

                if (allSynchronized)
                {
                    testOutputHelper.WriteLine("All 5 nodes successfully discovered each other.");
                    break;
                }

                await topologySemaphore.WaitAsync(TimeSpan.FromMilliseconds(1000), cts.Token);
            }

            allSynchronized.ShouldBeTrue("The 5-node cluster failed to fully discover each other within the expected timeout limit. This verifies cluster stabilization and bounds validation with multiple concurrent topic publications.");
        }
        finally
        {
            testOutputHelper.WriteLine("Stopping discovery loops and cleaning up nodes...");
            foreach (var node in nodes)
            {
                await node.DisposeAsync();
            }
        }
    }

    [IntegrationFact]
    public async Task MqttPeerDiscovery_WithTcpTransport_DiscoversTcpEndpoints_Succeeds()
    {
        // Arrange
        var meshId = $"mqtt-tcp-disc-{Guid.NewGuid():N}";
        var topicPrefix = $"integration-test/tcp-disc/{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());
        
        var portA = 8401;
        var portB = 8402;

        using var topologySemaphore = new SemaphoreSlim(0);
        Action onTopologyChanged = () => topologySemaphore.Release();

        testOutputHelper.WriteLine("Initializing TCP Nodes for MQTT Discovery...");
        await using var nodeA = CreateDiscoveryTestNode(meshId, peerAId, topicPrefix, onTopologyChanged, useTcpTransport: true, tcpPort: portA, handshakePort: 9031);
        await using var nodeB = CreateDiscoveryTestNode(meshId, peerBId, topicPrefix, onTopologyChanged, useTcpTransport: true, tcpPort: portB, handshakePort: 9032);

        // Act
        testOutputHelper.WriteLine("Starting MQTT Peer Discovery background services for TCP endpoints...");
        await nodeA.StartDiscoveryAsync(cts.Token);
        await nodeB.StartDiscoveryAsync(cts.Token);

        // Assert
        testOutputHelper.WriteLine("Waiting for discovery broadcasts to synchronize across the broker...");
        
        bool discovered = false;
        var timeoutTime = DateTime.UtcNow.AddSeconds(45);

        while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
        {
            var peersA = await nodeA.Registry.GetAllPeersAsync(meshId, cts.Token);
            var peersB = await nodeB.Registry.GetAllPeersAsync(meshId, cts.Token);

            var aDiscoveredB = peersA.FirstOrDefault(p => p.Id.Equals(peerBId));
            var bDiscoveredA = peersB.FirstOrDefault(p => p.Id.Equals(peerAId));

            if (aDiscoveredB.Endpoint == null || bDiscoveredA.Endpoint == null)
            {
                await topologySemaphore.WaitAsync(TimeSpan.FromMilliseconds(500), cts.Token);
                continue;
            }

            aDiscoveredB.Endpoint.ShouldBeOfType<TcpPeerEndpoint>();
            bDiscoveredA.Endpoint.ShouldBeOfType<TcpPeerEndpoint>();

            var bTcpEndpoint = (TcpPeerEndpoint)aDiscoveredB.Endpoint;
            bTcpEndpoint.Host.ShouldBe("127.0.0.1");
            bTcpEndpoint.Port.ShouldBe(portB);

            discovered = true;
            break;
        }

        discovered.ShouldBeTrue("Nodes failed to discover each other's TCP endpoints within the expected timeout limit.");

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
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());
        
        var portA = 8601;
        var portB = 8602;

        using var topologySemaphore = new SemaphoreSlim(0);
        Action onTopologyChanged = () => topologySemaphore.Release();

        testOutputHelper.WriteLine("Initializing Multi-Mesh Nodes for MQTT Discovery...");
        await using var nodeA = CreateMultiMeshNode(peerAId, mesh1Id, mesh2Id, onTopologyChanged, portA, topicPrefix1, topicPrefix2, 9041, 9042);
        await using var nodeB = CreateMultiMeshNode(peerBId, mesh1Id, mesh2Id, onTopologyChanged, portB, topicPrefix1, topicPrefix2, 9051, 9052);

        // Act
        testOutputHelper.WriteLine("Starting background services...");
        await nodeA.StartAsync(cts.Token);
        await nodeB.StartAsync(cts.Token);

        // Assert
        testOutputHelper.WriteLine("Waiting for discovery broadcasts to synchronize across both meshes...");
        
        bool mesh1Discovered = false;
        bool mesh2Discovered = false;
        var timeoutTime = DateTime.UtcNow.AddSeconds(60);

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
                    aDiscoveredB1.Endpoint.ShouldBeOfType<TcpPeerEndpoint>();
                    bDiscoveredA1.Endpoint.ShouldBeOfType<TcpPeerEndpoint>();
                    
                    var aTcpEndpoint = (TcpPeerEndpoint)aDiscoveredB1.Endpoint;
                    var bTcpEndpoint = (TcpPeerEndpoint)bDiscoveredA1.Endpoint;
                    
                    aTcpEndpoint.Port.ShouldBe(portB);
                    bTcpEndpoint.Port.ShouldBe(portA);
                    
                    mesh1Discovered = true;
                    testOutputHelper.WriteLine("Mesh 1 (TCP) synchronized.");
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

            await topologySemaphore.WaitAsync(TimeSpan.FromMilliseconds(500), cts.Token);
        }

        mesh1Discovered.ShouldBeTrue("Nodes failed to discover each other on Mesh 1 (TCP).");
        mesh2Discovered.ShouldBeTrue("Nodes failed to discover each other on Mesh 2 (MQTT).");

        testOutputHelper.WriteLine("Stopping discovery loops...");
        await nodeA.StopAsync(cts.Token);
        await nodeB.StopAsync(cts.Token);
    }

    private MqttDiscoveryTestNode CreateDiscoveryTestNode(string meshId, PeerId peerId, string topicPrefix, Action onTopologyChanged, bool useTcpTransport = false, int tcpPort = 0, int handshakePort = 0)
    {
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });
        
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        services.AddSingleton<IPeerTopologyObserver>(new TestTopologyObserver(onTopologyChanged));

        services.Configure<FailureDetectorOptions>(meshId, options => 
        {
            options.HeartbeatInterval = TimeSpan.FromSeconds(120);
        });

        var meshBuilder = services.AddP2pMesh(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        })
        .AddGossipNetwork();

        if (useTcpTransport)
        {
            meshBuilder.AddTcpTransport(options =>
            {
                options.ListenHost = "127.0.0.1";
                options.ListenPort = tcpPort;
            });
        }
        else
        {
            meshBuilder.AddMqttTransport(options =>
            {
                options.Host = "broker.freemqtt.com";
                options.Username = "freemqtt";
                options.Password = "public";
                options.Port = 1883;
                options.TopicPrefix = topicPrefix;
            });
        }

        meshBuilder.AddMqttPeerHandshake(options =>
        {
            options.Host = "broker.freemqtt.com";
            options.Username = "freemqtt";
            options.Password = "public";
            options.Port = 1883;
            options.TopicPrefix = topicPrefix;
            options.HandshakeTimeout = TimeSpan.FromSeconds(10);
            if (handshakePort > 0)
            {
                options.HandshakePort = handshakePort;
            }
        });

        meshBuilder.AddMqttPeerDiscovery(options =>
        {
            options.Host = "broker.freemqtt.com";
            options.Username = "freemqtt";
            options.Password = "public";
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
        Action onTopologyChanged,
        int mesh1TcpPort,
        string topicPrefix1,
        string topicPrefix2,
        int handshakePort1,
        int handshakePort2)
    {
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });
        
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        services.AddSingleton<IPeerTopologyObserver>(new TestTopologyObserver(onTopologyChanged));

        services.Configure<FailureDetectorOptions>(mesh1Id, options => 
        {
            options.HeartbeatInterval = TimeSpan.FromSeconds(120);
        });

        services.Configure<FailureDetectorOptions>(mesh2Id, options => 
        {
            options.HeartbeatInterval = TimeSpan.FromSeconds(120);
        });

        services.AddP2pMesh(mesh1Id, options =>
        {
            options.LocalPeerId = peerId.Value;
        })
        .AddGossipNetwork()
        .AddTcpTransport(options =>
        {
            options.ListenHost = "127.0.0.1";
            options.ListenPort = mesh1TcpPort;
        })
        .AddMqttPeerHandshake(options =>
        {
            options.Host = "broker.freemqtt.com";
            options.Username = "freemqtt";
            options.Password = "public";
            options.Port = 1883;
            options.TopicPrefix = topicPrefix1;
            options.HandshakeTimeout = TimeSpan.FromSeconds(10);
            if (handshakePort1 > 0)
            {
                options.HandshakePort = handshakePort1;
            }
        })
        .AddMqttPeerDiscovery(options =>
        {
            options.Host = "broker.freemqtt.com";
            options.Username = "freemqtt";
            options.Password = "public";
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
        .AddMqttTransport(options =>
        {
            options.Host = "broker.freemqtt.com";
            options.Username = "freemqtt";
            options.Password = "public";
            options.Port = 1883;
            options.TopicPrefix = topicPrefix2;
        })
        .AddMqttPeerHandshake(options =>
        {
            options.Host = "broker.freemqtt.com";
            options.Username = "freemqtt";
            options.Password = "public";
            options.Port = 1883;
            options.TopicPrefix = topicPrefix2;
            options.HandshakeTimeout = TimeSpan.FromSeconds(10);
            if (handshakePort2 > 0)
            {
                options.HandshakePort = handshakePort2;
            }
        })
        .AddMqttPeerDiscovery(options =>
        {
            options.Host = "broker.freemqtt.com";
            options.Username = "freemqtt";
            options.Password = "public";
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
            var hostedServices = Provider.GetServices<IHostedService>();
            foreach (var hostedService in hostedServices)
            {
                await hostedService.StartAsync(cancellationToken);
            }
        }

        public async Task StopDiscoveryAsync(CancellationToken cancellationToken)
        {
            var hostedServices = Provider.GetServices<IHostedService>();
            foreach (var hostedService in hostedServices.Reverse())
            {
                await hostedService.StopAsync(cancellationToken);
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

    private sealed class TestTopologyObserver : IPeerTopologyObserver
    {
        private readonly Action onTopologyChanged;

        public TestTopologyObserver(Action onTopologyChanged)
        {
            this.onTopologyChanged = onTopologyChanged ?? throw new ArgumentNullException(nameof(onTopologyChanged));
        }

        public Task OnPeerJoinedAsync(string meshId, PeerNode node, CancellationToken cancellationToken)
        {
            onTopologyChanged();
            return Task.CompletedTask;
        }

        public Task OnPeerDepartedAsync(string meshId, PeerId peerId, CancellationToken cancellationToken)
        {
            onTopologyChanged();
            return Task.CompletedTask;
        }

        public Task OnPeerStatusChangedAsync(string meshId, PeerId peerId, PeerStatus newStatus, CancellationToken cancellationToken)
        {
            onTopologyChanged();
            return Task.CompletedTask;
        }
    }
}