namespace Ama.Enterprise.P2p.AspNetCore.IntegrationTests.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.AspNetCore.Extensions;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Ama.Enterprise.UnitTests.Networking;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

public sealed class AspNetCorePeerHandshakeIntegrationTests : IClassFixture<NetworkResourceManager>
{
    private readonly ITestOutputHelper testOutputHelper;
    private readonly NetworkResourceManager resourceManager;

    public AspNetCorePeerHandshakeIntegrationTests(ITestOutputHelper testOutputHelper, NetworkResourceManager resourceManager)
    {
        this.testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
        this.resourceManager = resourceManager ?? throw new ArgumentNullException(nameof(resourceManager));
    }

    [IntegrationFact]
    public async Task AspNetCorePeerHandshaker_DirectHandshake_Succeeds()
    {
        // Arrange
        var meshId = $"aspnetcore-direct-{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        testOutputHelper.WriteLine("Initializing Nodes for Direct ASP.NET Core Standalone Handshake...");
        await using var nodeA = CreateTestNode(meshId, peerAId, portA, null, 0);
        await using var nodeB = CreateTestNode(meshId, peerBId, portB, null, 0);

        // Act
        testOutputHelper.WriteLine("Starting ASP.NET Core Handshaker background services...");
        await nodeA.StartAsync(cts.Token);
        await nodeB.StartAsync(cts.Token);

        // Give web servers a moment to bind and start listening
        await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);

        testOutputHelper.WriteLine($"Node A firing manual HandshakeAsync to Node B explicitly unmapped as 127.0.0.1...");
        
        var localNodeA = new PeerNode(peerAId, new AspNetCorePeerEndpoint("127.0.0.1", portA));
        
        var endpointB = new IPEndPoint(IPAddress.Parse("127.0.0.1"), portB);
        var discoveredNode = await nodeA.Handshaker.HandshakeAsync(localNodeA, endpointB, cts.Token);

        // Assert
        discoveredNode.ShouldNotBeNull("Handshake failed to return a valid PeerNode.");
        discoveredNode.Value.Id.ShouldBe(peerBId);
        discoveredNode.Value.Endpoint.ShouldBeOfType<AspNetCorePeerEndpoint>();

        var aspEndpoint = (AspNetCorePeerEndpoint)discoveredNode.Value.Endpoint;
        aspEndpoint.Port.ShouldBe(portB);

        testOutputHelper.WriteLine("Direct handshake completed successfully.");
    }

    [IntegrationFact]
    public async Task AspNetCorePeerHandshaker_IntegratedMode_DirectHandshake_Succeeds()
    {
        // Arrange
        var meshId = $"aspnetcore-integ-{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        testOutputHelper.WriteLine("Initializing WebApplications for Integrated ASP.NET Core Handshake...");
        await using var appA = CreateIntegratedTestNode(meshId, peerAId, portA);
        await using var appB = CreateIntegratedTestNode(meshId, peerBId, portB);

        // Act
        testOutputHelper.WriteLine("Starting ASP.NET Core apps...");
        await appA.StartAsync(cts.Token);
        await appB.StartAsync(cts.Token);

        // Give web servers a moment to bind and start listening
        await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);

        testOutputHelper.WriteLine($"Node A firing manual HandshakeAsync to Node B...");
        
        var handshakerA = appA.Services.GetRequiredKeyedService<IPeerHandshaker>(meshId);
        
        var localNodeA = new PeerNode(peerAId, new AspNetCorePeerEndpoint("127.0.0.1", portA));
        
        var endpointB = new IPEndPoint(IPAddress.Parse("127.0.0.1"), portB);
        var discoveredNode = await handshakerA.HandshakeAsync(localNodeA, endpointB, cts.Token);

        // Assert
        discoveredNode.ShouldNotBeNull("Handshake failed to return a valid PeerNode.");
        discoveredNode.Value.Id.ShouldBe(peerBId);
        discoveredNode.Value.Endpoint.ShouldBeOfType<AspNetCorePeerEndpoint>();

        var aspEndpoint = (AspNetCorePeerEndpoint)discoveredNode.Value.Endpoint;
        aspEndpoint.Port.ShouldBe(portB);

        testOutputHelper.WriteLine("Integrated mode handshake completed successfully.");
        
        await appA.StopAsync(cts.Token);
        await appB.StopAsync(cts.Token);
    }

    [IntegrationFact]
    public async Task AspNetCorePeerHandshaker_WithUdpDiscovery_DiscoversEndpoints_Succeeds()
    {
        // Arrange
        var meshId = $"aspnetcore-udp-{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();
        
        var multicastGroup = "239.2.2.2";
        var multicastPort = resourceManager.GetNextPort();

        testOutputHelper.WriteLine("Initializing Nodes for UDP Discovery + ASP.NET Core Handshake...");
        await using var nodeA = CreateTestNode(meshId, peerAId, portA, multicastGroup, multicastPort);
        await using var nodeB = CreateTestNode(meshId, peerBId, portB, multicastGroup, multicastPort);

        // Act
        testOutputHelper.WriteLine("Starting P2P infrastructure...");
        await nodeA.StartAsync(cts.Token);
        await nodeB.StartAsync(cts.Token);

        // Assert - Polling loop for UDP multicasts and isolated ASP.NET Core handshakes
        testOutputHelper.WriteLine("Waiting for Phase 1 (UDP) and Phase 2 (ASP.NET Core) to synchronize endpoints natively decoupled...");
        
        bool discovered = false;
        var timeoutTime = DateTime.UtcNow.AddSeconds(45);

        while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
        {
            var peersA = await nodeA.Registry.GetAllPeersAsync(meshId, cts.Token);
            var peersB = await nodeB.Registry.GetAllPeersAsync(meshId, cts.Token);

            var aDiscoveredB = peersA.FirstOrDefault(p => p.Id.Equals(peerBId));
            var bDiscoveredA = peersB.FirstOrDefault(p => p.Id.Equals(peerAId));

            if (aDiscoveredB.Endpoint is not null && bDiscoveredA.Endpoint is not null)
            {
                aDiscoveredB.Endpoint.ShouldBeOfType<AspNetCorePeerEndpoint>();
                bDiscoveredA.Endpoint.ShouldBeOfType<AspNetCorePeerEndpoint>();

                var bEndpoint = (AspNetCorePeerEndpoint)aDiscoveredB.Endpoint;
                bEndpoint.Port.ShouldBe(portB);

                discovered = true;
                break;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);
        }

        discovered.ShouldBeTrue("Nodes failed to discover each other via UDP multicast and ASP.NET Core HTTP handshakes implicitly mapping explicit target ports explicitly securely effectively safely within limits.");
    }

    [IntegrationFact]
    public async Task AspNetCorePeerHandshaker_FiveNodes_ShouldFormClusterAndSynchronize()
    {
        // Arrange
        var meshId = $"aspnetcore-five-{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

        var peers = Enumerable.Range(0, 5).Select(_ => new PeerId(Guid.NewGuid())).ToList();
        var ports = Enumerable.Range(0, 5).Select(_ => resourceManager.GetNextPort()).ToList();
        
        var multicastGroup = "239.5.5.5";
        var multicastPort = resourceManager.GetNextPort();

        var nodes = new List<AspNetCoreTestNode>();
        testOutputHelper.WriteLine("Initializing 5 Nodes for ASP.NET Core/UDP Topology...");
        
        for (int i = 0; i < 5; i++)
        {
            nodes.Add(CreateTestNode(meshId, peers[i], ports[i], multicastGroup, multicastPort));
        }

        try
        {
            // Act
            testOutputHelper.WriteLine("Starting all 5 nodes concurrently...");
            var startTasks = nodes.Select(n => n.StartAsync(cts.Token));
            await Task.WhenAll(startTasks);

            // Assert
            testOutputHelper.WriteLine("Waiting for 5-node cluster to fully synchronize via Gossip...");
            bool allSynchronized = false;
            var timeoutTime = DateTime.UtcNow.AddSeconds(60);

            while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
            {
                allSynchronized = true;
                foreach (var node in nodes)
                {
                    var registryPeers = await node.Registry.GetAllPeersAsync(meshId, cts.Token);
                    // A node should systematically know about all other 4 peers appropriately across the distributed boundaries
                    var missing = peers.Where(p => p != node.Id && !registryPeers.Any(rp => rp.Id.Equals(p) && rp.Endpoint != null)).ToList();
                    
                    if (missing.Any())
                    {
                        allSynchronized = false;
                        break;
                    }
                }

                if (allSynchronized)
                {
                    break;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(1000), cts.Token);
            }

            allSynchronized.ShouldBeTrue("The 5-node cluster failed to fully synchronize its peer registry within the timeout. This checks if the multi-node gossip handles the topology completely.");
        }
        finally
        {
            // Cleanup
            testOutputHelper.WriteLine("Cleaning up 5-node cluster gracefully...");
            foreach (var node in nodes)
            {
                await node.DisposeAsync();
            }
        }
    }

    [IntegrationFact]
    public async Task AspNetCorePeerHandshaker_ShouldMapEndpointsCorrectly_WhenUsingMultipleMeshes()
    {
        // Arrange
        var mesh1Id = $"aspnetcore-multi-1-{Guid.NewGuid():N}";
        var mesh2Id = $"aspnetcore-multi-2-{Guid.NewGuid():N}";
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());
        
        var mesh1PortA = resourceManager.GetNextPort();
        var mesh1PortB = resourceManager.GetNextPort();
        var mesh2PortA = resourceManager.GetNextPort();
        var mesh2PortB = resourceManager.GetNextPort();

        var multicastGroup1 = "239.3.3.3";
        var multicastPort1 = resourceManager.GetNextPort();
        
        var multicastGroup2 = "239.4.4.4";
        var multicastPort2 = resourceManager.GetNextPort();

        testOutputHelper.WriteLine("Initializing Multi-Mesh Nodes using decoupled ASP.NET Core + UDP natively mapped target ports explicitly securely...");
        await using var nodeA = CreateMultiMeshNode(peerAId, mesh1Id, mesh2Id, mesh1PortA, mesh2PortA, multicastGroup1, multicastPort1, multicastGroup2, multicastPort2);
        await using var nodeB = CreateMultiMeshNode(peerBId, mesh1Id, mesh2Id, mesh1PortB, mesh2PortB, multicastGroup1, multicastPort1, multicastGroup2, multicastPort2);

        // Act
        testOutputHelper.WriteLine("Starting background services spanning all isolated meshes...");
        await nodeA.StartAsync(cts.Token);
        await nodeB.StartAsync(cts.Token);

        // Assert
        testOutputHelper.WriteLine("Waiting for isolated discovery processes across both bounds...");
        
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

                if (aDiscoveredB1.Endpoint is not null && bDiscoveredA1.Endpoint is not null)
                {
                    var aEndpoint = (AspNetCorePeerEndpoint)aDiscoveredB1.Endpoint;
                    var bEndpoint = (AspNetCorePeerEndpoint)bDiscoveredA1.Endpoint;
                    
                    aEndpoint.Port.ShouldBe(mesh1PortB);
                    bEndpoint.Port.ShouldBe(mesh1PortA);
                    
                    mesh1Discovered = true;
                    testOutputHelper.WriteLine("Mesh 1 (ASP.NET Core/UDP) synchronized appropriately.");
                }
            }

            if (!mesh2Discovered)
            {
                var peers2A = await nodeA.Registry.GetAllPeersAsync(mesh2Id, cts.Token);
                var peers2B = await nodeB.Registry.GetAllPeersAsync(mesh2Id, cts.Token);
                
                var aDiscoveredB2 = peers2A.FirstOrDefault(p => p.Id.Equals(peerBId));
                var bDiscoveredA2 = peers2B.FirstOrDefault(p => p.Id.Equals(peerAId));

                if (aDiscoveredB2.Endpoint is not null && bDiscoveredA2.Endpoint is not null)
                {
                    var aEndpoint2 = (AspNetCorePeerEndpoint)aDiscoveredB2.Endpoint;
                    
                    aEndpoint2.Port.ShouldBe(mesh2PortB);
                    
                    mesh2Discovered = true;
                    testOutputHelper.WriteLine("Mesh 2 (ASP.NET Core/UDP) synchronized smoothly isolating bounds.");
                }
            }

            if (mesh1Discovered && mesh2Discovered)
            {
                break;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);
        }

        mesh1Discovered.ShouldBeTrue("Nodes failed to discover each other on strictly bound Mesh 1 natively.");
        mesh2Discovered.ShouldBeTrue("Nodes failed to discover each other on implicitly separated Mesh 2 securely.");
    }

    private AspNetCoreTestNode CreateTestNode(string meshId, PeerId peerId, int listenPort, string? multicastGroup, int multicastPort)
    {
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });
        
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        services.AddKeyedSingleton<PeerEndpoint>(meshId, new AspNetCorePeerEndpoint("127.0.0.1", listenPort));

        services.Configure<FailureDetectorOptions>(meshId, options => 
        {
            options.HeartbeatInterval = TimeSpan.FromSeconds(120);
        });

        var meshBuilder = services.AddP2pMesh(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        })
        .AddGossipNetwork()
        .AddAspNetCorePeerHandshake(options =>
        {
            options.HostingMode = AspNetCoreHostingMode.Standalone;
            options.StandaloneListenHost = "+";
            options.StandaloneListenPort = listenPort;
            options.AdvertisedHandshakePort = listenPort;
            options.HandshakeTimeout = TimeSpan.FromSeconds(10);
        });

        if (!string.IsNullOrEmpty(multicastGroup))
        {
            meshBuilder.AddUdpPeerDiscovery(options =>
            {
                options.MulticastAddress = multicastGroup;
                options.MulticastPort = multicastPort;
                options.DiscoveryInterval = TimeSpan.FromSeconds(2);
            });
        }

        var provider = services.BuildServiceProvider();

        return new AspNetCoreTestNode(
            provider,
            peerId,
            provider.GetRequiredKeyedService<IPeerHandshaker>(meshId),
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private WebApplication CreateIntegratedTestNode(string meshId, PeerId peerId, int listenPort)
    {
        var builder = WebApplication.CreateBuilder();

        builder.Logging.AddXunit(testOutputHelper);
        builder.Logging.SetMinimumLevel(LogLevel.Trace);

        builder.Services.AddCrdt();
        builder.Services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        builder.Services.AddKeyedSingleton<PeerEndpoint>(meshId, new AspNetCorePeerEndpoint("127.0.0.1", listenPort));

        builder.Services.Configure<FailureDetectorOptions>(meshId, options => 
        {
            options.HeartbeatInterval = TimeSpan.FromSeconds(120);
        });

        builder.Services.AddP2pMesh(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        })
        .AddGossipNetwork()
        .AddAspNetCorePeerHandshake(options =>
        {
            options.HostingMode = AspNetCoreHostingMode.Integrated;
            options.AdvertisedHandshakePort = listenPort;
            options.HandshakeTimeout = TimeSpan.FromSeconds(10);
            options.PathPrefix = "/test-handshake/";
        });

        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(IPAddress.Parse("127.0.0.1"), listenPort);
        });

        var app = builder.Build();
        app.MapP2pMeshHandshakes("/test-handshake");

        return app;
    }

    private MultiMeshAspNetCoreTestNode CreateMultiMeshNode(
        PeerId peerId,
        string mesh1Id,
        string mesh2Id,
        int listenPort1,
        int listenPort2,
        string multicastGroup1,
        int multicastPort1,
        string multicastGroup2,
        int multicastPort2)
    {
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });
        
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        services.AddKeyedSingleton<PeerEndpoint>(mesh1Id, new AspNetCorePeerEndpoint("127.0.0.1", listenPort1));
        services.AddKeyedSingleton<PeerEndpoint>(mesh2Id, new AspNetCorePeerEndpoint("127.0.0.1", listenPort2));

        services.Configure<FailureDetectorOptions>(mesh1Id, options => options.HeartbeatInterval = TimeSpan.FromSeconds(120));
        services.Configure<FailureDetectorOptions>(mesh2Id, options => options.HeartbeatInterval = TimeSpan.FromSeconds(120));

        services.AddP2pMesh(mesh1Id, options => options.LocalPeerId = peerId.Value)
            .AddGossipNetwork()
            .AddAspNetCorePeerHandshake(options =>
            {
                options.HostingMode = AspNetCoreHostingMode.Standalone;
                options.StandaloneListenHost = "+";
                options.StandaloneListenPort = listenPort1;
                options.AdvertisedHandshakePort = listenPort1;
                options.HandshakeTimeout = TimeSpan.FromSeconds(10);
            })
            .AddUdpPeerDiscovery(options =>
            {
                options.MulticastAddress = multicastGroup1;
                options.MulticastPort = multicastPort1;
                options.DiscoveryInterval = TimeSpan.FromSeconds(2);
            });

        services.AddP2pMesh(mesh2Id, options => options.LocalPeerId = peerId.Value)
            .AddAspNetCorePeerHandshake(options =>
            {
                options.HostingMode = AspNetCoreHostingMode.Standalone;
                options.StandaloneListenHost = "+";
                options.StandaloneListenPort = listenPort2;
                options.AdvertisedHandshakePort = listenPort2;
                options.HandshakeTimeout = TimeSpan.FromSeconds(10);
            })
            .AddUdpPeerDiscovery(options =>
            {
                options.MulticastAddress = multicastGroup2;
                options.MulticastPort = multicastPort2;
                options.DiscoveryInterval = TimeSpan.FromSeconds(2);
            });

        var provider = services.BuildServiceProvider();

        return new MultiMeshAspNetCoreTestNode(
            provider,
            peerId,
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private sealed record AspNetCoreTestNode(
        ServiceProvider Provider,
        PeerId Id,
        IPeerHandshaker Handshaker,
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

    private sealed record MultiMeshAspNetCoreTestNode(
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