namespace Ama.Enterprise.P2p.Kestrel.IntegrationTests.Services;

using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Kestrel.Extensions;
using Ama.Enterprise.P2p.Kestrel.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

public sealed class KestrelPeerHandshakeIntegrationTests
{
    private readonly ITestOutputHelper testOutputHelper;
    private static int portCounter = 15000;

    public KestrelPeerHandshakeIntegrationTests(ITestOutputHelper testOutputHelper)
    {
        this.testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    }

    private static int GetNextPort() => Interlocked.Increment(ref portCounter);

    [IntegrationFact]
    public async Task KestrelPeerHandshaker_DirectHandshake_Succeeds()
    {
        // Arrange
        var meshId = $"kestrel-direct-{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var portA = GetNextPort();
        var portB = GetNextPort();

        testOutputHelper.WriteLine("Initializing Nodes for Direct Kestrel Handshake...");
        await using var nodeA = CreateTestNode(meshId, peerAId, portA, null, 0);
        await using var nodeB = CreateTestNode(meshId, peerBId, portB, null, 0);

        // Act
        testOutputHelper.WriteLine("Starting Kestrel Handshaker background services...");
        await nodeA.StartAsync(cts.Token);
        await nodeB.StartAsync(cts.Token);

        // Give Kestrel servers a moment to bind and start listening
        await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);

        testOutputHelper.WriteLine($"Node A firing manual HandshakeAsync to Node B on port {portB}...");
        
        var targetEndpoint = new IPEndPoint(IPAddress.Loopback, portB);
        var localNodeA = new PeerNode(peerAId, new KestrelPeerEndpoint("127.0.0.1", portA));
        
        var discoveredNode = await nodeA.Handshaker.HandshakeAsync(localNodeA, targetEndpoint, cts.Token);

        // Assert
        discoveredNode.ShouldNotBeNull("Handshake failed to return a valid PeerNode.");
        discoveredNode.Value.Id.ShouldBe(peerBId);
        discoveredNode.Value.Endpoint.ShouldBeOfType<KestrelPeerEndpoint>();

        var kestrelEndpoint = (KestrelPeerEndpoint)discoveredNode.Value.Endpoint;
        kestrelEndpoint.Port.ShouldBe(portB);

        testOutputHelper.WriteLine("Direct handshake completed successfully.");
    }

    [IntegrationFact]
    public async Task KestrelPeerHandshaker_WithUdpDiscovery_DiscoversEndpoints_Succeeds()
    {
        // Arrange
        var meshId = $"kestrel-udp-{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var portA = GetNextPort();
        var portB = GetNextPort();
        
        var multicastGroup = "239.2.2.2";
        var multicastPort = GetNextPort();

        testOutputHelper.WriteLine("Initializing Nodes for UDP Discovery + Kestrel Handshake...");
        await using var nodeA = CreateTestNode(meshId, peerAId, portA, multicastGroup, multicastPort);
        await using var nodeB = CreateTestNode(meshId, peerBId, portB, multicastGroup, multicastPort);

        // Act
        testOutputHelper.WriteLine("Starting P2P infrastructure...");
        await nodeA.StartAsync(cts.Token);
        await nodeB.StartAsync(cts.Token);

        // Assert - Polling loop for UDP multicasts and isolated Kestrel handshakes
        testOutputHelper.WriteLine("Waiting for Phase 1 (UDP) and Phase 2 (Kestrel) to synchronize endpoints...");
        
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
                aDiscoveredB.Endpoint.ShouldBeOfType<KestrelPeerEndpoint>();
                bDiscoveredA.Endpoint.ShouldBeOfType<KestrelPeerEndpoint>();

                var bKestrelEndpoint = (KestrelPeerEndpoint)aDiscoveredB.Endpoint;
                bKestrelEndpoint.Port.ShouldBe(portB);

                discovered = true;
                break;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);
        }

        discovered.ShouldBeTrue("Nodes failed to discover each other via UDP multicast and Kestrel HTTP handshakes within the limit.");
    }

    [IntegrationFact]
    public async Task KestrelPeerHandshaker_ShouldMapEndpointsCorrectly_WhenUsingMultipleMeshes()
    {
        // Arrange
        var mesh1Id = $"kestrel-multi-1-{Guid.NewGuid():N}";
        var mesh2Id = $"kestrel-multi-2-{Guid.NewGuid():N}";
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());
        
        var mesh1PortA = GetNextPort();
        var mesh1PortB = GetNextPort();
        var mesh2PortA = GetNextPort();
        var mesh2PortB = GetNextPort();

        var multicastGroup1 = "239.3.3.3";
        var multicastPort1 = GetNextPort();
        
        var multicastGroup2 = "239.4.4.4";
        var multicastPort2 = GetNextPort();

        testOutputHelper.WriteLine("Initializing Multi-Mesh Nodes using decoupled Kestrel + UDP...");
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
                    var aKestrelEndpoint = (KestrelPeerEndpoint)aDiscoveredB1.Endpoint;
                    var bKestrelEndpoint = (KestrelPeerEndpoint)bDiscoveredA1.Endpoint;
                    
                    aKestrelEndpoint.Port.ShouldBe(mesh1PortB);
                    bKestrelEndpoint.Port.ShouldBe(mesh1PortA);
                    
                    mesh1Discovered = true;
                    testOutputHelper.WriteLine("Mesh 1 (Kestrel/UDP) synchronized appropriately.");
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
                    var aKestrelEndpoint2 = (KestrelPeerEndpoint)aDiscoveredB2.Endpoint;
                    
                    aKestrelEndpoint2.Port.ShouldBe(mesh2PortB);
                    
                    mesh2Discovered = true;
                    testOutputHelper.WriteLine("Mesh 2 (Kestrel/UDP) synchronized smoothly isolating bounds.");
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

    private KestrelTestNode CreateTestNode(string meshId, PeerId peerId, int kestrelPort, string? multicastGroup, int multicastPort)
    {
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });
        
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        // Manually inject the Keyed Endpoint to correctly identify natively inside purely handshaker focused integrations
        services.AddKeyedSingleton<PeerEndpoint>(meshId, new KestrelPeerEndpoint("127.0.0.1", kestrelPort));

        services.Configure<FailureDetectorOptions>(meshId, options => 
        {
            options.HeartbeatInterval = TimeSpan.FromSeconds(120);
        });

        var meshBuilder = services.AddP2pMesh(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        })
        .AddGossipNetwork()
        .AddKestrelPeerHandshake(options =>
        {
            options.ListenHost = "127.0.0.1";
            options.ListenPort = kestrelPort;
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

        return new KestrelTestNode(
            provider,
            peerId,
            provider.GetRequiredKeyedService<IPeerHandshaker>(meshId),
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private MultiMeshKestrelTestNode CreateMultiMeshNode(
        PeerId peerId,
        string mesh1Id,
        string mesh2Id,
        int kestrelPort1,
        int kestrelPort2,
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

        services.AddKeyedSingleton<PeerEndpoint>(mesh1Id, new KestrelPeerEndpoint("127.0.0.1", kestrelPort1));
        services.AddKeyedSingleton<PeerEndpoint>(mesh2Id, new KestrelPeerEndpoint("127.0.0.1", kestrelPort2));

        services.Configure<FailureDetectorOptions>(mesh1Id, options => options.HeartbeatInterval = TimeSpan.FromSeconds(120));
        services.Configure<FailureDetectorOptions>(mesh2Id, options => options.HeartbeatInterval = TimeSpan.FromSeconds(120));

        services.AddP2pMesh(mesh1Id, options => options.LocalPeerId = peerId.Value)
            .AddGossipNetwork()
            .AddKestrelPeerHandshake(options =>
            {
                options.ListenHost = "127.0.0.1";
                options.ListenPort = kestrelPort1;
                options.HandshakeTimeout = TimeSpan.FromSeconds(10);
            })
            .AddUdpPeerDiscovery(options =>
            {
                options.MulticastAddress = multicastGroup1;
                options.MulticastPort = multicastPort1;
                options.DiscoveryInterval = TimeSpan.FromSeconds(2);
            });

        services.AddP2pMesh(mesh2Id, options => options.LocalPeerId = peerId.Value)
            .AddPushPullGossipNetwork()
            .AddKestrelPeerHandshake(options =>
            {
                options.ListenHost = "127.0.0.1";
                options.ListenPort = kestrelPort2;
                options.HandshakeTimeout = TimeSpan.FromSeconds(10);
            })
            .AddUdpPeerDiscovery(options =>
            {
                options.MulticastAddress = multicastGroup2;
                options.MulticastPort = multicastPort2;
                options.DiscoveryInterval = TimeSpan.FromSeconds(2);
            });

        var provider = services.BuildServiceProvider();

        return new MultiMeshKestrelTestNode(
            provider,
            peerId,
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private sealed record KestrelTestNode(
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

    private sealed record MultiMeshKestrelTestNode(
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