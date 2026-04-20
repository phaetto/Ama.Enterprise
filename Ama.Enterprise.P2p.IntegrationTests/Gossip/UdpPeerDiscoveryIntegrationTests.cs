namespace Ama.Enterprise.P2p.IntegrationTests.Gossip;

using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Contains integration tests focusing on the UDP multicast discovery mechanism.
/// </summary>
public sealed class UdpPeerDiscoveryIntegrationTests : IDisposable
{
    private readonly ITestOutputHelper testOutputHelper;
    private readonly IList<ServiceProvider> serviceProviders = new List<ServiceProvider>();

    public UdpPeerDiscoveryIntegrationTests(ITestOutputHelper testOutputHelper)
    {
        this.testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    }

    [IntegrationFact]
    public async Task DiscoverPeersAsync_ShouldFindOtherNodes_WhenTheyAreListening()
    {
        using var cancellationSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var nodeId1 = Guid.NewGuid();
        var nodeId2 = Guid.NewGuid();
        var nodeId3 = Guid.NewGuid();

        var port1 = 8301;
        var port2 = 8302;
        var port3 = 8303;

        // Use a distinct multicast port specifically for this test to avoid local execution collisions
        var multicastPort = 8035; 

        var node1 = CreateDiscoveryNode(nodeId1, port1, multicastPort);
        var node2 = CreateDiscoveryNode(nodeId2, port2, multicastPort);
        var node3 = CreateDiscoveryNode(nodeId3, port3, multicastPort);

        // Start all nodes so their UDP background listeners bind and become active
        // By calling StartAsync on the P2pHostedService orchestrator, it cascades StartAsync to the tied UdpPeerDiscovery instance
        await node1.HostedService.StartAsync(cancellationSource.Token);
        await node2.HostedService.StartAsync(cancellationSource.Token);
        await node3.HostedService.StartAsync(cancellationSource.Token);

        // Provide a short delay for sockets to fully bind on the OS level
        await Task.Delay(500, cancellationSource.Token);

        // Act - Node 1 requests a network discovery
        var discoveredPeers = await node1.Discovery.DiscoverPeersAsync(cancellationSource.Token);

        // Assert
        var peersList = discoveredPeers.ToList();
        peersList.Count.ShouldBeGreaterThanOrEqualTo(2);
        
        peersList.Any(p => p.Id.Value == nodeId2).ShouldBeTrue();
        peersList.Any(p => p.Id.Value == nodeId3).ShouldBeTrue();
        
        // Assert that a node does not discover itself in the returned results
        peersList.Any(p => p.Id.Value == nodeId1).ShouldBeFalse();

        // Cleanup
        await node1.HostedService.StopAsync(cancellationSource.Token);
        await node2.HostedService.StopAsync(cancellationSource.Token);
        await node3.HostedService.StopAsync(cancellationSource.Token);
    }

    [IntegrationFact]
    public async Task DiscoverPeersAsync_ShouldMapEndpointsCorrectly_WhenUsingMultipleMeshes()
    {
        using var cancellationSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var nodeId1 = Guid.NewGuid();
        var nodeId2 = Guid.NewGuid();
        var nodeId3 = Guid.NewGuid();

        var node1Mesh1Port = 8401;
        var node1Mesh2Port = 8501;
        
        var node2Mesh1Port = 8402;
        var node2Mesh2Port = 8502;
        
        var node3Mesh1Port = 8403;
        var node3Mesh2Port = 8503;

        var multicastPort1 = 8036; 
        var multicastPort2 = 8037; 

        var node1 = CreateMultiMeshNode(nodeId1, node1Mesh1Port, node1Mesh2Port, multicastPort1, multicastPort2);
        var node2 = CreateMultiMeshNode(nodeId2, node2Mesh1Port, node2Mesh2Port, multicastPort1, multicastPort2);
        var node3 = CreateMultiMeshNode(nodeId3, node3Mesh1Port, node3Mesh2Port, multicastPort1, multicastPort2);

        await node1.HostedService.StartAsync(cancellationSource.Token);
        await node2.HostedService.StartAsync(cancellationSource.Token);
        await node3.HostedService.StartAsync(cancellationSource.Token);

        await Task.Delay(500, cancellationSource.Token);

        var discoveryMesh1 = node1.Provider.GetRequiredKeyedService<IPeerDiscovery>("Mesh1");
        var discoveredPeersMesh1 = await discoveryMesh1.DiscoverPeersAsync(cancellationSource.Token);

        var peersList1 = discoveredPeersMesh1.ToList();
        peersList1.Count.ShouldBeGreaterThanOrEqualTo(2);
        
        var peer2Mesh1 = peersList1.FirstOrDefault(p => p.Id.Value == nodeId2);
        peer2Mesh1.ShouldNotBe(default);
        peer2Mesh1.Endpoint.ShouldBeOfType<HttpPeerEndpoint>().Port.ShouldBe(node2Mesh1Port);

        var peer3Mesh1 = peersList1.FirstOrDefault(p => p.Id.Value == nodeId3);
        peer3Mesh1.ShouldNotBe(default);
        peer3Mesh1.Endpoint.ShouldBeOfType<HttpPeerEndpoint>().Port.ShouldBe(node3Mesh1Port);

        var discoveryMesh2 = node1.Provider.GetRequiredKeyedService<IPeerDiscovery>("Mesh2");
        var discoveredPeersMesh2 = await discoveryMesh2.DiscoverPeersAsync(cancellationSource.Token);

        var peersList2 = discoveredPeersMesh2.ToList();
        peersList2.Count.ShouldBeGreaterThanOrEqualTo(2);

        var peer2Mesh2 = peersList2.FirstOrDefault(p => p.Id.Value == nodeId2);
        peer2Mesh2.ShouldNotBe(default);
        peer2Mesh2.Endpoint.ShouldBeOfType<HttpPeerEndpoint>().Port.ShouldBe(node2Mesh2Port);

        var peer3Mesh2 = peersList2.FirstOrDefault(p => p.Id.Value == nodeId3);
        peer3Mesh2.ShouldNotBe(default);
        peer3Mesh2.Endpoint.ShouldBeOfType<HttpPeerEndpoint>().Port.ShouldBe(node3Mesh2Port);

        await node1.HostedService.StopAsync(cancellationSource.Token);
        await node2.HostedService.StopAsync(cancellationSource.Token);
        await node3.HostedService.StopAsync(cancellationSource.Token);
    }

    public void Dispose()
    {
        foreach (var provider in serviceProviders)
        {
            provider.Dispose();
        }
        
        serviceProviders.Clear();
    }

    private SingleMeshTestNode CreateDiscoveryNode(Guid peerId, int listenPort, int multicastPort)
    {
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        var meshId = "UdpTestMesh";

        services.AddP2pMesh(meshId, nodeOptions => 
            {
                nodeOptions.LocalPeerId = peerId;
            })
            .AddGossipNetwork()
            .AddHttpTransport(opts => 
            { 
                opts.ListenPort = listenPort; 
                opts.ListenHost = "localhost"; 
                opts.PathPrefix = "/p2p/gossip/";
            })
            .AddUdpPeerDiscovery(options => 
            {
                options.MulticastAddress = "239.255.0.1"; 
                options.MulticastPort = multicastPort;
                options.DiscoveryTimeout = TimeSpan.FromSeconds(5);
            });

        services.AddKeyedSingleton<IFailureDetector>(meshId, (sp, key) => new TimeBasedFailureDetector((string)key!, sp.GetRequiredService<IOptionsMonitor<FailureDetectorOptions>>(), sp.GetRequiredService<ILogger<TimeBasedFailureDetector>>()));
        services.AddKeyedSingleton<IPeerAuthenticator>(meshId, (sp, key) => new PassThroughPeerAuthenticator((string)key!, sp.GetRequiredService<ILogger<PassThroughPeerAuthenticator>>()));

        var provider = services.BuildServiceProvider();
        serviceProviders.Add(provider);

        var discovery = provider.GetRequiredKeyedService<IPeerDiscovery>(meshId);
        var hostedService = provider.GetServices<IHostedService>().OfType<P2pHostedService>().First();

        return new SingleMeshTestNode(discovery, hostedService);
    }

    private MultiMeshTestNode CreateMultiMeshNode(Guid peerId, int mesh1Port, int mesh2Port, int multicastPort1, int multicastPort2)
    {
        var services = new ServiceCollection();
        
        services.AddCrdt();
        
        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        var mesh1Id = "Mesh1";
        var mesh2Id = "Mesh2";

        services.AddP2pMesh(mesh1Id, nodeOptions => 
            {
                nodeOptions.LocalPeerId = peerId;
            })
            .AddGossipNetwork()
            .AddHttpTransport(opts => 
            { 
                opts.ListenPort = mesh1Port; 
                opts.ListenHost = "localhost";
                opts.PathPrefix = "/p2p/mesh1/";
            })
            .AddUdpPeerDiscovery(options => 
            {
                options.MulticastAddress = "239.255.0.1"; 
                options.MulticastPort = multicastPort1;
                options.DiscoveryTimeout = TimeSpan.FromSeconds(5);
            });

        services.AddKeyedSingleton<IFailureDetector>(mesh1Id, (sp, key) => new TimeBasedFailureDetector((string)key!, sp.GetRequiredService<IOptionsMonitor<FailureDetectorOptions>>(), sp.GetRequiredService<ILogger<TimeBasedFailureDetector>>()));
        services.AddKeyedSingleton<IPeerAuthenticator>(mesh1Id, (sp, key) => new PassThroughPeerAuthenticator((string)key!, sp.GetRequiredService<ILogger<PassThroughPeerAuthenticator>>()));

        services.AddP2pMesh(mesh2Id, nodeOptions => 
            {
                nodeOptions.LocalPeerId = peerId;
            })
            .AddPushPullGossipNetwork()
            .AddHttpTransport(opts => 
            { 
                opts.ListenPort = mesh2Port; 
                opts.ListenHost = "localhost";
                opts.PathPrefix = "/p2p/mesh2/";
            })
            .AddUdpPeerDiscovery(options => 
            {
                options.MulticastAddress = "239.255.0.1"; 
                options.MulticastPort = multicastPort2;
                options.DiscoveryTimeout = TimeSpan.FromSeconds(5);
            });

        services.AddKeyedSingleton<IFailureDetector>(mesh2Id, (sp, key) => new TimeBasedFailureDetector((string)key!, sp.GetRequiredService<IOptionsMonitor<FailureDetectorOptions>>(), sp.GetRequiredService<ILogger<TimeBasedFailureDetector>>()));
        services.AddKeyedSingleton<IPeerAuthenticator>(mesh2Id, (sp, key) => new PassThroughPeerAuthenticator((string)key!, sp.GetRequiredService<ILogger<PassThroughPeerAuthenticator>>()));

        var provider = services.BuildServiceProvider();
        serviceProviders.Add(provider);

        var hostedService = provider.GetServices<IHostedService>().OfType<P2pHostedService>().First();

        return new MultiMeshTestNode(provider, hostedService);
    }

    private readonly record struct SingleMeshTestNode(IPeerDiscovery Discovery, IHostedService HostedService);

    private readonly record struct MultiMeshTestNode(ServiceProvider Provider, IHostedService HostedService);
}