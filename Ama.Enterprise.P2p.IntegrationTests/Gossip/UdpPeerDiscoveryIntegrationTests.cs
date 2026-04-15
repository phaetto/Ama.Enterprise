namespace Ama.Enterprise.P2p.IntegrationTests.Gossip;

using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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

        // Graceful Cleanup
        await node1.HostedService.StopAsync(cancellationSource.Token);
        await node2.HostedService.StopAsync(cancellationSource.Token);
        await node3.HostedService.StopAsync(cancellationSource.Token);
    }

    private (IPeerDiscovery Discovery, IHostedService HostedService) CreateDiscoveryNode(Guid peerId, int listenPort, int multicastPort)
    {
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        var meshId = "UdpTestMesh";

        // Add both Gossip Network (which registers core dependencies like IPeerRegistry for the mesh)
        // and Udp Peer Discovery config paired inside the same builder.
        services.AddP2pMesh(meshId, nodeOptions => 
            {
                nodeOptions.LocalPeerId = peerId;
            })
            .AddGossipNetwork()
            .AddHttpTransport(opts => 
            { 
                opts.ListenPort = listenPort; 
                opts.ListenHost = "localhost"; // Override from '+' to 'localhost' to avoid Access Denied under unprivileged execution on Windows
                opts.PathPrefix = "/p2p/gossip/";
            })
            .AddUdpPeerDiscovery(options => 
            {
                options.MulticastAddress = "239.255.0.1"; 
                options.MulticastPort = multicastPort;
                options.DiscoveryTimeout = TimeSpan.FromSeconds(5);
            });

        var provider = services.BuildServiceProvider();
        serviceProviders.Add(provider);

        var discovery = provider.GetRequiredKeyedService<IPeerDiscovery>(meshId);
        
        // Retrieve the centralized P2P hosted service that orchestrates all configured meshes
        var hostedService = provider.GetServices<IHostedService>().OfType<P2pHostedService>().First();

        return (discovery, hostedService);
    }

    public void Dispose()
    {
        foreach (var provider in serviceProviders)
        {
            provider.Dispose();
        }
        
        serviceProviders.Clear();
    }
}