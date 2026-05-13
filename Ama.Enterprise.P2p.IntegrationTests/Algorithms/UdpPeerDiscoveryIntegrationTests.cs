namespace Ama.Enterprise.P2p.IntegrationTests.Algorithms;

using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
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
/// Contains integration tests focusing on the Two-Phase UDP multicast discovery mechanism.
/// </summary>
public sealed class UdpPeerDiscoveryIntegrationTests(ITestOutputHelper testOutputHelper) : IDisposable
{
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    private readonly IList<ServiceProvider> serviceProviders = new List<ServiceProvider>();

    private static int udpPortCounter = 13000;
    private static int GetNextUdpPort() => Interlocked.Increment(ref udpPortCounter);

    [IntegrationFact]
    public async Task DiscoverPeersAsync_ShouldFindOtherNodes_WhenTheyAreListening()
    {
        using var cancellationSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var nodeId1 = Guid.NewGuid();
        var nodeId2 = Guid.NewGuid();

        var port1 = 8301;
        var port2 = 8302;

        var multicastPort = 8035;
        
        var handshakeListen1 = 8601;
        var handshakeListen2 = 8602;

        var node1 = CreateDiscoveryNode(nodeId1, port1, multicastPort, handshakeListen1);
        var node2 = CreateDiscoveryNode(nodeId2, port2, multicastPort, handshakeListen2);

        await StartAllHostedServicesAsync(node1.HostedServices, cancellationSource.Token);
        await StartAllHostedServicesAsync(node2.HostedServices, cancellationSource.Token);

        await Task.Delay(500, cancellationSource.Token);

        var discoveredPeers = await node1.Discovery.DiscoverPeersAsync(cancellationSource.Token);

        var peersList = discoveredPeers.ToList();
        peersList.Count.ShouldBeGreaterThanOrEqualTo(1);
        
        peersList.Any(p => p.Id.Value == nodeId2).ShouldBeTrue();
        peersList.Any(p => p.Id.Value == nodeId1).ShouldBeFalse();

        await StopAllHostedServicesAsync(node1.HostedServices, cancellationSource.Token);
        await StopAllHostedServicesAsync(node2.HostedServices, cancellationSource.Token);
    }

    [IntegrationFact]
    public async Task DiscoverPeersAsync_ShouldMapEndpointsCorrectly_WhenUsingMultipleMeshes()
    {
        using var cancellationSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var nodeId1 = Guid.NewGuid();
        var nodeId2 = Guid.NewGuid();

        var node1Mesh1Port = 8401;
        var node1Mesh2Port = 8501;
        
        var node2Mesh1Port = 8402;
        var node2Mesh2Port = 8502;

        var multicastPort1 = 8036; 
        var multicastPort2 = 8037; 

        var node1Mesh1Handshake = 8701;
        var node1Mesh2Handshake = 8801;
        
        var node2Mesh1Handshake = 8702;
        var node2Mesh2Handshake = 8802;

        var node1 = CreateMultiMeshNode(nodeId1, node1Mesh1Port, node1Mesh2Port, multicastPort1, multicastPort2, node1Mesh1Handshake, node1Mesh2Handshake);
        var node2 = CreateMultiMeshNode(nodeId2, node2Mesh1Port, node2Mesh2Port, multicastPort1, multicastPort2, node2Mesh1Handshake, node2Mesh2Handshake);

        await StartAllHostedServicesAsync(node1.HostedServices, cancellationSource.Token);
        await StartAllHostedServicesAsync(node2.HostedServices, cancellationSource.Token);

        await Task.Delay(500, cancellationSource.Token);

        var discoveryMesh1 = node1.Provider.GetRequiredKeyedService<IPeerDiscovery>("Mesh1");
        var discoveredPeersMesh1 = await discoveryMesh1.DiscoverPeersAsync(cancellationSource.Token);

        var peersList1 = discoveredPeersMesh1.ToList();
        peersList1.Count.ShouldBeGreaterThanOrEqualTo(1);
        
        var peer2Mesh1 = peersList1.FirstOrDefault(p => p.Id.Value == nodeId2);
        peer2Mesh1.ShouldNotBe(default);
        peer2Mesh1.Endpoint.ShouldBeOfType<TcpPeerEndpoint>().Port.ShouldBe(node2Mesh1Port);

        var discoveryMesh2 = node1.Provider.GetRequiredKeyedService<IPeerDiscovery>("Mesh2");
        var discoveredPeersMesh2 = await discoveryMesh2.DiscoverPeersAsync(cancellationSource.Token);

        var peersList2 = discoveredPeersMesh2.ToList();
        peersList2.Count.ShouldBeGreaterThanOrEqualTo(1);

        var peer2Mesh2 = peersList2.FirstOrDefault(p => p.Id.Value == nodeId2);
        peer2Mesh2.ShouldNotBe(default);
        peer2Mesh2.Endpoint.ShouldBeOfType<TcpPeerEndpoint>().Port.ShouldBe(node2Mesh2Port);

        await StopAllHostedServicesAsync(node1.HostedServices, cancellationSource.Token);
        await StopAllHostedServicesAsync(node2.HostedServices, cancellationSource.Token);
    }

    [IntegrationFact]
    public async Task DiscoverPeersAsync_FiveNodes_ShouldDiscoverWithoutErrors()
    {
        using var cancellationSource = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        var nodeIds = Enumerable.Range(0, 5).Select(_ => Guid.NewGuid()).ToList();
        var ports = Enumerable.Range(0, 5).Select(_ => GetNextUdpPort()).ToList();
        var handshakeListenPorts = Enumerable.Range(0, 5).Select(_ => GetNextUdpPort()).ToList();

        var multicastPort = GetNextUdpPort();
        
        var nodes = new List<SingleMeshTestNode>();

        for (int i = 0; i < 5; i++)
        {
            nodes.Add(CreateDiscoveryNode(nodeIds[i], ports[i], multicastPort, handshakeListenPorts[i]));
        }

        var startTasks = nodes.Select(n => StartAllHostedServicesAsync(n.HostedServices, cancellationSource.Token));
        await Task.WhenAll(startTasks);

        await Task.Delay(2000, cancellationSource.Token);

        var discoveryTasks = nodes.Select(n => n.Discovery.DiscoverPeersAsync(cancellationSource.Token));
        var results = await Task.WhenAll(discoveryTasks);

        for (int i = 0; i < 5; i++)
        {
            var peersList = results[i].ToList();
            peersList.ShouldNotBeEmpty();
        }

        var stopTasks = nodes.Select(n => StopAllHostedServicesAsync(n.HostedServices, cancellationSource.Token));
        await Task.WhenAll(stopTasks);
    }

    public void Dispose()
    {
        foreach (var provider in serviceProviders)
        {
            provider.Dispose();
        }
        
        serviceProviders.Clear();
    }

    private SingleMeshTestNode CreateDiscoveryNode(Guid peerId, int listenPort, int multicastPort, int handshakeListenPort)
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
            .AddTcpTransport(opts => 
            { 
                opts.ListenPort = listenPort; 
                opts.ListenHost = "127.0.0.1"; 
            })
            .AddUdpPeerDiscovery(options => 
            {
                options.MulticastAddress = "239.255.0.1"; 
                options.MulticastPort = multicastPort;
                options.DiscoveryTimeout = TimeSpan.FromSeconds(5);
            })
            .AddUdpPeerHandshake(options => 
            {
                options.ListenPort = handshakeListenPort;
                options.HandshakeTimeout = TimeSpan.FromSeconds(5);
            });

        services.AddKeyedSingleton<IFailureDetector>(meshId, (sp, key) => new TimeBasedFailureDetector((string)key!, sp.GetRequiredService<IOptionsMonitor<FailureDetectorOptions>>(), sp.GetRequiredService<ILogger<TimeBasedFailureDetector>>()));
        services.AddKeyedSingleton<IPeerAuthenticator>(meshId, (sp, key) => new PassThroughPeerAuthenticator((string)key!, sp.GetRequiredService<ILogger<PassThroughPeerAuthenticator>>()));

        var provider = services.BuildServiceProvider();
        serviceProviders.Add(provider);

        var discovery = provider.GetRequiredKeyedService<IPeerDiscovery>(meshId);
        var hostedServices = provider.GetServices<IHostedService>().ToList();

        return new SingleMeshTestNode(discovery, hostedServices);
    }

    private MultiMeshTestNode CreateMultiMeshNode(Guid peerId, int mesh1Port, int mesh2Port, int multicastPort1, int multicastPort2, int mesh1HandshakeListenPort, int mesh2HandshakeListenPort)
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
            .AddTcpTransport(opts => 
            { 
                opts.ListenPort = mesh1Port; 
                opts.ListenHost = "127.0.0.1";
            })
            .AddUdpPeerDiscovery(options => 
            {
                options.MulticastAddress = "239.255.0.1"; 
                options.MulticastPort = multicastPort1;
                options.DiscoveryTimeout = TimeSpan.FromSeconds(5);
            })
            .AddUdpPeerHandshake(options =>
            {
                options.ListenPort = mesh1HandshakeListenPort;
                options.HandshakeTimeout = TimeSpan.FromSeconds(5);
            });

        services.AddKeyedSingleton<IFailureDetector>(mesh1Id, (sp, key) => new TimeBasedFailureDetector((string)key!, sp.GetRequiredService<IOptionsMonitor<FailureDetectorOptions>>(), sp.GetRequiredService<ILogger<TimeBasedFailureDetector>>()));
        services.AddKeyedSingleton<IPeerAuthenticator>(mesh1Id, (sp, key) => new PassThroughPeerAuthenticator((string)key!, sp.GetRequiredService<ILogger<PassThroughPeerAuthenticator>>()));

        services.AddP2pMesh(mesh2Id, nodeOptions => 
            {
                nodeOptions.LocalPeerId = peerId;
            })
            .AddTcpTransport(opts => 
            { 
                opts.ListenPort = mesh2Port; 
                opts.ListenHost = "127.0.0.1";
            })
            .AddUdpPeerDiscovery(options => 
            {
                options.MulticastAddress = "239.255.0.1"; 
                options.MulticastPort = multicastPort2;
                options.DiscoveryTimeout = TimeSpan.FromSeconds(5);
            })
            .AddUdpPeerHandshake(options =>
            {
                options.ListenPort = mesh2HandshakeListenPort;
                options.HandshakeTimeout = TimeSpan.FromSeconds(5);
            });

        services.AddKeyedSingleton<IFailureDetector>(mesh2Id, (sp, key) => new TimeBasedFailureDetector((string)key!, sp.GetRequiredService<IOptionsMonitor<FailureDetectorOptions>>(), sp.GetRequiredService<ILogger<TimeBasedFailureDetector>>()));
        services.AddKeyedSingleton<IPeerAuthenticator>(mesh2Id, (sp, key) => new PassThroughPeerAuthenticator((string)key!, sp.GetRequiredService<ILogger<PassThroughPeerAuthenticator>>()));

        var provider = services.BuildServiceProvider();
        serviceProviders.Add(provider);

        var hostedServices = provider.GetServices<IHostedService>().ToList();

        return new MultiMeshTestNode(provider, hostedServices);
    }
    
    private static async Task StartAllHostedServicesAsync(IEnumerable<IHostedService> services, CancellationToken cancellationToken)
    {
        foreach (var service in services)
        {
            await service.StartAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task StopAllHostedServicesAsync(IEnumerable<IHostedService> services, CancellationToken cancellationToken)
    {
        foreach (var service in services.Reverse())
        {
            await service.StopAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private readonly record struct SingleMeshTestNode(IPeerDiscovery Discovery, IEnumerable<IHostedService> HostedServices);

    private readonly record struct MultiMeshTestNode(ServiceProvider Provider, IEnumerable<IHostedService> HostedServices);
}