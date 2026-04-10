namespace Ama.Enterprise.P2p.UnitTests.Gossip.Extensions;

using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Gossip;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shouldly;
using Xunit;
using System;
using Ama.Enterprise.P2p.Services.Transports;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Models.Transports;

public sealed class ServiceCollectionExtensionsTests
{
    private const string TestMeshId = "TestMesh";

    [Fact]
    public void AddP2pMesh_ShouldRegisterRequiredKeyedServices()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddCrdt();

        // Act
        services.AddP2pMesh(TestMeshId)
            .AddGossipNetwork(options =>
            {
                options.Fanout = 5;
            })
            .AddHttpTransport<GossipMessage>(options =>
            {
                options.ListenPort = 12345;
            });

        var provider = services.BuildServiceProvider();

        // Assert
        var gossipOptionsMonitor = provider.GetRequiredService<IOptionsMonitor<GossipOptions>>();
        var gossipOptions = gossipOptionsMonitor.Get(TestMeshId);
        
        gossipOptions.Fanout.ShouldBe(5);

        var httpOptionsMonitor = provider.GetRequiredService<IOptionsMonitor<HttpTransportOptions>>();
        var httpOptions = httpOptionsMonitor.Get(TestMeshId);

        httpOptions.ListenPort.ShouldBe(12345);

        // Core singletons un-keyed
        provider.GetRequiredService<IPeerRegistry>().ShouldBeOfType<InMemoryPeerRegistry>();
        provider.GetRequiredService<IP2pProtocol>().ShouldBeOfType<GossipProtocol>();

        // Mesh specific keyed
        provider.GetRequiredKeyedService<IPeerAuthenticator>(TestMeshId).ShouldBeOfType<PassThroughPeerAuthenticator>();
        provider.GetRequiredKeyedService<IPeerSelector>(TestMeshId).ShouldBeOfType<RandomPeerSelector>();
        provider.GetRequiredKeyedService<IFailureDetector>(TestMeshId).ShouldBeOfType<TimeBasedFailureDetector>();
        provider.GetRequiredKeyedService<ITransport<GossipMessage>>(TestMeshId).ShouldBeOfType<HttpTransport<GossipMessage>>();
        provider.GetRequiredKeyedService<ITransportListener<GossipMessage>>(TestMeshId).ShouldBeOfType<HttpTransportListener<GossipMessage>>();
        provider.GetRequiredKeyedService<IMessageDispatcher<GossipMessage>>(TestMeshId).ShouldBeOfType<MessageDispatcher<GossipMessage>>();

        var hostedServices = provider.GetServices<IHostedService>();
        hostedServices.ShouldContain(s => s is P2pHostedService);
    }

    [Fact]
    public void AddP2pMesh_WithNullOrEmptyMeshId_ShouldThrowArgumentException()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act & Assert
        Should.Throw<ArgumentException>(() => services.AddP2pMesh(""));
        Should.Throw<ArgumentException>(() => services.AddP2pMesh(null!));
    }
}