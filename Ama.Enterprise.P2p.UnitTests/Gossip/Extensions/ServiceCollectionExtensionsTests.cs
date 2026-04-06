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

public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddP2pGossipNetwork_ShouldRegisterRequiredServices()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddCrdt();

        // Act
        services.AddP2pGossipNetwork(options =>
        {
            options.Fanout = 5;
            options.ListenPort = 12345;
        });

        var provider = services.BuildServiceProvider();

        // Assert
        var options = provider.GetRequiredService<IOptions<GossipOptions>>();
        options.Value.Fanout.ShouldBe(5);
        options.Value.ListenPort.ShouldBe(12345);

        provider.GetRequiredService<IPeerRegistry>().ShouldBeOfType<InMemoryPeerRegistry>();
        provider.GetRequiredService<IPeerAuthenticator>().ShouldBeOfType<PassThroughPeerAuthenticator>();
        provider.GetRequiredService<IPeerSelector>().ShouldBeOfType<RandomPeerSelector>();
        provider.GetRequiredService<IFailureDetector>().ShouldBeOfType<TimeBasedFailureDetector>();
        provider.GetRequiredService<ITransport<GossipMessage>>().ShouldBeOfType<HttpTransport>();
        provider.GetRequiredService<ITransportListener<GossipMessage>>().ShouldBeOfType<HttpTransportListener>();
        provider.GetRequiredService<IMessageDispatcher<GossipMessage>>().ShouldBeOfType<MessageDispatcher<GossipMessage>>();
        provider.GetRequiredService<IGossipProtocol>().ShouldBeOfType<GossipProtocol>();

        var hostedServices = provider.GetServices<IHostedService>();
        hostedServices.ShouldContain(s => s is P2pHostedService);
    }

    [Fact]
    public void AddP2pGossipNetwork_WithNullServices_ShouldThrowArgumentNullException()
    {
        // Arrange
        IServiceCollection? services = null;

        // Act & Assert
        Should.Throw<ArgumentNullException>(() => services!.AddP2pGossipNetwork());
    }
}