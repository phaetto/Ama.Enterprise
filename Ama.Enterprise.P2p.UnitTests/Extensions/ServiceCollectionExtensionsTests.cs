using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models;
using Ama.Enterprise.P2p.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shouldly;

namespace Ama.Enterprise.P2p.UnitTests.Extensions;

public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddP2pGossipNetwork_ShouldRegisterRequiredServices()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

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

        provider.GetRequiredService<IGossipSerializer>().ShouldBeOfType<SystemTextJsonGossipSerializer>();
        provider.GetRequiredService<IPeerRegistry>().ShouldBeOfType<InMemoryPeerRegistry>();
        provider.GetRequiredService<IPeerAuthenticator>().ShouldBeOfType<PassThroughPeerAuthenticator>();
        provider.GetRequiredService<IPeerSelector>().ShouldBeOfType<RandomPeerSelector>();
        provider.GetRequiredService<IFailureDetector>().ShouldBeOfType<TimeBasedFailureDetector>();
        provider.GetRequiredService<ITransport>().ShouldBeOfType<HttpTransport>();
        provider.GetRequiredService<ITransportListener>().ShouldBeOfType<HttpTransportListener>();
        provider.GetRequiredService<IMessageDispatcher>().ShouldBeOfType<MessageDispatcher>();
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