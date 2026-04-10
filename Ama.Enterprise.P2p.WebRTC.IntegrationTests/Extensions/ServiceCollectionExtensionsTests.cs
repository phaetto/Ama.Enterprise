namespace Ama.Enterprise.P2p.WebRTC.IntegrationTests.Extensions;

using System;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.Extensions;
using Ama.Enterprise.P2p.WebRTC.Services;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Shouldly;
using Xunit;

public sealed class ServiceCollectionExtensionsTests
{
    public readonly record struct DummyMessage(string Data);

    [Fact]
    public void AddWebRtcTransport_ShouldRegisterAllRequiredKeyedServices()
    {
        // Arrange
        var services = new ServiceCollection();
        var meshId = "test-mesh";
        
        // Register cross-module dependencies natively expected by the builder
        services.AddSingleton(Mock.Of<ICrdtSerializer>());
        services.AddSingleton(Mock.Of<IPeerRegistry>());
        services.AddLogging();
        services.Configure<P2pNodeOptions>(meshId, o => o.LocalPeerId = Guid.NewGuid());
        
        var meshBuilderMock = new Mock<IP2pMeshBuilder>();
        meshBuilderMock.Setup(b => b.Services).Returns(services);
        meshBuilderMock.Setup(b => b.MeshId).Returns(meshId);

        // Act
        meshBuilderMock.Object.AddWebRtcTransport<DummyMessage>(options =>
        {
            options.IceGatheringTimeout = TimeSpan.FromSeconds(5);
        });

        using var serviceProvider = services.BuildServiceProvider();

        // Assert
        var connectionManager = serviceProvider.GetKeyedService<WebRtcConnectionManager>(meshId);
        connectionManager.ShouldNotBeNull();

        var iConnectionManager = serviceProvider.GetKeyedService<IWebRtcConnectionManager>(meshId);
        iConnectionManager.ShouldNotBeNull();
        iConnectionManager.ShouldBeSameAs(connectionManager);

        var iInvitationService = serviceProvider.GetKeyedService<IWebRtcInvitationService>(meshId);
        iInvitationService.ShouldNotBeNull();
        iInvitationService.ShouldBeSameAs(connectionManager);

        var transport = serviceProvider.GetKeyedService<ITransport<DummyMessage>>(meshId);
        transport.ShouldNotBeNull();
        transport.ShouldBeOfType<WebRtcTransport<DummyMessage>>();

        var listener = serviceProvider.GetKeyedService<ITransportListener<DummyMessage>>(meshId);
        listener.ShouldNotBeNull();
        listener.ShouldBeOfType<WebRtcTransportListener<DummyMessage>>();
    }
}