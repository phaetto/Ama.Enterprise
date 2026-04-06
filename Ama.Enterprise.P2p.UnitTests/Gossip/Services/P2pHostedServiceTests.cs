namespace Ama.Enterprise.P2p.UnitTests.Gossip.Services;

using Ama.Enterprise.P2p.Services.Gossip;
using Moq;

public sealed class P2pHostedServiceTests
{
    [Fact]
    public async Task StartAsync_ShouldCallGossipProtocolStartAsync()
    {
        // Arrange
        var protocolMock = new Mock<IGossipProtocol>();
        var service = new P2pHostedService(protocolMock.Object);
        var token = new CancellationTokenSource().Token;

        // Act
        await service.StartAsync(token);

        // Assert
        protocolMock.Verify(p => p.StartAsync(token), Times.Once);
    }

    [Fact]
    public async Task StopAsync_ShouldCallGossipProtocolStopAsync()
    {
        // Arrange
        var protocolMock = new Mock<IGossipProtocol>();
        var service = new P2pHostedService(protocolMock.Object);
        var token = new CancellationTokenSource().Token;

        // Act
        await service.StopAsync(token);

        // Assert
        protocolMock.Verify(p => p.StopAsync(token), Times.Once);
    }
}