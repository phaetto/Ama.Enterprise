namespace Ama.Enterprise.P2p.UnitTests.Gossip.Services;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Gossip;
using Moq;
using Xunit;

public sealed class P2pHostedServiceTests
{
    [Fact]
    public async Task StartAsync_ShouldCallP2pProtocolStartAsync()
    {
        // Arrange
        var protocolMock = new Mock<IP2pProtocol>();
        var service = new P2pHostedService(protocolMock.Object);
        var token = new CancellationTokenSource().Token;

        // Act
        await service.StartAsync(token);

        // Assert
        protocolMock.Verify(p => p.StartAsync(token), Times.Once);
    }

    [Fact]
    public async Task StopAsync_ShouldCallP2pProtocolStopAsync()
    {
        // Arrange
        var protocolMock = new Mock<IP2pProtocol>();
        var service = new P2pHostedService(protocolMock.Object);
        var token = new CancellationTokenSource().Token;

        // Act
        await service.StopAsync(token);

        // Assert
        protocolMock.Verify(p => p.StopAsync(token), Times.Once);
    }
}