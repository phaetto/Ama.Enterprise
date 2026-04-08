namespace Ama.Enterprise.P2p.UnitTests.Gossip.Services;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

public sealed class P2pHostedServiceTests
{
    private const string TestMeshId = "TestMesh";

    [Fact]
    public async Task StartAsync_ShouldCallP2pProtocolStartAsync_ForRegisteredMeshes()
    {
        // Arrange
        var protocolMock = new Mock<IP2pProtocol>();
        var services = new ServiceCollection();
        services.AddKeyedSingleton(TestMeshId, protocolMock.Object);
        var serviceProvider = services.BuildServiceProvider();

        var meshes = new[] { new P2pMeshMetadata(TestMeshId) };
        var loggerMock = new Mock<ILogger<P2pHostedService>>();
        
        var service = new P2pHostedService(serviceProvider, meshes, loggerMock.Object);
        var token = new CancellationTokenSource().Token;

        // Act
        await service.StartAsync(token);

        // Assert
        protocolMock.Verify(p => p.StartAsync(token), Times.Once);
    }

    [Fact]
    public async Task StopAsync_ShouldCallP2pProtocolStopAsync_ForRegisteredMeshes()
    {
        // Arrange
        var protocolMock = new Mock<IP2pProtocol>();
        var services = new ServiceCollection();
        services.AddKeyedSingleton(TestMeshId, protocolMock.Object);
        var serviceProvider = services.BuildServiceProvider();

        var meshes = new[] { new P2pMeshMetadata(TestMeshId) };
        var loggerMock = new Mock<ILogger<P2pHostedService>>();
        
        var service = new P2pHostedService(serviceProvider, meshes, loggerMock.Object);
        var token = new CancellationTokenSource().Token;

        // Act
        await service.StopAsync(token);

        // Assert
        protocolMock.Verify(p => p.StopAsync(token), Times.Once);
    }
}