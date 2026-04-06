namespace Ama.Enterprise.P2p.UnitTests.Gossip.Services;

using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;

public sealed class RandomPeerSelectorTests
{
    private readonly Mock<IPeerRegistry> registryMock;
    private readonly RandomPeerSelector selector;

    public RandomPeerSelectorTests()
    {
        registryMock = new Mock<IPeerRegistry>();
        var loggerMock = new Mock<ILogger<RandomPeerSelector>>();
        selector = new RandomPeerSelector(registryMock.Object, loggerMock.Object);
    }

    [Fact]
    public async Task GetPeersAsync_WhenFanoutIsZero_ShouldThrowArgumentOutOfRangeException()
    {
        // Act & Assert
        await Should.ThrowAsync<ArgumentOutOfRangeException>(async () =>
            await selector.GetPeersAsync(0, CancellationToken.None));
    }

    [Fact]
    public async Task GetPeersAsync_WithNoActivePeers_ShouldReturnEmpty()
    {
        // Arrange
        registryMock.Setup(r => r.GetPeersByStatusAsync(PeerStatus.Active, It.IsAny<CancellationToken>()))
            .ReturnsAsync([]);

        // Act
        var result = await selector.GetPeersAsync(3, CancellationToken.None);

        // Assert
        result.ShouldBeEmpty();
    }

    [Fact]
    public async Task GetPeersAsync_WithActivePeers_ShouldReturnRequestedFanoutCount()
    {
        // Arrange
        var activePeers = Enumerable.Range(1, 5)
            .Select(_ => new PeerNode(new PeerId(Guid.NewGuid()), new PeerEndpoint("localhost", 8080)))
            .ToList();

        registryMock.Setup(r => r.GetPeersByStatusAsync(PeerStatus.Active, It.IsAny<CancellationToken>()))
            .ReturnsAsync(activePeers);

        // Act
        var result = (await selector.GetPeersAsync(3, CancellationToken.None)).ToList();

        // Assert
        result.Count.ShouldBe(3);
        result.ShouldAllBe(p => activePeers.Contains(p));
    }

    [Fact]
    public async Task GetPeersAsync_WithFewerActivePeersThanFanout_ShouldReturnAllAvailable()
    {
        // Arrange
        var activePeers = Enumerable.Range(1, 2)
            .Select(_ => new PeerNode(new PeerId(Guid.NewGuid()), new PeerEndpoint("localhost", 8080)))
            .ToList();

        registryMock.Setup(r => r.GetPeersByStatusAsync(PeerStatus.Active, It.IsAny<CancellationToken>()))
            .ReturnsAsync(activePeers);

        // Act
        var result = (await selector.GetPeersAsync(5, CancellationToken.None)).ToList();

        // Assert
        result.Count.ShouldBe(2);
        result.ShouldAllBe(p => activePeers.Contains(p));
    }
}