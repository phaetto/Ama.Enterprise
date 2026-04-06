using Ama.Enterprise.P2p.Models;
using Ama.Enterprise.P2p.Services;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;
using Xunit;

namespace Ama.Enterprise.P2p.UnitTests.Services;

public sealed class RandomPeerSelectorTests
{
    private readonly Mock<IPeerRegistry> registryMock;
    private readonly RandomPeerSelector selector;

    public RandomPeerSelectorTests()
    {
        this.registryMock = new Mock<IPeerRegistry>();
        var loggerMock = new Mock<ILogger<RandomPeerSelector>>();
        this.selector = new RandomPeerSelector(this.registryMock.Object, loggerMock.Object);
    }

    [Fact]
    public async Task GetPeersForGossipAsync_WhenFanoutIsZero_ShouldThrowArgumentOutOfRangeException()
    {
        // Act & Assert
        await Should.ThrowAsync<ArgumentOutOfRangeException>(async () =>
            await this.selector.GetPeersForGossipAsync(0, CancellationToken.None));
    }

    [Fact]
    public async Task GetPeersForGossipAsync_WithNoActivePeers_ShouldReturnEmpty()
    {
        // Arrange
        this.registryMock.Setup(r => r.GetPeersByStatusAsync(PeerStatus.Active, It.IsAny<CancellationToken>()))
            .ReturnsAsync([]);

        // Act
        var result = await this.selector.GetPeersForGossipAsync(3, CancellationToken.None);

        // Assert
        result.ShouldBeEmpty();
    }

    [Fact]
    public async Task GetPeersForGossipAsync_WithActivePeers_ShouldReturnRequestedFanoutCount()
    {
        // Arrange
        var activePeers = Enumerable.Range(1, 5)
            .Select(_ => new PeerNode(new PeerId(Guid.NewGuid()), new PeerEndpoint("localhost", 8080)))
            .ToList();

        this.registryMock.Setup(r => r.GetPeersByStatusAsync(PeerStatus.Active, It.IsAny<CancellationToken>()))
            .ReturnsAsync(activePeers);

        // Act
        var result = (await this.selector.GetPeersForGossipAsync(3, CancellationToken.None)).ToList();

        // Assert
        result.Count.ShouldBe(3);
        result.ShouldAllBe(p => activePeers.Contains(p));
    }

    [Fact]
    public async Task GetPeersForGossipAsync_WithFewerActivePeersThanFanout_ShouldReturnAllAvailable()
    {
        // Arrange
        var activePeers = Enumerable.Range(1, 2)
            .Select(_ => new PeerNode(new PeerId(Guid.NewGuid()), new PeerEndpoint("localhost", 8080)))
            .ToList();

        this.registryMock.Setup(r => r.GetPeersByStatusAsync(PeerStatus.Active, It.IsAny<CancellationToken>()))
            .ReturnsAsync(activePeers);

        // Act
        var result = (await this.selector.GetPeersForGossipAsync(5, CancellationToken.None)).ToList();

        // Assert
        result.Count.ShouldBe(2);
        result.ShouldAllBe(p => activePeers.Contains(p));
    }
}