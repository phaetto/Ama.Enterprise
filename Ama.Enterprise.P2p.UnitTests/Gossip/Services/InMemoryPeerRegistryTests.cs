namespace Ama.Enterprise.P2p.UnitTests.Gossip.Services;

using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;

public sealed class InMemoryPeerRegistryTests
{
    private readonly Mock<IPeerTopologyObserver> observerMock;
    private readonly Mock<ILogger<InMemoryPeerRegistry>> loggerMock;
    private readonly InMemoryPeerRegistry registry;

    public InMemoryPeerRegistryTests()
    {
        this.observerMock = new Mock<IPeerTopologyObserver>();
        this.loggerMock = new Mock<ILogger<InMemoryPeerRegistry>>();
        this.registry = new InMemoryPeerRegistry([this.observerMock.Object], this.loggerMock.Object);
    }

    [Fact]
    public async Task AddOrUpdatePeerAsync_NewPeer_ShouldAddAndNotifyJoined()
    {
        // Arrange
        var peerId = new PeerId(Guid.NewGuid());
        var node = new PeerNode(peerId, new PeerEndpoint("localhost", 8080));

        // Act
        await this.registry.AddOrUpdatePeerAsync(node, PeerStatus.Active, CancellationToken.None);

        // Assert
        var peers = await this.registry.GetAllPeersAsync(CancellationToken.None);
        peers.ShouldContain(node);

        this.observerMock.Verify(o => o.OnPeerJoinedAsync(node, It.IsAny<CancellationToken>()), Times.Once);
        this.observerMock.Verify(o => o.OnPeerStatusChangedAsync(It.IsAny<PeerId>(), It.IsAny<PeerStatus>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task AddOrUpdatePeerAsync_ExistingPeer_StatusChanged_ShouldNotifyStatusChange()
    {
        // Arrange
        var peerId = new PeerId(Guid.NewGuid());
        var node = new PeerNode(peerId, new PeerEndpoint("localhost", 8080));
        
        await this.registry.AddOrUpdatePeerAsync(node, PeerStatus.Active, CancellationToken.None);
        this.observerMock.Invocations.Clear();

        // Act
        await this.registry.AddOrUpdatePeerAsync(node, PeerStatus.Suspect, CancellationToken.None);

        // Assert
        this.observerMock.Verify(o => o.OnPeerJoinedAsync(It.IsAny<PeerNode>(), It.IsAny<CancellationToken>()), Times.Never);
        this.observerMock.Verify(o => o.OnPeerStatusChangedAsync(peerId, PeerStatus.Suspect, It.IsAny<CancellationToken>()), Times.Once);

        var suspectPeers = await this.registry.GetPeersByStatusAsync(PeerStatus.Suspect, CancellationToken.None);
        suspectPeers.ShouldContain(node);
    }

    [Fact]
    public async Task RemovePeerAsync_ExistingPeer_ShouldRemoveAndNotifyDeparted()
    {
        // Arrange
        var peerId = new PeerId(Guid.NewGuid());
        var node = new PeerNode(peerId, new PeerEndpoint("localhost", 8080));
        
        await this.registry.AddOrUpdatePeerAsync(node, PeerStatus.Active, CancellationToken.None);
        this.observerMock.Invocations.Clear();

        // Act
        await this.registry.RemovePeerAsync(peerId, CancellationToken.None);

        // Assert
        var peers = await this.registry.GetAllPeersAsync(CancellationToken.None);
        peers.ShouldBeEmpty();

        this.observerMock.Verify(o => o.OnPeerDepartedAsync(peerId, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task AddOrUpdatePeerAsync_EmptyGuid_ShouldThrowArgumentException()
    {
        // Arrange
        var peerId = new PeerId(Guid.Empty);
        var node = new PeerNode(peerId, new PeerEndpoint("localhost", 8080));

        // Act & Assert
        await Should.ThrowAsync<ArgumentException>(async () => 
            await this.registry.AddOrUpdatePeerAsync(node, PeerStatus.Active, CancellationToken.None));
    }
}