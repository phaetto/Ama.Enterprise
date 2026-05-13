namespace Ama.Enterprise.P2p.UnitTests.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;
using Xunit;

public sealed class InMemoryPeerRegistryTests
{
    private const string TestMeshId = "TestMesh";
    private readonly Mock<IPeerTopologyObserver> observerMock;
    private readonly Mock<ILogger<InMemoryPeerRegistry>> loggerMock;
    private readonly InMemoryPeerRegistry registry;

    public InMemoryPeerRegistryTests()
    {
        observerMock = new Mock<IPeerTopologyObserver>();
        loggerMock = new Mock<ILogger<InMemoryPeerRegistry>>();
        registry = new InMemoryPeerRegistry([observerMock.Object], loggerMock.Object);
    }

    [Fact]
    public async Task AddOrUpdatePeerAsync_NewPeer_ShouldAddAndNotifyJoined()
    {
        // Arrange
        var peerId = new PeerId(Guid.NewGuid());
        var node = new PeerNode(peerId, new HttpPeerEndpoint("localhost", 8080));

        // Act
        await registry.AddOrUpdatePeerAsync(TestMeshId, node, PeerStatus.Active, CancellationToken.None);

        // Assert
        var peers = await registry.GetAllPeersAsync(TestMeshId, CancellationToken.None);
        peers.ShouldContain(node);

        observerMock.Verify(o => o.OnPeerJoinedAsync(TestMeshId, node, It.IsAny<CancellationToken>()), Times.Once);
        observerMock.Verify(o => o.OnPeerStatusChangedAsync(It.IsAny<string>(), It.IsAny<PeerId>(), It.IsAny<PeerStatus>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task AddOrUpdatePeerAsync_ExistingPeer_StatusChanged_ShouldNotifyStatusChange()
    {
        // Arrange
        var peerId = new PeerId(Guid.NewGuid());
        var node = new PeerNode(peerId, new HttpPeerEndpoint("localhost", 8080));
        
        await registry.AddOrUpdatePeerAsync(TestMeshId, node, PeerStatus.Active, CancellationToken.None);
        observerMock.Invocations.Clear();

        // Act
        await registry.AddOrUpdatePeerAsync(TestMeshId, node, PeerStatus.Suspect, CancellationToken.None);

        // Assert
        observerMock.Verify(o => o.OnPeerJoinedAsync(It.IsAny<string>(), It.IsAny<PeerNode>(), It.IsAny<CancellationToken>()), Times.Never);
        observerMock.Verify(o => o.OnPeerStatusChangedAsync(TestMeshId, peerId, PeerStatus.Suspect, It.IsAny<CancellationToken>()), Times.Once);

        var suspectPeers = await registry.GetPeersByStatusAsync(TestMeshId, PeerStatus.Suspect, CancellationToken.None);
        suspectPeers.ShouldContain(node);
    }

    [Fact]
    public async Task RemovePeerAsync_ExistingPeer_ShouldRemoveAndNotifyDeparted()
    {
        // Arrange
        var peerId = new PeerId(Guid.NewGuid());
        var node = new PeerNode(peerId, new HttpPeerEndpoint("localhost", 8080));
        
        await registry.AddOrUpdatePeerAsync(TestMeshId, node, PeerStatus.Active, CancellationToken.None);
        observerMock.Invocations.Clear();

        // Act
        await registry.RemovePeerAsync(TestMeshId, peerId, CancellationToken.None);

        // Assert
        var peers = await registry.GetAllPeersAsync(TestMeshId, CancellationToken.None);
        peers.ShouldBeEmpty();

        observerMock.Verify(o => o.OnPeerDepartedAsync(TestMeshId, peerId, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task AddOrUpdatePeerAsync_EmptyGuid_ShouldThrowArgumentException()
    {
        // Arrange
        var peerId = new PeerId(Guid.Empty);
        var node = new PeerNode(peerId, new HttpPeerEndpoint("localhost", 8080));

        // Act & Assert
        await Should.ThrowAsync<ArgumentException>(async () => 
            await registry.AddOrUpdatePeerAsync(TestMeshId, node, PeerStatus.Active, CancellationToken.None));
    }
}