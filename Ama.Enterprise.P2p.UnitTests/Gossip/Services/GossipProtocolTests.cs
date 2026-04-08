namespace Ama.Enterprise.P2p.UnitTests.Gossip.Services;

using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Gossip;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

public sealed class GossipProtocolTests
{
    private const string TestMeshId = "TestMesh";
    private readonly Mock<ITransport<GossipMessage>> transportMock;
    private readonly Mock<ITransportListener<GossipMessage>> listenerMock;
    private readonly Mock<IPeerSelector> peerSelectorMock;
    private readonly Mock<IMessageDispatcher<GossipMessage>> dispatcherMock;
    private readonly Mock<ILogger<GossipProtocol>> loggerMock;
    private readonly Mock<IOptionsMonitor<GossipOptions>> gossipOptionsMock;
    private readonly Mock<IOptionsMonitor<P2pNodeOptions>> nodeOptionsMock;

    public GossipProtocolTests()
    {
        transportMock = new Mock<ITransport<GossipMessage>>();
        listenerMock = new Mock<ITransportListener<GossipMessage>>();
        peerSelectorMock = new Mock<IPeerSelector>();
        dispatcherMock = new Mock<IMessageDispatcher<GossipMessage>>();
        loggerMock = new Mock<ILogger<GossipProtocol>>();
        
        gossipOptionsMock = new Mock<IOptionsMonitor<GossipOptions>>();
        gossipOptionsMock.Setup(o => o.Get(TestMeshId)).Returns(new GossipOptions { GossipInterval = TimeSpan.FromMilliseconds(50), Fanout = 2 });

        nodeOptionsMock = new Mock<IOptionsMonitor<P2pNodeOptions>>();
        nodeOptionsMock.Setup(o => o.Get(TestMeshId)).Returns(new P2pNodeOptions 
        { 
            LocalPeerId = Guid.NewGuid(),
            LocalEndpoint = new HttpPeerEndpoint("localhost", 8080)
        });
    }

    private GossipProtocol CreateProtocol() => new(
        TestMeshId,
        gossipOptionsMock.Object,
        nodeOptionsMock.Object,
        transportMock.Object,
        listenerMock.Object,
        peerSelectorMock.Object,
        dispatcherMock.Object,
        loggerMock.Object);

    [Fact]
    public async Task StartAsync_ShouldStartListener()
    {
        // Arrange
        using var protocol = CreateProtocol();
        listenerMock.Setup(l => l.StartListeningAsync(It.IsAny<Func<GossipMessage, Task>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // Act
        await protocol.StartAsync(CancellationToken.None);

        // Assert
        listenerMock.Verify(l => l.StartListeningAsync(It.IsAny<Func<GossipMessage, Task>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task BroadcastAsync_ShouldDispatchLocallyAndEnqueueForGossiping()
    {
        // Arrange
        using var protocol = CreateProtocol();
        listenerMock.Setup(l => l.StartListeningAsync(It.IsAny<Func<GossipMessage, Task>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
            
        var payload = new byte[] { 1, 2, 3 };

        await protocol.StartAsync(CancellationToken.None);

        // Act
        await protocol.BroadcastAsync(payload, CancellationToken.None);

        // Assert
        dispatcherMock.Verify(d => d.DispatchAsync(
            It.Is<GossipMessage>(m => m.Payload.ToArray().SequenceEqual(payload)), 
            It.IsAny<CancellationToken>()), Times.Once);
            
        // Stop to clean up background tasks
        await protocol.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task GossipLoop_ShouldForwardEnqueuedMessagesToSelectedPeers()
    {
        // Arrange
        using var protocol = CreateProtocol();
        listenerMock.Setup(l => l.StartListeningAsync(It.IsAny<Func<GossipMessage, Task>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
            
        var peerNode = new PeerNode(new PeerId(Guid.NewGuid()), new HttpPeerEndpoint("localhost", 8080));
        peerSelectorMock.Setup(ps => ps.GetPeersAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new[] { peerNode });

        await protocol.StartAsync(CancellationToken.None);
        await protocol.BroadcastAsync(new byte[] { 1, 2, 3 }, CancellationToken.None);

        // Act
        // Wait enough time for the background tick to fire (interval is 50ms)
        await Task.Delay(150);

        // Assert
        transportMock.Verify(t => t.SendAsync(
            It.Is<PeerEndpoint>(e => e is HttpPeerEndpoint && ((HttpPeerEndpoint)e).Host == "localhost" && ((HttpPeerEndpoint)e).Port == 8080),
            It.Is<GossipMessage>(m => m.Payload.ToArray().SequenceEqual(new byte[] { 1, 2, 3 })),
            It.IsAny<CancellationToken>()), Times.AtLeastOnce);

        await protocol.StopAsync(CancellationToken.None);
    }
}