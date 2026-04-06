using Ama.Enterprise.P2p.Models;
using Ama.Enterprise.P2p.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace Ama.Enterprise.P2p.UnitTests.Services;

public sealed class GossipProtocolTests
{
    private readonly Mock<ITransport> transportMock;
    private readonly Mock<ITransportListener> listenerMock;
    private readonly Mock<IPeerSelector> peerSelectorMock;
    private readonly Mock<IMessageDispatcher> dispatcherMock;
    private readonly Mock<ILogger<GossipProtocol>> loggerMock;
    private readonly IOptions<GossipOptions> options;

    public GossipProtocolTests()
    {
        this.transportMock = new Mock<ITransport>();
        this.listenerMock = new Mock<ITransportListener>();
        this.peerSelectorMock = new Mock<IPeerSelector>();
        this.dispatcherMock = new Mock<IMessageDispatcher>();
        this.loggerMock = new Mock<ILogger<GossipProtocol>>();
        this.options = Options.Create(new GossipOptions { GossipInterval = TimeSpan.FromMilliseconds(50), Fanout = 2 });
    }

    private GossipProtocol CreateProtocol() => new(
        this.options,
        this.transportMock.Object,
        this.listenerMock.Object,
        this.peerSelectorMock.Object,
        this.dispatcherMock.Object,
        this.loggerMock.Object);

    [Fact]
    public async Task StartAsync_ShouldStartListener()
    {
        // Arrange
        using var protocol = this.CreateProtocol();
        this.listenerMock.Setup(l => l.StartListeningAsync(It.IsAny<Func<GossipMessage, Task>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // Act
        await protocol.StartAsync(CancellationToken.None);

        // Assert
        this.listenerMock.Verify(l => l.StartListeningAsync(It.IsAny<Func<GossipMessage, Task>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task BroadcastAsync_ShouldDispatchLocallyAndEnqueueForGossiping()
    {
        // Arrange
        using var protocol = this.CreateProtocol();
        this.listenerMock.Setup(l => l.StartListeningAsync(It.IsAny<Func<GossipMessage, Task>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
            
        var payload = new byte[] { 1, 2, 3 };

        await protocol.StartAsync(CancellationToken.None);

        // Act
        await protocol.BroadcastAsync(payload, CancellationToken.None);

        // Assert
        this.dispatcherMock.Verify(d => d.DispatchAsync(
            It.Is<GossipMessage>(m => m.Payload.ToArray().SequenceEqual(payload)), 
            It.IsAny<CancellationToken>()), Times.Once);
            
        // Stop to clean up background tasks
        await protocol.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task GossipLoop_ShouldForwardEnqueuedMessagesToSelectedPeers()
    {
        // Arrange
        using var protocol = this.CreateProtocol();
        this.listenerMock.Setup(l => l.StartListeningAsync(It.IsAny<Func<GossipMessage, Task>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
            
        var peerNode = new PeerNode(new PeerId(Guid.NewGuid()), new PeerEndpoint("localhost", 8080));
        this.peerSelectorMock.Setup(ps => ps.GetPeersForGossipAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new[] { peerNode });

        await protocol.StartAsync(CancellationToken.None);
        await protocol.BroadcastAsync(new byte[] { 1, 2, 3 }, CancellationToken.None);

        // Act
        // Wait enough time for the background tick to fire (interval is 50ms)
        await Task.Delay(150);

        // Assert
        this.transportMock.Verify(t => t.SendAsync(
            It.Is<PeerEndpoint>(e => e.Host == "localhost" && e.Port == 8080),
            It.Is<GossipMessage>(m => m.Payload.ToArray().SequenceEqual(new byte[] { 1, 2, 3 })),
            It.IsAny<CancellationToken>()), Times.AtLeastOnce);

        await protocol.StopAsync(CancellationToken.None);
    }
}