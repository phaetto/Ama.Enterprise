namespace Ama.Enterprise.P2p.UnitTests.Gossip.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Gossip;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

public sealed class GossipProtocolTests
{
    private const string TestMeshId = "TestMesh";
    private readonly Mock<ITransportRouter> transportRouterMock;
    private readonly Mock<IInboundMessageQueue<GossipMessage>> inboundQueueMock;
    private readonly Mock<IPeerSelector> peerSelectorMock;
    private readonly Mock<IApplicationPayloadDispatcher> dispatcherMock;
    private readonly Mock<IFailureDetector> failureDetectorMock;
    private readonly Mock<IPeerRegistry> peerRegistryMock;
    private readonly Mock<ILogger<GossipProtocol>> loggerMock;
    private readonly Mock<IOptionsMonitor<GossipOptions>> gossipOptionsMock;
    private readonly Mock<IOptionsMonitor<P2pNodeOptions>> nodeOptionsMock;
    private readonly IServiceProvider serviceProvider;

    public GossipProtocolTests()
    {
        transportRouterMock = new Mock<ITransportRouter>();
        inboundQueueMock = new Mock<IInboundMessageQueue<GossipMessage>>();
        peerSelectorMock = new Mock<IPeerSelector>();
        dispatcherMock = new Mock<IApplicationPayloadDispatcher>();
        failureDetectorMock = new Mock<IFailureDetector>();
        peerRegistryMock = new Mock<IPeerRegistry>();
        loggerMock = new Mock<ILogger<GossipProtocol>>();
        
        gossipOptionsMock = new Mock<IOptionsMonitor<GossipOptions>>();
        gossipOptionsMock.Setup(o => o.Get(TestMeshId)).Returns(new GossipOptions { GossipInterval = TimeSpan.FromMilliseconds(50), Fanout = 2 });

        nodeOptionsMock = new Mock<IOptionsMonitor<P2pNodeOptions>>();
        nodeOptionsMock.Setup(o => o.Get(TestMeshId)).Returns(new P2pNodeOptions 
        { 
            LocalPeerId = Guid.NewGuid()
        });

        inboundQueueMock.Setup(q => q.ReadAllAsync(It.IsAny<CancellationToken>()))
            .Returns(EmptyAsyncEnumerable());

        failureDetectorMock.Setup(f => f.EvaluatePeerHealthAsync(It.IsAny<PeerId>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(PeerStatus.Active);

        peerRegistryMock.Setup(p => p.GetAllPeersAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Enumerable.Empty<PeerNode>());

        var services = new ServiceCollection();
        services.AddKeyedSingleton(TestMeshId, transportRouterMock.Object);
        services.AddKeyedSingleton(TestMeshId, inboundQueueMock.Object);
        services.AddKeyedSingleton(TestMeshId, peerSelectorMock.Object);
        services.AddKeyedSingleton(TestMeshId, dispatcherMock.Object);
        services.AddKeyedSingleton(TestMeshId, failureDetectorMock.Object);
        
        // IPeerRegistry is registered as a regular singleton globally, mapping meshes natively
        services.AddSingleton(peerRegistryMock.Object);
        
        serviceProvider = services.BuildServiceProvider();
    }

    [Fact]
    public async Task StartAsync_ShouldStartReadingFromInboundQueue()
    {
        // Arrange
        using var protocol = CreateProtocol();

        // Act
        await protocol.StartAsync(CancellationToken.None);

        // Allow async background loops to spin up
        await Task.Delay(50);

        // Assert
        inboundQueueMock.Verify(q => q.ReadAllAsync(It.IsAny<CancellationToken>()), Times.Once);

        await protocol.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task BroadcastAsync_ShouldDispatchLocallyAndEnqueueForGossiping()
    {
        // Arrange
        using var protocol = CreateProtocol();
        var payload = new byte[] { 1, 2, 3 };

        await protocol.StartAsync(CancellationToken.None);

        // Act
        await protocol.BroadcastAsync(payload, CancellationToken.None);

        // Assert
        dispatcherMock.Verify(d => d.DispatchAsync(
            TestMeshId,
            It.Is<PeerId>(p => p.Value == nodeOptionsMock.Object.Get(TestMeshId).LocalPeerId),
            It.Is<ReadOnlyMemory<byte>>(m => m.ToArray().SequenceEqual(payload)), 
            It.IsAny<CancellationToken>()), Times.Once);
            
        // Stop to clean up background tasks
        await protocol.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task GossipLoop_ShouldForwardEnqueuedMessagesToSelectedPeers()
    {
        // Arrange
        using var protocol = CreateProtocol();
            
        var peerNode = new PeerNode(new PeerId(Guid.NewGuid()), new HttpPeerEndpoint("localhost", 8080));
        peerSelectorMock.Setup(ps => ps.GetPeersAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new[] { peerNode });

        await protocol.StartAsync(CancellationToken.None);
        await protocol.BroadcastAsync(new byte[] { 1, 2, 3 }, CancellationToken.None);

        // Act
        // Wait enough time for the background tick to fire (interval is 50ms)
        await Task.Delay(150);

        // Assert
        transportRouterMock.Verify(t => t.SendAsync(
            It.Is<PeerEndpoint>(e => e is HttpPeerEndpoint && ((HttpPeerEndpoint)e).Host == "localhost" && ((HttpPeerEndpoint)e).Port == 8080),
            It.Is<GossipMessage>(m => m.Payload.ToArray().SequenceEqual(new byte[] { 1, 2, 3 })),
            It.IsAny<CancellationToken>()), Times.AtLeastOnce);

        await protocol.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ProcessInboundQueue_ShouldDispatchIncomingMessagesAndForward()
    {
        // Arrange
        var testMessage = new GossipMessage(TestMeshId, Constants.ProtocolVersion, Guid.NewGuid(), new PeerId(Guid.NewGuid()), 10, new byte[] { 4, 5, 6 });
        
        inboundQueueMock.Setup(q => q.ReadAllAsync(It.IsAny<CancellationToken>()))
            .Returns(YieldSingleMessageAsync(testMessage));

        using var protocol = CreateProtocol();

        // Act
        await protocol.StartAsync(CancellationToken.None);
        
        // Wait for inbound processing loop to pick up the message
        await Task.Delay(100);

        // Assert
        dispatcherMock.Verify(d => d.DispatchAsync(
            TestMeshId,
            testMessage.SenderId,
            It.Is<ReadOnlyMemory<byte>>(m => m.ToArray().SequenceEqual(testMessage.Payload.ToArray())), 
            It.IsAny<CancellationToken>()), Times.Once);

        // Check if heartbeat record logic was triggered correctly on inbound generic processing
        failureDetectorMock.Verify(f => f.RecordHeartbeatAsync(testMessage.SenderId, It.IsAny<CancellationToken>()), Times.Once);

        await protocol.StopAsync(CancellationToken.None);
    }

    private GossipProtocol CreateProtocol() => new(
        serviceProvider,
        new[] { new P2pMeshMetadata(TestMeshId) },
        gossipOptionsMock.Object,
        nodeOptionsMock.Object,
        loggerMock.Object);

    private async IAsyncEnumerable<GossipMessage> EmptyAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield(); 
        yield break;
    }

    private async IAsyncEnumerable<GossipMessage> YieldSingleMessageAsync(GossipMessage message, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        yield return message;
    }
}