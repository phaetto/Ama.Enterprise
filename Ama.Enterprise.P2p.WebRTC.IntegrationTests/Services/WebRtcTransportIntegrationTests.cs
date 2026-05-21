namespace Ama.Enterprise.P2p.WebRTC.IntegrationTests.Services;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Algorithms;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.Extensions;
using Ama.Enterprise.P2p.WebRTC.Models;
using Ama.Enterprise.P2p.WebRTC.Services;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

public sealed class WebRtcTransportIntegrationTests(ITestOutputHelper testOutputHelper)
{
    private sealed record DummyPeerEndpoint : PeerEndpoint;

    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));

    [IntegrationFact]
    public async Task WebRtcTransport_EndToEndMessageExchange_Succeeds()
    {
        // Arrange
        var meshId = "integration-mesh";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var payloadBytes = System.Text.Encoding.UTF8.GetBytes("Hello Decentralized World");
        var messageToSend = new GossipMessage(meshId, Guid.NewGuid(), peerAId, 10, payloadBytes);

        testOutputHelper.WriteLine("Initializing DI Nodes...");
        await using var nodeA = CreateTestNode(meshId, peerAId);
        await using var nodeB = CreateTestNode(meshId, peerBId);

        // Act - Establish WebRTC Data Channel via SDP exchange
        testOutputHelper.WriteLine("Creating invitation on Node A...");
        var offerDto = await nodeA.InvitationService.CreateInvitationAsync(cts.Token);
        
        testOutputHelper.WriteLine("Accepting invitation on Node B...");
        var answerDto = await nodeB.InvitationService.AcceptInvitationAsync(offerDto.SdpOffer, cts.Token);
        
        testOutputHelper.WriteLine("Finalizing invitation on Node A...");
        await nodeA.InvitationService.FinalizeInvitationAsync(offerDto.ConnectionId, answerDto.SdpAnswer, cts.Token);

        var messageCompletionSource = new TaskCompletionSource<GossipMessage>();

        await nodeB.Listener.StartListeningAsync(msg =>
        {
            if (msg is GossipMessage gossipMsg)
            {
                testOutputHelper.WriteLine("Message received by Listener B.");
                messageCompletionSource.TrySetResult(gossipMsg);
            }
            return Task.CompletedTask;
        }, cts.Token);

        // Give WebRTC negotiation and data channel time to transition state to 'Open'
        testOutputHelper.WriteLine("Waiting for WebRTC data channels to open...");
        await Task.Delay(TimeSpan.FromSeconds(8), cts.Token);

        var endpointB = new WebRtcPeerEndpoint(offerDto.ConnectionId); 
        
        // Act - Send
        testOutputHelper.WriteLine("Sending message from Transport A...");
        await nodeA.Transport.SendAsync(endpointB, messageToSend, cts.Token);

        // Assert
        testOutputHelper.WriteLine("Awaiting message handle block...");
        var receivedMessage = await messageCompletionSource.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);
        
        var receivedText = System.Text.Encoding.UTF8.GetString(receivedMessage.Payload.ToArray());
        receivedText.ShouldBe("Hello Decentralized World");

        await nodeB.Listener.StopListeningAsync(cts.Token);
        testOutputHelper.WriteLine("Test finished.");
    }

    [IntegrationFact]
    public async Task WebRtcTransport_BidirectionalMessageExchange_Succeeds()
    {
        // Arrange
        var meshId = "integration-mesh-bidirectional";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var messageAtoB = new GossipMessage(meshId, Guid.NewGuid(), peerAId, 10, System.Text.Encoding.UTF8.GetBytes("AtoB"));
        var messageBtoA = new GossipMessage(meshId, Guid.NewGuid(), peerBId, 10, System.Text.Encoding.UTF8.GetBytes("BtoA"));

        await using var nodeA = CreateTestNode(meshId, peerAId);
        await using var nodeB = CreateTestNode(meshId, peerBId);

        var offerDto = await nodeA.InvitationService.CreateInvitationAsync(cts.Token);
        var answerDto = await nodeB.InvitationService.AcceptInvitationAsync(offerDto.SdpOffer, cts.Token);
        await nodeA.InvitationService.FinalizeInvitationAsync(offerDto.ConnectionId, answerDto.SdpAnswer, cts.Token);

        var messageCompletionSourceA = new TaskCompletionSource<GossipMessage>();
        var messageCompletionSourceB = new TaskCompletionSource<GossipMessage>();

        await nodeA.Listener.StartListeningAsync(msg =>
        {
            if (msg is GossipMessage gossipMsg)
            {
                messageCompletionSourceA.TrySetResult(gossipMsg);
            }
            return Task.CompletedTask;
        }, cts.Token);

        await nodeB.Listener.StartListeningAsync(msg =>
        {
            if (msg is GossipMessage gossipMsg)
            {
                messageCompletionSourceB.TrySetResult(gossipMsg);
            }
            return Task.CompletedTask;
        }, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(8), cts.Token);

        var endpointB = new WebRtcPeerEndpoint(offerDto.ConnectionId);
        var endpointA = new WebRtcPeerEndpoint(answerDto.ConnectionId);

        // Act - Send in both directions
        await nodeA.Transport.SendAsync(endpointB, messageAtoB, cts.Token);
        await nodeB.Transport.SendAsync(endpointA, messageBtoA, cts.Token);

        // Assert
        var receivedByA = await messageCompletionSourceA.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);
        var receivedByB = await messageCompletionSourceB.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);
        
        System.Text.Encoding.UTF8.GetString(receivedByA.Payload.ToArray()).ShouldBe("BtoA");
        System.Text.Encoding.UTF8.GetString(receivedByB.Payload.ToArray()).ShouldBe("AtoB");

        await nodeA.Listener.StopListeningAsync(cts.Token);
        await nodeB.Listener.StopListeningAsync(cts.Token);
    }

    [IntegrationFact]
    public async Task WebRtcTransport_PeerDiscovery_Handshake_RegistersInPeerRegistry_Succeeds()
    {
        // Arrange
        var meshId = "integration-mesh-discovery";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        await using var nodeA = CreateTestNode(meshId, peerAId);
        await using var nodeB = CreateTestNode(meshId, peerBId);

        // Act
        var offerDto = await nodeA.InvitationService.CreateInvitationAsync(cts.Token);
        var answerDto = await nodeB.InvitationService.AcceptInvitationAsync(offerDto.SdpOffer, cts.Token);
        await nodeA.InvitationService.FinalizeInvitationAsync(offerDto.ConnectionId, answerDto.SdpAnswer, cts.Token);

        bool peerBDiscoveredByA = false;
        bool peerADiscoveredByB = false;

        // Give the WebRTC handshake message time to implicitly transmit and populate the registry
        for (int i = 0; i < 15; i++)
        {
            var peersA = await nodeA.Registry.GetAllPeersAsync(meshId, cts.Token);
            var peersB = await nodeB.Registry.GetAllPeersAsync(meshId, cts.Token);

            if (!peerBDiscoveredByA && peersA.Any(p => p.Id == peerBId)) peerBDiscoveredByA = true;
            if (!peerADiscoveredByB && peersB.Any(p => p.Id == peerAId)) peerADiscoveredByB = true;

            if (peerBDiscoveredByA && peerADiscoveredByB) break;

            await Task.Delay(1000, cts.Token);
        }

        // Assert
        peerBDiscoveredByA.ShouldBeTrue("Node A did not discover Node B via handshake.");
        peerADiscoveredByB.ShouldBeTrue("Node B did not discover Node A via handshake.");
    }

    [IntegrationFact]
    public async Task WebRtcTransport_CanHandle_ReturnsFalseForOtherEndpoints()
    {
        // Arrange
        var meshId = "integration-mesh-canhandle";
        var peerAId = new PeerId(Guid.NewGuid());
        
        await using var nodeA = CreateTestNode(meshId, peerAId);

        var dummyEndpoint = new DummyPeerEndpoint();
        var message = new GossipMessage(meshId, Guid.NewGuid(), peerAId, 10, Array.Empty<byte>());
        
        // Act
        var canHandle = nodeA.Transport.CanHandle(dummyEndpoint);
        
        // Assert
        canHandle.ShouldBeFalse();

        // Ensure SendAsync gracefully completes without throwing for unsupported endpoints
        await Should.NotThrowAsync(() => nodeA.Transport.SendAsync(dummyEndpoint, message, CancellationToken.None));
    }

    [IntegrationFact]
    public async Task WebRtcInvitationService_AcceptEmptyOffer_ThrowsArgumentException()
    {
        var meshId = "integration-mesh-empty-offer";
        var peerAId = new PeerId(Guid.NewGuid());
        
        await using var nodeA = CreateTestNode(meshId, peerAId);

        await Should.ThrowAsync<ArgumentException>(() => 
            nodeA.InvitationService.AcceptInvitationAsync(string.Empty, CancellationToken.None));
    }

    [IntegrationFact]
    public async Task WebRtcInvitationService_FinalizeUnknownConnection_ThrowsInvalidOperationException()
    {
        var meshId = "integration-mesh-unknown-finalize";
        var peerAId = new PeerId(Guid.NewGuid());
        
        await using var nodeA = CreateTestNode(meshId, peerAId);

        await Should.ThrowAsync<InvalidOperationException>(() => 
            nodeA.InvitationService.FinalizeInvitationAsync(Guid.NewGuid(), "dummy-sdp-answer", CancellationToken.None));
    }

    private WebRtcTestNode CreateTestNode(string meshId, PeerId peerId)
    {
        var services = new ServiceCollection();

        // Register core CRDT capabilities natively
        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });
        
        // Core P2P dependencies required by WebRtc Connection Manager
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        services.Configure<P2pNodeOptions>(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        });

        services.AddP2pMesh(meshId)
            .AddWebRtcTransport(options =>
            {
                // Disable external STUN lookup to accelerate local integration tests efficiently
                options.IceServers = Array.Empty<string>();
                options.IceGatheringTimeout = TimeSpan.FromSeconds(2);
            });

        var provider = services.BuildServiceProvider();

        return new WebRtcTestNode(
            provider,
            peerId,
            provider.GetRequiredKeyedService<IWebRtcInvitationService>(meshId),
            provider.GetRequiredKeyedService<ITransport>(meshId),
            provider.GetRequiredKeyedService<ITransportListener>(meshId),
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private sealed record WebRtcTestNode(
        ServiceProvider Provider,
        PeerId Id,
        IWebRtcInvitationService InvitationService,
        ITransport Transport,
        ITransportListener Listener,
        IPeerRegistry Registry) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Provider.DisposeAsync();
        }
    }
}