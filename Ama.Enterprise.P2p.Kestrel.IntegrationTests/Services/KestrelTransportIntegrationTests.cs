namespace Ama.Enterprise.P2p.Kestrel.IntegrationTests.Services;

using System;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Kestrel.Extensions;
using Ama.Enterprise.P2p.Kestrel.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

public sealed class KestrelTransportIntegrationTests(ITestOutputHelper testOutputHelper)
{
    private sealed record DummyPeerEndpoint : PeerEndpoint;

    private static int portCounter = 50000;
    
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));

    private static int GetNextPort() => Interlocked.Increment(ref portCounter);

    [IntegrationFact]
    public async Task KestrelTransport_EndToEndMessageExchange_Succeeds()
    {
        // Arrange
        var meshId = "kestrel-mesh-e2e";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var portA = GetNextPort();
        var portB = GetNextPort();

        var payloadBytes = System.Text.Encoding.UTF8.GetBytes("Hello Kestrel World");
        var messageToSend = new GossipMessage(meshId, Guid.NewGuid(), peerAId, 10, payloadBytes);

        testOutputHelper.WriteLine($"Initializing DI Nodes on ports {portA} and {portB}...");
        await using var nodeA = CreateTestNode(meshId, peerAId, portA);
        await using var nodeB = CreateTestNode(meshId, peerBId, portB);

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

        // Give Kestrel a moment to bind and listen
        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        var endpointB = new KestrelPeerEndpoint("localhost", portB); 
        
        // Act - Send
        testOutputHelper.WriteLine("Sending message from Transport A...");
        await nodeA.Transport.SendAsync(endpointB, messageToSend, cts.Token);

        // Assert
        testOutputHelper.WriteLine("Awaiting message handle block...");
        var receivedMessage = await messageCompletionSource.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);
        
        var receivedText = System.Text.Encoding.UTF8.GetString(receivedMessage.Payload.ToArray());
        receivedText.ShouldBe("Hello Kestrel World");

        await nodeB.Listener.StopListeningAsync(cts.Token);
        testOutputHelper.WriteLine("Test finished.");
    }

    [IntegrationFact]
    public async Task KestrelTransport_BidirectionalMessageExchange_Succeeds()
    {
        // Arrange
        var meshId = "kestrel-mesh-bidirectional";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var portA = GetNextPort();
        var portB = GetNextPort();

        var messageAtoB = new GossipMessage(meshId, Guid.NewGuid(), peerAId, 10, System.Text.Encoding.UTF8.GetBytes("AtoB via Kestrel"));
        var messageBtoA = new GossipMessage(meshId, Guid.NewGuid(), peerBId, 10, System.Text.Encoding.UTF8.GetBytes("BtoA via Kestrel"));

        await using var nodeA = CreateTestNode(meshId, peerAId, portA);
        await using var nodeB = CreateTestNode(meshId, peerBId, portB);

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

        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        var endpointB = new KestrelPeerEndpoint("localhost", portB);
        var endpointA = new KestrelPeerEndpoint("localhost", portA);

        // Act - Send in both directions
        await nodeA.Transport.SendAsync(endpointB, messageAtoB, cts.Token);
        await nodeB.Transport.SendAsync(endpointA, messageBtoA, cts.Token);

        // Assert
        var receivedByA = await messageCompletionSourceA.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);
        var receivedByB = await messageCompletionSourceB.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);
        
        System.Text.Encoding.UTF8.GetString(receivedByA.Payload.ToArray()).ShouldBe("BtoA via Kestrel");
        System.Text.Encoding.UTF8.GetString(receivedByB.Payload.ToArray()).ShouldBe("AtoB via Kestrel");

        await nodeA.Listener.StopListeningAsync(cts.Token);
        await nodeB.Listener.StopListeningAsync(cts.Token);
    }

    [IntegrationFact]
    public async Task KestrelTransport_SendToDeadEndpoint_RemovesPeerAndThrowsHttpRequestException()
    {
        // Arrange
        var meshId = "kestrel-mesh-dead";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var deadPeerId = new PeerId(Guid.NewGuid());
        
        var portA = GetNextPort();
        var deadPort = GetNextPort();

        await using var nodeA = CreateTestNode(meshId, peerAId, portA);

        var deadEndpoint = new KestrelPeerEndpoint("localhost", deadPort);
        
        // Manually inject a peer into the registry to test auto-removal explicitly
        var dummyNode = new PeerNode(deadPeerId, deadEndpoint);
        await nodeA.Registry.AddOrUpdatePeerAsync(meshId, dummyNode, PeerStatus.Active, cts.Token);

        var message = new GossipMessage(meshId, Guid.NewGuid(), peerAId, 10, Array.Empty<byte>());
        
        // Act & Assert
        await Should.ThrowAsync<HttpRequestException>(() => 
            nodeA.Transport.SendAsync(deadEndpoint, message, cts.Token));

        var registeredPeers = await nodeA.Registry.GetAllPeersAsync(meshId, cts.Token);
        registeredPeers.Any(p => p.Id == deadPeerId).ShouldBeFalse("Transport failed to automatically remove dead peer from registry.");
    }

    [IntegrationFact]
    public async Task KestrelTransport_CanHandle_ReturnsFalseForOtherEndpoints()
    {
        // Arrange
        var meshId = "kestrel-mesh-canhandle";
        var peerAId = new PeerId(Guid.NewGuid());
        var portA = GetNextPort();
        
        await using var nodeA = CreateTestNode(meshId, peerAId, portA);

        var dummyEndpoint = new DummyPeerEndpoint();
        var message = new GossipMessage(meshId, Guid.NewGuid(), peerAId, 10, Array.Empty<byte>());
        
        // Act
        var canHandle = nodeA.Transport.CanHandle(dummyEndpoint);
        
        // Assert
        canHandle.ShouldBeFalse();

        // Ensure SendAsync completes without throwing for unsupported endpoints
        await Should.NotThrowAsync(() => nodeA.Transport.SendAsync(dummyEndpoint, message, CancellationToken.None));
    }

    private KestrelTestNode CreateTestNode(string meshId, PeerId peerId, int listenPort)
    {
        var services = new ServiceCollection();

        // Register core CRDT capabilities
        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });
        
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        services.Configure<P2pNodeOptions>(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        });

        services.AddP2pMesh(meshId)
            .AddGossipNetwork() // Installs AOT contexts explicitly bridging GossipMessage serialization
            .AddKestrelTransport(options =>
            {
                options.ListenHost = "localhost";
                options.ListenPort = listenPort;
                options.PathPrefix = "/test/p2p/messages/";
            });

        var provider = services.BuildServiceProvider();

        return new KestrelTestNode(
            provider,
            peerId,
            provider.GetRequiredKeyedService<ITransport>(meshId),
            provider.GetRequiredKeyedService<ITransportListener>(meshId),
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private sealed record KestrelTestNode(
        ServiceProvider Provider,
        PeerId Id,
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