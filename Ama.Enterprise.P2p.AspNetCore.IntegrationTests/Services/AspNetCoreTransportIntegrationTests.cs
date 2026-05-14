namespace Ama.Enterprise.P2p.AspNetCore.IntegrationTests.Services;

using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.AspNetCore.Extensions;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Ama.Enterprise.UnitTests.Networking;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

public sealed class AspNetCoreTransportIntegrationTests(ITestOutputHelper testOutputHelper, NetworkResourceManager resourceManager) : IClassFixture<NetworkResourceManager>
{
    private sealed record DummyPeerEndpoint : PeerEndpoint;

    private sealed class TestApplicationPayloadHandler : IApplicationPayloadHandler
    {
        public TaskCompletionSource<byte[]> MessageReceived { get; } = new();

        public Task HandlePayloadAsync(string meshId, PeerId senderId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
        {
            MessageReceived.TrySetResult(payload.ToArray());
            return Task.CompletedTask;
        }
    }
    
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    private readonly NetworkResourceManager resourceManager = resourceManager ?? throw new ArgumentNullException(nameof(resourceManager));

    [IntegrationFact]
    public async Task AspNetCoreTransport_EndToEndMessageExchange_Succeeds()
    {
        // Arrange
        var meshId = "aspnetcore-mesh-e2e";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        var payloadBytes = System.Text.Encoding.UTF8.GetBytes("Hello AspNetCore World");
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

        // Give Standalone web server a moment to bind and listen
        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        var endpointB = new AspNetCorePeerEndpoint("127.0.0.1", portB); 
        
        // Act - Send
        testOutputHelper.WriteLine("Sending message from Transport A...");
        await nodeA.Transport.SendAsync(endpointB, messageToSend, cts.Token);

        // Assert
        testOutputHelper.WriteLine("Awaiting message handle block...");
        var receivedMessage = await messageCompletionSource.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);
        
        var receivedText = System.Text.Encoding.UTF8.GetString(receivedMessage.Payload.ToArray());
        receivedText.ShouldBe("Hello AspNetCore World");

        await nodeB.Listener.StopListeningAsync(cts.Token);
        testOutputHelper.WriteLine("Test finished.");
    }

    [IntegrationFact]
    public async Task AspNetCoreTransport_IntegratedMode_EndToEndMessageExchange_Succeeds()
    {
        // Arrange
        var meshId = "aspnetcore-integ-e2e";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        var payloadBytes = System.Text.Encoding.UTF8.GetBytes("Hello Integrated AspNetCore World");
        var messageToSend = new GossipMessage(meshId, Guid.NewGuid(), peerAId, 10, payloadBytes);

        var handlerB = new TestApplicationPayloadHandler();

        testOutputHelper.WriteLine($"Initializing WebApplications on ports {portA} and {portB}...");
        await using var appA = CreateIntegratedTransportNode(meshId, peerAId, portA);
        await using var appB = CreateIntegratedTransportNode(meshId, peerBId, portB, handlerB);

        await appA.StartAsync(cts.Token);
        await appB.StartAsync(cts.Token);

        // Give web server a moment to bind and listen
        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        var transportA = appA.Services.GetRequiredKeyedService<ITransport>(meshId);
        var endpointB = new AspNetCorePeerEndpoint("127.0.0.1", portB); 
        
        // Act - Send
        testOutputHelper.WriteLine("Sending message from Transport A...");
        await transportA.SendAsync(endpointB, messageToSend, cts.Token);

        // Assert
        testOutputHelper.WriteLine("Awaiting message handle block...");
        var receivedPayload = await handlerB.MessageReceived.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);
        
        var receivedText = System.Text.Encoding.UTF8.GetString(receivedPayload);
        receivedText.ShouldBe("Hello Integrated AspNetCore World");

        await appA.StopAsync(cts.Token);
        await appB.StopAsync(cts.Token);
    }

    [IntegrationFact]
    public async Task AspNetCoreTransport_BidirectionalMessageExchange_Succeeds()
    {
        // Arrange
        var meshId = "aspnetcore-mesh-bidirectional";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        var messageAtoB = new GossipMessage(meshId, Guid.NewGuid(), peerAId, 10, System.Text.Encoding.UTF8.GetBytes("AtoB via AspNetCore"));
        var messageBtoA = new GossipMessage(meshId, Guid.NewGuid(), peerBId, 10, System.Text.Encoding.UTF8.GetBytes("BtoA via AspNetCore"));

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

        var endpointB = new AspNetCorePeerEndpoint("127.0.0.1", portB);
        var endpointA = new AspNetCorePeerEndpoint("127.0.0.1", portA);

        // Act - Send in both directions
        await nodeA.Transport.SendAsync(endpointB, messageAtoB, cts.Token);
        await nodeB.Transport.SendAsync(endpointA, messageBtoA, cts.Token);

        // Assert
        var receivedByA = await messageCompletionSourceA.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);
        var receivedByB = await messageCompletionSourceB.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);
        
        System.Text.Encoding.UTF8.GetString(receivedByA.Payload.ToArray()).ShouldBe("BtoA via AspNetCore");
        System.Text.Encoding.UTF8.GetString(receivedByB.Payload.ToArray()).ShouldBe("AtoB via AspNetCore");

        await nodeA.Listener.StopListeningAsync(cts.Token);
        await nodeB.Listener.StopListeningAsync(cts.Token);
    }

    [IntegrationFact]
    public async Task AspNetCoreTransport_SendToDeadEndpoint_RemovesPeerAndThrowsHttpRequestException()
    {
        // Arrange
        var meshId = "aspnetcore-mesh-dead";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var deadPeerId = new PeerId(Guid.NewGuid());
        
        var portA = resourceManager.GetNextPort();
        var deadPort = resourceManager.GetNextPort();

        await using var nodeA = CreateTestNode(meshId, peerAId, portA);

        var deadEndpoint = new AspNetCorePeerEndpoint("127.0.0.1", deadPort);
        
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
    public async Task AspNetCoreTransport_CanHandle_ReturnsFalseForOtherEndpoints()
    {
        // Arrange
        var meshId = "aspnetcore-mesh-canhandle";
        var peerAId = new PeerId(Guid.NewGuid());
        var portA = resourceManager.GetNextPort();
        
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

    private AspNetCoreTestNode CreateTestNode(string meshId, PeerId peerId, int listenPort)
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
            .AddAspNetCoreTransport(options =>
            {
                options.HostingMode = AspNetCoreHostingMode.Standalone;
                options.StandaloneListenHost = "127.0.0.1";
                options.StandaloneListenPort = listenPort;
                options.AdvertisedHost = "127.0.0.1";
                options.AdvertisedPort = listenPort;
                options.PathPrefix = "/test/p2p/messages/";
            });

        var provider = services.BuildServiceProvider();

        return new AspNetCoreTestNode(
            provider,
            peerId,
            provider.GetRequiredKeyedService<ITransport>(meshId),
            provider.GetRequiredKeyedService<ITransportListener>(meshId),
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private WebApplication CreateIntegratedTransportNode(string meshId, PeerId peerId, int listenPort, IApplicationPayloadHandler? payloadHandler = null)
    {
        var builder = WebApplication.CreateBuilder();

        builder.Logging.AddXunit(testOutputHelper);
        builder.Logging.SetMinimumLevel(LogLevel.Trace);

        builder.Services.AddCrdt();
        builder.Services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        builder.Services.Configure<P2pNodeOptions>(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        });

        builder.Services.AddP2pMesh(meshId)
            .AddGossipNetwork()
            .AddAspNetCoreTransport(options =>
            {
                options.HostingMode = AspNetCoreHostingMode.Integrated;
                options.AdvertisedHost = "127.0.0.1";
                options.AdvertisedPort = listenPort;
                options.PathPrefix = "/test/p2p/messages/";
            });

        if (payloadHandler is not null)
        {
            builder.Services.AddKeyedSingleton(meshId, payloadHandler);
        }

        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(IPAddress.Parse("127.0.0.1"), listenPort);
        });

        var app = builder.Build();
        app.MapP2pMeshEndpoints("/test/p2p/messages");

        return app;
    }

    private sealed record AspNetCoreTestNode(
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