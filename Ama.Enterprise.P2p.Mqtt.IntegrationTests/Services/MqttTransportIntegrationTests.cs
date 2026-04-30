namespace Ama.Enterprise.P2p.Mqtt.IntegrationTests.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Mqtt.Extensions;
using Ama.Enterprise.P2p.Mqtt.Models;
using Ama.Enterprise.P2p.Mqtt.Services;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

public sealed class MqttTransportIntegrationTests(ITestOutputHelper testOutputHelper)
{
    private sealed record DummyPeerEndpoint : PeerEndpoint;

    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));

    [IntegrationFact(Skip = "Find another mqtt server to test")]
    public async Task MqttTransport_EndToEndMessageExchange_Succeeds()
    {
        // Arrange
        var meshId = "mqtt-mesh-e2e";
        var topicPrefix = "integration-test/e2e";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var payloadBytes = System.Text.Encoding.UTF8.GetBytes("Hello MQTT World");
        var messageToSend = new GossipMessage(meshId, Guid.NewGuid(), peerAId, 10, payloadBytes);

        testOutputHelper.WriteLine("Initializing DI Nodes...");
        await using var nodeA = CreateTestNode(meshId, peerAId, topicPrefix);
        await using var nodeB = CreateTestNode(meshId, peerBId, topicPrefix);

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

        // Start node A listener as well to ensure its client is connected to the broker
        await nodeA.Listener.StartListeningAsync(_ => Task.CompletedTask, cts.Token);

        var endpointB = new MqttPeerEndpoint(peerBId.Value.ToString("N")); 
        
        // Act & Assert - Retrying cleanly prevents race conditions with public broker subscription delays
        testOutputHelper.WriteLine("Sending message from Transport A iteratively until received...");
        
        GossipMessage? receivedMessage = null;
        var timeoutTime = DateTime.UtcNow.AddSeconds(20);

        while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
        {
            await nodeA.Transport.SendAsync(endpointB, messageToSend, cts.Token);

            try
            {
                // Wait briefly for the message to propagate natively
                receivedMessage = await messageCompletionSource.Task.WaitAsync(TimeSpan.FromSeconds(1), cts.Token);
                break;
            }
            catch (TimeoutException)
            {
                // Not received yet, loop natively triggering another robust delivery
            }
        }

        receivedMessage.ShouldNotBeNull("Message was not received within the operational retry limits effectively.");
        var receivedText = System.Text.Encoding.UTF8.GetString(receivedMessage.Payload.ToArray());
        receivedText.ShouldBe("Hello MQTT World");

        await nodeA.Listener.StopListeningAsync(cts.Token);
        await nodeB.Listener.StopListeningAsync(cts.Token);
        testOutputHelper.WriteLine("Test finished.");
    }

    [IntegrationFact(Skip = "Find another mqtt server to test")]
    public async Task MqttTransport_BidirectionalMessageExchange_Succeeds()
    {
        // Arrange
        var meshId = "mqtt-mesh-bidirectional";
        var topicPrefix = "integration-test/bidirectional";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var messageAtoB = new GossipMessage(meshId, Guid.NewGuid(), peerAId, 10, System.Text.Encoding.UTF8.GetBytes("AtoB"));
        var messageBtoA = new GossipMessage(meshId, Guid.NewGuid(), peerBId, 10, System.Text.Encoding.UTF8.GetBytes("BtoA"));

        await using var nodeA = CreateTestNode(meshId, peerAId, topicPrefix);
        await using var nodeB = CreateTestNode(meshId, peerBId, topicPrefix);

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

        var endpointB = new MqttPeerEndpoint(peerBId.Value.ToString("N"));
        var endpointA = new MqttPeerEndpoint(peerAId.Value.ToString("N"));

        // Act & Assert - Bidirectional looping ensuring network binds are dynamically resolved properly
        var timeoutTime = DateTime.UtcNow.AddSeconds(20);

        while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
        {
            if (!messageCompletionSourceA.Task.IsCompleted)
            {
                await nodeB.Transport.SendAsync(endpointA, messageBtoA, cts.Token);
            }

            if (!messageCompletionSourceB.Task.IsCompleted)
            {
                await nodeA.Transport.SendAsync(endpointB, messageAtoB, cts.Token);
            }

            try
            {
                // Wait briefly natively for both flows to cleanly materialize
                var delayTask = Task.Delay(TimeSpan.FromSeconds(1), cts.Token);
                await Task.WhenAny(Task.WhenAll(messageCompletionSourceA.Task, messageCompletionSourceB.Task), delayTask);
                
                if (messageCompletionSourceA.Task.IsCompleted && messageCompletionSourceB.Task.IsCompleted)
                {
                    break;
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
        
        messageCompletionSourceA.Task.IsCompletedSuccessfully.ShouldBeTrue("Node A did not gracefully receive Node B's payload.");
        messageCompletionSourceB.Task.IsCompletedSuccessfully.ShouldBeTrue("Node B did not gracefully receive Node A's payload.");

        var receivedByA = await messageCompletionSourceA.Task;
        var receivedByB = await messageCompletionSourceB.Task;
        
        System.Text.Encoding.UTF8.GetString(receivedByA.Payload.ToArray()).ShouldBe("BtoA");
        System.Text.Encoding.UTF8.GetString(receivedByB.Payload.ToArray()).ShouldBe("AtoB");

        await nodeA.Listener.StopListeningAsync(cts.Token);
        await nodeB.Listener.StopListeningAsync(cts.Token);
    }

    [IntegrationFact(Skip = "Find another mqtt server to test")]
    public async Task MqttTransport_CanHandle_ReturnsFalseForOtherEndpoints()
    {
        // Arrange
        var meshId = "mqtt-mesh-canhandle";
        var topicPrefix = "integration-test/canhandle";
        var peerAId = new PeerId(Guid.NewGuid());
        
        await using var nodeA = CreateTestNode(meshId, peerAId, topicPrefix);

        var dummyEndpoint = new DummyPeerEndpoint();
        var message = new GossipMessage(meshId, Guid.NewGuid(), peerAId, 10, Array.Empty<byte>());
        
        // Act
        var canHandle = nodeA.Transport.CanHandle(dummyEndpoint);
        
        // Assert
        canHandle.ShouldBeFalse();

        // Ensure SendAsync gracefully completes without throwing for unsupported endpoints
        await Should.NotThrowAsync(() => nodeA.Transport.SendAsync(dummyEndpoint, message, CancellationToken.None));
    }

    private MqttTestNode CreateTestNode(string meshId, PeerId peerId, string topicPrefix)
    {
        var services = new ServiceCollection();

        // Register core CRDT capabilities natively
        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });
        
        // Core P2P dependencies
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        services.Configure<P2pNodeOptions>(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        });

        services.AddP2pMesh(meshId)
            .AddGossipNetwork() // Installs AOT contexts explicitly bridging GossipMessage serialization
            .AddMqttTransport(options =>
            {
                options.Host = "test.mosquitto.org";
                options.Port = 1883;
                options.TopicPrefix = topicPrefix;
            });

        var provider = services.BuildServiceProvider();

        return new MqttTestNode(
            provider,
            peerId,
            provider.GetRequiredKeyedService<IMqttClientManager>(meshId),
            provider.GetRequiredKeyedService<ITransport>(meshId),
            provider.GetRequiredKeyedService<ITransportListener>(meshId),
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private sealed record MqttTestNode(
        ServiceProvider Provider,
        PeerId Id,
        IMqttClientManager ClientManager,
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