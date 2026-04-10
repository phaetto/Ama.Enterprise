namespace Ama.Enterprise.P2p.WebRTC.IntegrationTests.Services;

using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
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

[JsonSerializable(typeof(WebRtcTransportIntegrationTests.TestMessage))]
internal partial class WebRtcIntegrationTestJsonContext : JsonSerializerContext
{
}

public sealed class WebRtcTransportIntegrationTests
{
    public readonly record struct TestMessage(string Content);

    private readonly ITestOutputHelper testOutputHelper;

    public WebRtcTransportIntegrationTests(ITestOutputHelper testOutputHelper)
    {
        this.testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    }

    [IntegrationFact]
    public async Task WebRtcTransport_EndToEndMessageExchange_Succeeds()
    {
        // Arrange
        var meshId = "integration-mesh";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var messageToSend = new TestMessage("Hello Decentralized World");

        testOutputHelper.WriteLine("Initializing DI Nodes...");
        await using var nodeA = CreateTestNode(meshId, peerAId);
        await using var nodeB = CreateTestNode(meshId, peerBId);

        // Act - Establish WebRTC Data Channel via SDP exchange
        testOutputHelper.WriteLine("Creating invitation on Node A...");
        var (connIdA, offer) = await nodeA.InvitationService.CreateInvitationAsync(cts.Token);
        
        testOutputHelper.WriteLine("Accepting invitation on Node B...");
        var (connIdB, answer) = await nodeB.InvitationService.AcceptInvitationAsync(offer, cts.Token);
        
        testOutputHelper.WriteLine("Finalizing invitation on Node A...");
        await nodeA.InvitationService.FinalizeInvitationAsync(connIdA, answer, cts.Token);

        var messageCompletionSource = new TaskCompletionSource<TestMessage>();

        await nodeB.Listener.StartListeningAsync(msg =>
        {
            testOutputHelper.WriteLine("Message successfully received by Listener B!");
            messageCompletionSource.TrySetResult(msg);
            return Task.CompletedTask;
        }, cts.Token);

        // Give WebRTC negotiation and data channel time to transition state to 'Open'
        testOutputHelper.WriteLine("Waiting for WebRTC data channels to open...");
        await Task.Delay(TimeSpan.FromSeconds(8), cts.Token);

        var endpointB = new WebRtcPeerEndpoint(connIdA); 
        
        // Act - Send
        testOutputHelper.WriteLine("Sending message from Transport A...");
        await nodeA.Transport.SendAsync(endpointB, messageToSend, cts.Token);

        // Assert
        testOutputHelper.WriteLine("Awaiting message handle block...");
        var receivedMessage = await messageCompletionSource.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);
        
        receivedMessage.Content.ShouldBe("Hello Decentralized World");

        await nodeB.Listener.StopListeningAsync(cts.Token);
        testOutputHelper.WriteLine("Test finished correctly.");
    }

    private WebRtcTestNode CreateTestNode(string meshId, PeerId peerId)
    {
        var services = new ServiceCollection();

        // Register core CRDT capabilities handling JSON serializers natively natively via AOT contexts
        services.AddCrdt();

        // Register the AOT context strictly targeting the internal integration test message type payload gracefully
        services.AddCrdtJsonTypeInfoResolver(WebRtcIntegrationTestJsonContext.Default);
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
            .AddWebRtcTransport<TestMessage>(options =>
            {
                options.IceServers = Array.Empty<string>();
                options.IceGatheringTimeout = TimeSpan.FromSeconds(2);
            });

        var provider = services.BuildServiceProvider();

        return new WebRtcTestNode(
            provider,
            peerId,
            provider.GetRequiredKeyedService<IWebRtcInvitationService>(meshId),
            provider.GetRequiredKeyedService<ITransport<TestMessage>>(meshId),
            provider.GetRequiredKeyedService<ITransportListener<TestMessage>>(meshId)
        );
    }

    private sealed record WebRtcTestNode(
        ServiceProvider Provider,
        PeerId Id,
        IWebRtcInvitationService InvitationService,
        ITransport<TestMessage> Transport,
        ITransportListener<TestMessage> Listener) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Provider.DisposeAsync();
        }
    }
}