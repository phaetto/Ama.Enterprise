namespace Ama.Enterprise.P2p.IntegrationTests.Algorithms;

using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Ama.Enterprise.UnitTests.Networking;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Models;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Handlers;

/// <summary>
/// Contains complex integration tests validating actual generic TCP bindings natively, protocol cycles, and payload distributions.
/// </summary>
public sealed class P2pNetworkIntegrationTests(ITestOutputHelper testOutputHelper, NetworkResourceManager resourceManager) : IClassFixture<NetworkResourceManager>
{
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    private readonly NetworkResourceManager resourceManager = resourceManager ?? throw new ArgumentNullException(nameof(resourceManager));
    private const string TestMeshId = "BasicIntegrationMesh";

    [IntegrationFact]
    public async Task Network_ShouldPropagateMessage_ToAllConnectedNodes()
    {
        // Set up cancellation token with a safeguard timeout to prevent hanging tests
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Create 3 independent nodes running locally on different ports
        await using var nodeA = CreateTestNode(resourceManager.GetNextPort());
        await using var nodeB = CreateTestNode(resourceManager.GetNextPort());
        await using var nodeC = CreateTestNode(resourceManager.GetNextPort());

        // Establish a mesh topology for quick propagation
        await RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await RegisterPeerAsync(nodeA, nodeC, cts.Token);
        await RegisterPeerAsync(nodeB, nodeA, cts.Token);
        await RegisterPeerAsync(nodeB, nodeC, cts.Token);
        await RegisterPeerAsync(nodeC, nodeA, cts.Token);
        await RegisterPeerAsync(nodeC, nodeB, cts.Token);

        // Start generic host services (opens ports)
        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);
        await nodeC.HostedService.StartAsync(cts.Token);

        // Node A broadcasts a payload to the network
        var payload = Encoding.UTF8.GetBytes("IntegrationTestPayload123");
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        // Wait to allow gossip loop background tasks and TCP transports to fulfill
        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Assert that the message propagated throughout the test mesh
        nodeA.Handler.ReceivedMessages.Count.ShouldBe(1);
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(1);
        nodeC.Handler.ReceivedMessages.Count.ShouldBe(1);

        var messageB = nodeB.Handler.ReceivedMessages.First();
        Encoding.UTF8.GetString(messageB.Payload).ShouldBe("IntegrationTestPayload123");
    }

    [IntegrationFact]
    public async Task Network_ShouldDeduplicate_AlreadySeenMessages()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await using var nodeA = CreateTestNode(resourceManager.GetNextPort());
        await using var nodeB = CreateTestNode(resourceManager.GetNextPort());

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("DeduplicationTest");
        
        var messageId = Guid.NewGuid();
        var gossipMessage = new GossipMessage(TestMeshId, messageId, nodeA.Id, 5, payload);

        var router = nodeA.Provider.GetRequiredKeyedService<ITransportRouter>(TestMeshId);

        // First send directly explicitly mapping target node avoiding arbitrary epidemic evaluations
        await router.SendAsync(nodeB.Endpoint, gossipMessage, cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        // Second send (echo duplication directly pushing the exact same message UUID envelope natively securely over TCP limits)
        await router.SendAsync(nodeB.Endpoint, gossipMessage, cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        // Assert Node B handled deduplication within IP2pProtocol natively evaluating duplicate IDs cleanly
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(1);
    }

    [IntegrationFact]
    public async Task BroadcastAsync_ShouldThrow_WhenPayloadExceedsLimit()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var nodeA = CreateTestNode(resourceManager.GetNextPort());

        var largePayload = new byte[Constants.MaximumPayloadSizeBytes + 1];

        await Should.ThrowAsync<ArgumentException>(async () =>
        {
            await nodeA.Protocol.BroadcastAsync(largePayload, cts.Token);
        });
    }

    [IntegrationFact]
    public async Task Network_ShouldAdapt_WhenNodesJoinAndDrop()
    {
        // Generous timeout for a multi-phase lifecycle test
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        // Use distinct ports to avoid parallel test execution conflicts
        await using var nodeA = CreateTestNode(resourceManager.GetNextPort());
        await using var nodeB = CreateTestNode(resourceManager.GetNextPort());
        await using var nodeC = CreateTestNode(resourceManager.GetNextPort());

        // Phase 1: Setup initial A-B-C mesh
        await RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await RegisterPeerAsync(nodeA, nodeC, cts.Token);
        await RegisterPeerAsync(nodeB, nodeA, cts.Token);
        await RegisterPeerAsync(nodeB, nodeC, cts.Token);
        await RegisterPeerAsync(nodeC, nodeA, cts.Token);
        await RegisterPeerAsync(nodeC, nodeB, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);
        await nodeC.HostedService.StartAsync(cts.Token);

        // Send Message 1
        var payload1 = Encoding.UTF8.GetBytes("Message_Phase1");
        await nodeA.Protocol.BroadcastAsync(payload1, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Assert Phase 1: Everyone got Message 1
        HasPayload(nodeA, "Message_Phase1").ShouldBeTrue();
        HasPayload(nodeB, "Message_Phase1").ShouldBeTrue();
        HasPayload(nodeC, "Message_Phase1").ShouldBeTrue();

        // Phase 2: Node C crashes/drops out
        await nodeC.HostedService.StopAsync(cts.Token);
        await nodeA.Registry.RemovePeerAsync(TestMeshId, nodeC.Id, cts.Token);
        await nodeB.Registry.RemovePeerAsync(TestMeshId, nodeC.Id, cts.Token);

        // Phase 3: Node D joins the network mid-flight
        await using var nodeD = CreateTestNode(resourceManager.GetNextPort());
        
        await RegisterPeerAsync(nodeA, nodeD, cts.Token);
        await RegisterPeerAsync(nodeB, nodeD, cts.Token);
        await RegisterPeerAsync(nodeD, nodeA, cts.Token);
        await RegisterPeerAsync(nodeD, nodeB, cts.Token);
        
        await nodeD.HostedService.StartAsync(cts.Token);

        // Send Message 2 (From B)
        var payload2 = Encoding.UTF8.GetBytes("Message_Phase3");
        await nodeB.Protocol.BroadcastAsync(payload2, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Send Message 3 (From new Node D)
        var payload3 = Encoding.UTF8.GetBytes("Message_Phase3_FromD");
        await nodeD.Protocol.BroadcastAsync(payload3, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Assert Final State:
        // A should have 3 messages
        nodeA.Handler.ReceivedMessages.Count.ShouldBe(3);
        HasPayload(nodeA, "Message_Phase3").ShouldBeTrue();
        HasPayload(nodeA, "Message_Phase3_FromD").ShouldBeTrue();

        // B should have 3 messages
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(3);
        HasPayload(nodeB, "Message_Phase3").ShouldBeTrue();
        HasPayload(nodeB, "Message_Phase3_FromD").ShouldBeTrue();

        // C should ONLY have the 1 message from before it dropped
        nodeC.Handler.ReceivedMessages.Count.ShouldBe(1);
        HasPayload(nodeC, "Message_Phase3").ShouldBeFalse();
        HasPayload(nodeC, "Message_Phase3_FromD").ShouldBeFalse();

        // D should have the 2 messages sent after it joined
        nodeD.Handler.ReceivedMessages.Count.ShouldBe(2);
        HasPayload(nodeD, "Message_Phase1").ShouldBeFalse(); // Joined late, missed phase 1
        HasPayload(nodeD, "Message_Phase3").ShouldBeTrue();
        HasPayload(nodeD, "Message_Phase3_FromD").ShouldBeTrue();
    }

    private bool HasPayload(TestNode node, string expectedText)
    {
        ArgumentNullException.ThrowIfNull(node);

        if (string.IsNullOrEmpty(expectedText))
        {
            throw new ArgumentException("Expected text cannot be null or empty.", nameof(expectedText));
        }

        return node.Handler.ReceivedMessages.Any(m => 
            Encoding.UTF8.GetString(m.Payload) == expectedText);
    }

    private TestNode CreateTestNode(int port)
    {
        var peerId = new PeerId(Guid.NewGuid());
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        services.AddP2pMesh(TestMeshId, options =>
            {
                options.LocalPeerId = peerId.Value;
            })
            .AddGossipNetwork(options =>
            {
                options.GossipInterval = TimeSpan.FromMilliseconds(500); 
                options.Fanout = 2;
                options.DefaultTimeToLive = 5;
            })
            .AddTcpTransport(options =>
            {
                options.ListenHost = "127.0.0.1";
                options.ListenPort = port;
            });

        var handler = new TestMessageHandler();
        services.AddSingleton(handler);
        
        services.AddKeyedSingleton<IApplicationPayloadHandler>(TestMeshId, (sp, key) => sp.GetRequiredService<TestMessageHandler>());

        var provider = services.BuildServiceProvider();

        var endpoint = new TcpPeerEndpoint("127.0.0.1", port);

        return new TestNode(
            provider,
            peerId,
            endpoint,
            handler,
            provider.GetServices<IHostedService>().OfType<P2pHostedService>().First(),
            provider.GetRequiredService<IP2pAlgorithm>(),
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private async Task RegisterPeerAsync(TestNode sourceNode, TestNode targetNode, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sourceNode);

        ArgumentNullException.ThrowIfNull(targetNode);

        var nodeDetails = new PeerNode(targetNode.Id, targetNode.Endpoint);
        await sourceNode.Registry.AddOrUpdatePeerAsync(TestMeshId, nodeDetails, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
    }
}