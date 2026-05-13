namespace Ama.Enterprise.P2p.IntegrationTests.Algorithms;

using Ama.CRDT.Extensions;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using System;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Models;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Handlers;

/// <summary>
/// Contains complex integration tests validating actual UDP binding, datagram cycles, and payload distributions.
/// </summary>
public sealed class UdpNetworkIntegrationTests(ITestOutputHelper testOutputHelper)
{
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    private const string TestMeshId = "UdpIntegrationMesh";

    [IntegrationFact]
    public async Task Network_ShouldPropagateMessage_ToAllConnectedNodes_ViaUdp()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Create 3 independent nodes running locally on different UDP ports
        await using var nodeA = CreateTestNode(8401);
        await using var nodeB = CreateTestNode(8402);
        await using var nodeC = CreateTestNode(8403);

        await RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await RegisterPeerAsync(nodeA, nodeC, cts.Token);
        await RegisterPeerAsync(nodeB, nodeA, cts.Token);
        await RegisterPeerAsync(nodeB, nodeC, cts.Token);
        await RegisterPeerAsync(nodeC, nodeA, cts.Token);
        await RegisterPeerAsync(nodeC, nodeB, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);
        await nodeC.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("UdpIntegrationTestPayload123");
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeA.Handler.ReceivedMessages.Count.ShouldBe(1);
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(1);
        nodeC.Handler.ReceivedMessages.Count.ShouldBe(1);

        var messageB = nodeB.Handler.ReceivedMessages.First();
        Encoding.UTF8.GetString(messageB.Payload).ShouldBe("UdpIntegrationTestPayload123");
    }

    [IntegrationFact]
    public async Task Network_ShouldDeduplicate_AlreadySeenMessages_ViaUdp()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await using var nodeA = CreateTestNode(8404);
        await using var nodeB = CreateTestNode(8405);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("UdpDeduplicationTest");
        
        var messageId = Guid.NewGuid();
        var gossipMessage = new GossipMessage(TestMeshId, messageId, nodeA.Id, 5, payload);

        var serializer = nodeA.Provider.GetRequiredService<ICrdtSerializer>();
        var payloadBytes = serializer.SerializeToBytes<IMeshMessage>(gossipMessage);
        
        using var client = new UdpClient();

        // First manual UDP transmission
        await client.SendAsync(payloadBytes, "127.0.0.1", 8405, cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        // Second manual UDP transmission (echo duplication directly pushing identical envelope)
        await client.SendAsync(payloadBytes, "127.0.0.1", 8405, cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        // Assert Node B handled deduplication within IP2pProtocol natively dropping datagram repetition
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(1);
    }

    [IntegrationFact]
    public async Task Network_ShouldAdapt_WhenNodesJoinAndDrop_ViaUdp()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        await using var nodeA = CreateTestNode(8411);
        await using var nodeB = CreateTestNode(8412);
        await using var nodeC = CreateTestNode(8413);

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
        var payload1 = Encoding.UTF8.GetBytes("Message_Phase1_Udp");
        await nodeA.Protocol.BroadcastAsync(payload1, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        HasPayload(nodeA, "Message_Phase1_Udp").ShouldBeTrue();
        HasPayload(nodeB, "Message_Phase1_Udp").ShouldBeTrue();
        HasPayload(nodeC, "Message_Phase1_Udp").ShouldBeTrue();

        // Phase 2: Node C crashes/drops out
        await nodeC.HostedService.StopAsync(cts.Token);
        await nodeA.Registry.RemovePeerAsync(TestMeshId, nodeC.Id, cts.Token);
        await nodeB.Registry.RemovePeerAsync(TestMeshId, nodeC.Id, cts.Token);

        // Phase 3: Node D joins the network mid-flight
        await using var nodeD = CreateTestNode(8414);
        
        await RegisterPeerAsync(nodeA, nodeD, cts.Token);
        await RegisterPeerAsync(nodeB, nodeD, cts.Token);
        await RegisterPeerAsync(nodeD, nodeA, cts.Token);
        await RegisterPeerAsync(nodeD, nodeB, cts.Token);
        
        await nodeD.HostedService.StartAsync(cts.Token);

        // Send Message 2 (From B)
        var payload2 = Encoding.UTF8.GetBytes("Message_Phase3_Udp");
        await nodeB.Protocol.BroadcastAsync(payload2, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Send Message 3 (From new Node D)
        var payload3 = Encoding.UTF8.GetBytes("Message_Phase3_FromD_Udp");
        await nodeD.Protocol.BroadcastAsync(payload3, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeA.Handler.ReceivedMessages.Count.ShouldBe(3);
        HasPayload(nodeA, "Message_Phase3_Udp").ShouldBeTrue();
        HasPayload(nodeA, "Message_Phase3_FromD_Udp").ShouldBeTrue();

        nodeB.Handler.ReceivedMessages.Count.ShouldBe(3);
        HasPayload(nodeB, "Message_Phase3_Udp").ShouldBeTrue();
        HasPayload(nodeB, "Message_Phase3_FromD_Udp").ShouldBeTrue();

        nodeC.Handler.ReceivedMessages.Count.ShouldBe(1);
        HasPayload(nodeC, "Message_Phase3_Udp").ShouldBeFalse();

        nodeD.Handler.ReceivedMessages.Count.ShouldBe(2);
        HasPayload(nodeD, "Message_Phase1_Udp").ShouldBeFalse(); // Missed phase 1
        HasPayload(nodeD, "Message_Phase3_Udp").ShouldBeTrue();
        HasPayload(nodeD, "Message_Phase3_FromD_Udp").ShouldBeTrue();
    }

    private bool HasPayload(TestNode node, string expectedText)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedText);

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
            .AddUdpTransport(options =>
            {
                options.ListenHost = "127.0.0.1";
                options.ListenPort = port;
            });

        var handler = new TestMessageHandler();
        services.AddSingleton(handler);
        
        services.AddKeyedSingleton<IApplicationPayloadHandler>(TestMeshId, (sp, key) => sp.GetRequiredService<TestMessageHandler>());

        var provider = services.BuildServiceProvider();

        var endpoint = new UdpPeerEndpoint("127.0.0.1", port);

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