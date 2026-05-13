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
using System.Buffers.Binary;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Models;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Handlers;

/// <summary>
/// Contains complex integration tests validating actual TCP binding, protocol cycles, and payload distributions.
/// </summary>
public sealed class TcpNetworkIntegrationTests(ITestOutputHelper testOutputHelper)
{
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    private const string TestMeshId = "TcpIntegrationMesh";

    [IntegrationFact]
    public async Task Network_ShouldPropagateMessage_ToAllConnectedNodes_ViaTcp()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var portA = GetAvailablePort();
        var portB = GetAvailablePort();
        var portC = GetAvailablePort();

        // Create 3 independent nodes running locally on different dynamically assigned TCP ports
        await using var nodeA = CreateTestNode(portA);
        await using var nodeB = CreateTestNode(portB);
        await using var nodeC = CreateTestNode(portC);

        await RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await RegisterPeerAsync(nodeA, nodeC, cts.Token);
        await RegisterPeerAsync(nodeB, nodeA, cts.Token);
        await RegisterPeerAsync(nodeB, nodeC, cts.Token);
        await RegisterPeerAsync(nodeC, nodeA, cts.Token);
        await RegisterPeerAsync(nodeC, nodeB, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);
        await nodeC.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("TcpIntegrationTestPayload123");
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeA.Handler.ReceivedMessages.Count.ShouldBe(1);
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(1);
        nodeC.Handler.ReceivedMessages.Count.ShouldBe(1);

        var messageB = nodeB.Handler.ReceivedMessages.First();
        Encoding.UTF8.GetString(messageB.Payload).ShouldBe("TcpIntegrationTestPayload123");
    }

    [IntegrationFact]
    public async Task Network_ShouldDeduplicate_AlreadySeenMessages_ViaTcp()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var portA = GetAvailablePort();
        var portB = GetAvailablePort();

        await using var nodeA = CreateTestNode(portA);
        await using var nodeB = CreateTestNode(portB);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("TcpDeduplicationTest");
        
        var messageId = Guid.NewGuid();
        var gossipMessage = new GossipMessage(TestMeshId, messageId, nodeA.Id, 5, payload);

        var serializer = nodeA.Provider.GetRequiredService<ICrdtSerializer>();
        var payloadBytes = serializer.SerializeToBytes<IMeshMessage>(gossipMessage);
        
        var lengthBytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, payloadBytes.Length);

        // First manual TCP push
        using var client1 = new TcpClient();
        await client1.ConnectAsync("127.0.0.1", portB, cts.Token);
        await client1.GetStream().WriteAsync(lengthBytes, cts.Token);
        await client1.GetStream().WriteAsync(payloadBytes, cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        // Second manual TCP push (echo duplication directly pushing the exact same message UUID envelope)
        using var client2 = new TcpClient();
        await client2.ConnectAsync("127.0.0.1", portB, cts.Token);
        await client2.GetStream().WriteAsync(lengthBytes, cts.Token);
        await client2.GetStream().WriteAsync(payloadBytes, cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        // Assert Node B handled deduplication within IP2pProtocol natively
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(1);
    }

    [IntegrationFact]
    public async Task Network_ShouldAdapt_WhenNodesJoinAndDrop_ViaTcp()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        var portA = GetAvailablePort();
        var portB = GetAvailablePort();
        var portC = GetAvailablePort();
        var portD = GetAvailablePort();

        await using var nodeA = CreateTestNode(portA);
        await using var nodeB = CreateTestNode(portB);
        await using var nodeC = CreateTestNode(portC);

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
        var payload1 = Encoding.UTF8.GetBytes("Message_Phase1_Tcp");
        await nodeA.Protocol.BroadcastAsync(payload1, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        HasPayload(nodeA, "Message_Phase1_Tcp").ShouldBeTrue();
        HasPayload(nodeB, "Message_Phase1_Tcp").ShouldBeTrue();
        HasPayload(nodeC, "Message_Phase1_Tcp").ShouldBeTrue();

        // Phase 2: Node C crashes/drops out
        await nodeC.HostedService.StopAsync(cts.Token);
        await nodeA.Registry.RemovePeerAsync(TestMeshId, nodeC.Id, cts.Token);
        await nodeB.Registry.RemovePeerAsync(TestMeshId, nodeC.Id, cts.Token);

        // Phase 3: Node D joins the network mid-flight
        await using var nodeD = CreateTestNode(portD);
        
        await RegisterPeerAsync(nodeA, nodeD, cts.Token);
        await RegisterPeerAsync(nodeB, nodeD, cts.Token);
        await RegisterPeerAsync(nodeD, nodeA, cts.Token);
        await RegisterPeerAsync(nodeD, nodeB, cts.Token);
        
        await nodeD.HostedService.StartAsync(cts.Token);

        // Send Message 2 (From B)
        var payload2 = Encoding.UTF8.GetBytes("Message_Phase3_Tcp");
        await nodeB.Protocol.BroadcastAsync(payload2, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Send Message 3 (From new Node D)
        var payload3 = Encoding.UTF8.GetBytes("Message_Phase3_FromD_Tcp");
        await nodeD.Protocol.BroadcastAsync(payload3, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeA.Handler.ReceivedMessages.Count.ShouldBe(3);
        HasPayload(nodeA, "Message_Phase3_Tcp").ShouldBeTrue();
        HasPayload(nodeA, "Message_Phase3_FromD_Tcp").ShouldBeTrue();

        nodeB.Handler.ReceivedMessages.Count.ShouldBe(3);
        HasPayload(nodeB, "Message_Phase3_Tcp").ShouldBeTrue();
        HasPayload(nodeB, "Message_Phase3_FromD_Tcp").ShouldBeTrue();

        nodeC.Handler.ReceivedMessages.Count.ShouldBe(1);
        HasPayload(nodeC, "Message_Phase3_Tcp").ShouldBeFalse();

        nodeD.Handler.ReceivedMessages.Count.ShouldBe(2);
        HasPayload(nodeD, "Message_Phase1_Tcp").ShouldBeFalse(); // Missed phase 1
        HasPayload(nodeD, "Message_Phase3_Tcp").ShouldBeTrue();
        HasPayload(nodeD, "Message_Phase3_FromD_Tcp").ShouldBeTrue();
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

    private static int GetAvailablePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}