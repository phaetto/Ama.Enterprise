namespace Ama.Enterprise.P2p.IntegrationTests.Gossip;

using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.IntegrationTests.Attributes;
using Ama.Enterprise.P2p.IntegrationTests.Extensions;
using Ama.Enterprise.P2p.IntegrationTests.Gossip.Handlers;
using Ama.Enterprise.P2p.IntegrationTests.Gossip.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

/// <summary>
/// Contains advanced integration tests focusing on edge cases, high concurrency, and specific network topologies.
/// </summary>
public sealed class P2pAdvancedIntegrationTests
{
    private readonly ITestOutputHelper testOutputHelper;
    private const string TestMeshId = "AdvancedIntegrationMesh";

    public P2pAdvancedIntegrationTests(ITestOutputHelper testOutputHelper)
    {
        this.testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    }

    [IntegrationFact]
    public async Task Network_ShouldPropagateOverMultipleHops_InChainTopology()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Create a linear chain: A <-> B <-> C <-> D
        // Node A only knows B, Node B knows A and C, Node C knows B and D, Node D only knows C.
        await using var nodeA = CreateTestNode(8201);
        await using var nodeB = CreateTestNode(8202);
        await using var nodeC = CreateTestNode(8203);
        await using var nodeD = CreateTestNode(8204);

        await RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await RegisterPeerAsync(nodeB, nodeA, cts.Token);
        
        await RegisterPeerAsync(nodeB, nodeC, cts.Token);
        await RegisterPeerAsync(nodeC, nodeB, cts.Token);
        
        await RegisterPeerAsync(nodeC, nodeD, cts.Token);
        await RegisterPeerAsync(nodeD, nodeC, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);
        await nodeC.HostedService.StartAsync(cts.Token);
        await nodeD.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("ChainTopologyMessage");
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        // Wait to allow multi-hop gossip to ripple through the network
        await Task.Delay(TimeSpan.FromSeconds(4), cts.Token);

        // Assert that the message successfully hopped through B and C to reach D
        HasPayload(nodeA, "ChainTopologyMessage").ShouldBeTrue(); // A also receives/processes the broadcast locally or via echo.
        HasPayload(nodeB, "ChainTopologyMessage").ShouldBeTrue();
        HasPayload(nodeC, "ChainTopologyMessage").ShouldBeTrue();
        HasPayload(nodeD, "ChainTopologyMessage").ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task Network_ShouldStopPropagating_WhenTtlIsExhausted()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Nodes configured with a strict TTL of 1
        await using var nodeA = CreateTestNode(8205, defaultTtl: 1);
        await using var nodeB = CreateTestNode(8206, defaultTtl: 1);
        await using var nodeC = CreateTestNode(8207, defaultTtl: 1);

        // Chain topology: A <-> B <-> C
        await RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await RegisterPeerAsync(nodeB, nodeA, cts.Token);
        await RegisterPeerAsync(nodeB, nodeC, cts.Token);
        await RegisterPeerAsync(nodeC, nodeB, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);
        await nodeC.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("TtlExpirationMessage");
        
        // Node A broadcasts. Since TTL=1, Node B will receive it, decrement TTL to 0, and SHOULD NOT forward it to C.
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Assert Node B got the message
        HasPayload(nodeB, "TtlExpirationMessage").ShouldBeTrue();
        
        // Assert Node C did NOT get the message due to TTL exhaustion at Node B
        HasPayload(nodeC, "TtlExpirationMessage").ShouldBeFalse();
    }

    [IntegrationFact]
    public async Task Network_ShouldHandleConcurrentBroadcasts_WithoutDataLoss()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        await using var nodeA = CreateTestNode(8208);
        await using var nodeB = CreateTestNode(8209);

        await RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await RegisterPeerAsync(nodeB, nodeA, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        const int messageCount = 50;

        // Flood Node A with concurrent broadcast requests
        var broadcastTasks = Enumerable.Range(0, messageCount).Select(i =>
        {
            var payload = Encoding.UTF8.GetBytes($"ConcurrentMessage_{i}");
            return nodeA.Protocol.BroadcastAsync(payload, cts.Token);
        });

        await Task.WhenAll(broadcastTasks);

        // Wait to allow all messages to be serialized, transmitted, and dispatched on B
        await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);

        // Assert Node B handled the heavy load and received exactly all distinct messages safely
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(messageCount);

        for (var i = 0; i < messageCount; i++)
        {
            HasPayload(nodeB, $"ConcurrentMessage_{i}").ShouldBeTrue();
        }
    }

    private bool HasPayload(TestNode node, string expectedText)
    {
        if (node is null)
        {
            throw new ArgumentNullException(nameof(node));
        }

        if (string.IsNullOrEmpty(expectedText))
        {
            throw new ArgumentException("Expected text cannot be null or empty.", nameof(expectedText));
        }

        return node.Handler.ReceivedMessages.Any(m => 
            Encoding.UTF8.GetString(m.Payload.Span) == expectedText);
    }

    private TestNode CreateTestNode(int port, int defaultTtl = 5)
    {
        var services = new ServiceCollection();

        services.AddCrdt();
        
        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        services.AddP2pMesh(TestMeshId)
            .AddGossipNetwork(options =>
            {
                options.ListenHost = "localhost";
                options.ListenPort = port;
                options.GossipInterval = TimeSpan.FromMilliseconds(500); 
                options.Fanout = 2;
                options.DefaultTimeToLive = defaultTtl;
            });

        var handler = new TestMessageHandler();
        services.AddSingleton(handler);
        
        services.AddKeyedSingleton<IMessageHandler<GossipMessage>>(TestMeshId, (sp, key) => sp.GetRequiredService<TestMessageHandler>());

        var provider = services.BuildServiceProvider();

        // Use polymorphic concrete endpoint
        var endpoint = new HttpPeerEndpoint("localhost", port);
        var peerId = new PeerId(Guid.NewGuid());

        return new TestNode(
            provider,
            peerId,
            endpoint,
            handler,
            provider.GetServices<IHostedService>().OfType<P2pHostedService>().First(),
            provider.GetRequiredKeyedService<IP2pProtocol>(TestMeshId),
            provider.GetRequiredKeyedService<IPeerRegistry>(TestMeshId)
        );
    }

    private async Task RegisterPeerAsync(TestNode sourceNode, TestNode targetNode, CancellationToken cancellationToken)
    {
        if (sourceNode is null)
        {
            throw new ArgumentNullException(nameof(sourceNode));
        }
        
        if (targetNode is null)
        {
            throw new ArgumentNullException(nameof(targetNode));
        }

        var nodeDetails = new PeerNode(targetNode.Id, targetNode.Endpoint);
        await sourceNode.Registry.AddOrUpdatePeerAsync(nodeDetails, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
    }
}