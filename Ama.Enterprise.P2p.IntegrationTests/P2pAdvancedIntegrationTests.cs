namespace Ama.Enterprise.P2p.IntegrationTests;

using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.IntegrationTests.Attributes;
using Ama.Enterprise.P2p.IntegrationTests.Extensions;
using Ama.Enterprise.P2p.IntegrationTests.Handlers;
using Ama.Enterprise.P2p.IntegrationTests.Models;
using Ama.Enterprise.P2p.Models;
using Ama.Enterprise.P2p.Services;
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
        await using var nodeA = this.CreateTestNode(8201);
        await using var nodeB = this.CreateTestNode(8202);
        await using var nodeC = this.CreateTestNode(8203);
        await using var nodeD = this.CreateTestNode(8204);

        await this.RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await this.RegisterPeerAsync(nodeB, nodeA, cts.Token);
        
        await this.RegisterPeerAsync(nodeB, nodeC, cts.Token);
        await this.RegisterPeerAsync(nodeC, nodeB, cts.Token);
        
        await this.RegisterPeerAsync(nodeC, nodeD, cts.Token);
        await this.RegisterPeerAsync(nodeD, nodeC, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);
        await nodeC.HostedService.StartAsync(cts.Token);
        await nodeD.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("ChainTopologyMessage");
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        // Wait to allow multi-hop gossip to ripple through the network
        await Task.Delay(TimeSpan.FromSeconds(4), cts.Token);

        // Assert that the message successfully hopped through B and C to reach D
        this.HasPayload(nodeA, "ChainTopologyMessage").ShouldBeTrue(); // A also receives/processes the broadcast locally or via echo.
        this.HasPayload(nodeB, "ChainTopologyMessage").ShouldBeTrue();
        this.HasPayload(nodeC, "ChainTopologyMessage").ShouldBeTrue();
        this.HasPayload(nodeD, "ChainTopologyMessage").ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task Network_ShouldStopPropagating_WhenTtlIsExhausted()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Nodes configured with a strict TTL of 1
        await using var nodeA = this.CreateTestNode(8205, defaultTtl: 1);
        await using var nodeB = this.CreateTestNode(8206, defaultTtl: 1);
        await using var nodeC = this.CreateTestNode(8207, defaultTtl: 1);

        // Chain topology: A <-> B <-> C
        await this.RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await this.RegisterPeerAsync(nodeB, nodeA, cts.Token);
        await this.RegisterPeerAsync(nodeB, nodeC, cts.Token);
        await this.RegisterPeerAsync(nodeC, nodeB, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);
        await nodeC.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("TtlExpirationMessage");
        
        // Node A broadcasts. Since TTL=1, Node B will receive it, decrement TTL to 0, and SHOULD NOT forward it to C.
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Assert Node B got the message
        this.HasPayload(nodeB, "TtlExpirationMessage").ShouldBeTrue();
        
        // Assert Node C did NOT get the message due to TTL exhaustion at Node B
        this.HasPayload(nodeC, "TtlExpirationMessage").ShouldBeFalse();
    }

    [IntegrationFact]
    public async Task Network_ShouldHandleConcurrentBroadcasts_WithoutDataLoss()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        await using var nodeA = this.CreateTestNode(8208);
        await using var nodeB = this.CreateTestNode(8209);

        await this.RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await this.RegisterPeerAsync(nodeB, nodeA, cts.Token);

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
            this.HasPayload(nodeB, $"ConcurrentMessage_{i}").ShouldBeTrue();
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
        
        services.AddLogging(builder => 
        {
            builder.AddXunit(this.testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        services.AddP2pGossipNetwork(options =>
        {
            options.ListenHost = "localhost";
            options.ListenPort = port;
            options.GossipInterval = TimeSpan.FromMilliseconds(500); 
            options.Fanout = 2;
            options.DefaultTimeToLive = defaultTtl;
        });

        var handler = new TestMessageHandler();
        services.AddSingleton<TestMessageHandler>(handler);
        
        services.AddSingleton<IMessageHandler>(sp => sp.GetRequiredService<TestMessageHandler>());

        var provider = services.BuildServiceProvider();

        var endpoint = new PeerEndpoint("localhost", port);
        var peerId = new PeerId(Guid.NewGuid());

        return new TestNode(
            provider,
            peerId,
            endpoint,
            handler,
            provider.GetRequiredService<IHostedService>(),
            provider.GetRequiredService<IGossipProtocol>(),
            provider.GetRequiredService<IPeerRegistry>()
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