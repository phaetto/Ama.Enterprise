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
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Shouldly;
using Xunit;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Models;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Handlers;
using Ama.Enterprise.P2p.Models.Algorithms;

/// <summary>
/// Integration tests verifying the structured Push-Pull Anti-Entropy gossip capabilities natively.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="PushPullGossipIntegrationTests"/> class.
/// </remarks>
public sealed class PushPullGossipIntegrationTests(ITestOutputHelper testOutputHelper, NetworkResourceManager resourceManager) : IClassFixture<NetworkResourceManager>
{
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    private readonly NetworkResourceManager resourceManager = resourceManager ?? throw new ArgumentNullException(nameof(resourceManager));
    private const string TestMeshId = "PushPullIntegrationMesh";

    [IntegrationFact]
    public async Task Network_ShouldRecoverMissingMessages_ViaPushPullAntiEntropy()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Configure gossip to be effectively disabled, isolating ONLY the push-pull anti-entropy mechanics.
        Action<PushPullGossipOptions> configureOptions = options =>
        {
            options.GossipInterval = TimeSpan.FromHours(1); // Standard broadcast gossip disabled
            options.EnablePushPull = true;
            options.PushPullInterval = TimeSpan.FromSeconds(1); // Push-Pull triggers quickly
            options.Fanout = 1;
            options.MaxDigestSize = 50;
        };

        await using var nodeA = CreateTestNode(resourceManager.GetNextPort(), configureOptions);
        await using var nodeB = CreateTestNode(resourceManager.GetNextPort(), configureOptions);

        await RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await RegisterPeerAsync(nodeB, nodeA, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("AntiEntropyRecoveryMessage");
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        // Wait to allow the Push-Pull digest exchange (Digest -> PullRequest -> Payload Fulfillment) to resolve securely
        await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);

        // Assert that Node B successfully recovered the missing payload through explicit anti-entropy despite pure gossip being stalled
        HasPayload(nodeA, "AntiEntropyRecoveryMessage").ShouldBeTrue();
        HasPayload(nodeB, "AntiEntropyRecoveryMessage").ShouldBeTrue();
    }

    [IntegrationFact]
    public async Task Network_ShouldNotRecoverMissingMessages_WhenPushPullIsDisabled()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Configure gossip to be effectively disabled AND push-pull to be disabled natively.
        Action<PushPullGossipOptions> configureOptions = options =>
        {
            options.GossipInterval = TimeSpan.FromHours(1); // Standard broadcast gossip disabled
            options.EnablePushPull = false; // Push-Pull entirely bypassed
            options.PushPullInterval = TimeSpan.FromSeconds(1);
            options.Fanout = 1;
        };

        await using var nodeA = CreateTestNode(resourceManager.GetNextPort(), configureOptions);
        await using var nodeB = CreateTestNode(resourceManager.GetNextPort(), configureOptions);

        await RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await RegisterPeerAsync(nodeB, nodeA, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("HiddenMessage");
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);

        // Assert that Node B did NOT recover the message since all propagation bounds were halted cleanly
        HasPayload(nodeA, "HiddenMessage").ShouldBeTrue();
        HasPayload(nodeB, "HiddenMessage").ShouldBeFalse();
    }

    [IntegrationFact]
    public async Task Network_ShouldPropagateNormally_ViaStandardGossipTick_WithPushPullProtocol()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Prove the PushPull protocol maintains standard backward-compatible broadcast gossip correctly.
        Action<PushPullGossipOptions> configureOptions = options =>
        {
            options.GossipInterval = TimeSpan.FromMilliseconds(500); // Standard gossip is FAST
            options.EnablePushPull = true;
            options.PushPullInterval = TimeSpan.FromHours(1); // Push-pull is stalled
            options.Fanout = 1;
        };

        await using var nodeA = CreateTestNode(resourceManager.GetNextPort(), configureOptions);
        await using var nodeB = CreateTestNode(resourceManager.GetNextPort(), configureOptions);

        await RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await RegisterPeerAsync(nodeB, nodeA, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("StandardGossipMessage");
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        // Wait to allow the standard gossip tick to forward the envelope completely
        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Assert standard fast-path gossip transmitted accurately natively
        HasPayload(nodeA, "StandardGossipMessage").ShouldBeTrue();
        HasPayload(nodeB, "StandardGossipMessage").ShouldBeTrue();
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

    private TestNode CreateTestNode(int port, Action<PushPullGossipOptions> configureOptions)
    {
        var services = new ServiceCollection();
        var uniqueId = Guid.NewGuid();
        var endpoint = new TcpPeerEndpoint("127.0.0.1", port);
        
        services.Configure<P2pNodeOptions>(TestMeshId, options =>
        {
            options.LocalPeerId = uniqueId;
        });

        services.AddCrdt();
        
        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        services.AddP2pMesh(TestMeshId)
            .AddPushPullGossipNetwork(configureOptions)
            .AddTcpTransport(options =>
            {
                options.ListenHost = "127.0.0.1";
                options.ListenPort = port;
            });

        // Ensure newly mapped keyed interfaces natively resolve bounds explicitly safely
        services.AddKeyedSingleton<IFailureDetector>(TestMeshId, (sp, key) => 
            new TimeBasedFailureDetector((string)key!, sp.GetRequiredService<IOptionsMonitor<FailureDetectorOptions>>(), sp.GetRequiredService<ILogger<TimeBasedFailureDetector>>()));
        
        services.AddKeyedSingleton<IPeerAuthenticator>(TestMeshId, (sp, key) => 
            new PassThroughPeerAuthenticator((string)key!, sp.GetRequiredService<ILogger<PassThroughPeerAuthenticator>>()));

        var handler = new TestMessageHandler();
        services.AddSingleton(handler);
        
        services.AddKeyedSingleton<IApplicationPayloadHandler>(TestMeshId, (sp, key) => sp.GetRequiredService<TestMessageHandler>());

        var provider = services.BuildServiceProvider();

        var peerId = new PeerId(uniqueId);

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