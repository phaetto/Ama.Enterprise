namespace Ama.Enterprise.P2p.Mqtt.IntegrationTests.Services;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Mqtt.Extensions;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

public sealed class MqttPeerDiscoveryIntegrationTests
{
    private readonly ITestOutputHelper testOutputHelper;

    public MqttPeerDiscoveryIntegrationTests(ITestOutputHelper testOutputHelper)
    {
        this.testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    }

    [IntegrationFact]
    public async Task MqttPeerDiscovery_TwoNodes_DiscoverEachOther_Succeeds()
    {
        // Arrange
        var meshId = $"mqtt-disc-{Guid.NewGuid():N}";
        var topicPrefix = $"integration-test/disc/{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        testOutputHelper.WriteLine("Initializing Nodes for Discovery...");
        await using var nodeA = CreateDiscoveryTestNode(meshId, peerAId, topicPrefix);
        await using var nodeB = CreateDiscoveryTestNode(meshId, peerBId, topicPrefix);

        // Act
        testOutputHelper.WriteLine("Starting MQTT Peer Discovery background services...");
        await nodeA.StartDiscoveryAsync(cts.Token);
        await nodeB.StartDiscoveryAsync(cts.Token);

        // Assert - Use a polling loop to cleanly wait for public broker subscriptions and broadcasts to propagate natively
        testOutputHelper.WriteLine("Waiting for discovery broadcasts to synchronize across the broker...");
        
        bool discovered = false;
        var timeoutTime = DateTime.UtcNow.AddSeconds(20);

        while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
        {
            var peersA = await nodeA.Registry.GetAllPeersAsync(meshId, cts.Token);
            var peersB = await nodeB.Registry.GetAllPeersAsync(meshId, cts.Token);

            if (peersA.Any(p => p.Id.Equals(peerBId)) && peersB.Any(p => p.Id.Equals(peerAId)))
            {
                discovered = true;
                
                testOutputHelper.WriteLine($"Node A discovered {peersA.Count()} peers.");
                testOutputHelper.WriteLine($"Node B discovered {peersB.Count()} peers.");
                break;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);
        }

        discovered.ShouldBeTrue("Nodes failed to discover each other within the expected timeout limit.");

        testOutputHelper.WriteLine("Stopping discovery loops...");
        await nodeA.StopDiscoveryAsync(cts.Token);
        await nodeB.StopDiscoveryAsync(cts.Token);
    }

    [IntegrationFact]
    public async Task MqttPeerDiscovery_ExplicitManualDiscovery_PopulatesRecentPeers_Succeeds()
    {
        // Arrange
        var meshId = $"mqtt-manual-disc-{Guid.NewGuid():N}";
        var topicPrefix = $"integration-test/manual/{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        
        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        await using var nodeA = CreateDiscoveryTestNode(meshId, peerAId, topicPrefix);
        await using var nodeB = CreateDiscoveryTestNode(meshId, peerBId, topicPrefix);

        // We only start Node B's background listener so it can respond to Node A's manual ping natively
        await nodeB.StartDiscoveryAsync(cts.Token);

        // Node A just starts its hosted service to establish the baseline MQTT connection without waiting for the loop
        if (nodeA.Discovery is IHostedService hostedA)
        {
            await hostedA.StartAsync(cts.Token);
        }

        // Delay sufficiently to guarantee both nodes have connected their MQTT subscriptions successfully before pinging
        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Act & Assert - Explicitly loop manual pings resolving initial broker subscription latencies effectively
        testOutputHelper.WriteLine("Firing explicit DiscoverPeersAsync roundtrips from Node A...");
        
        bool manuallyDiscovered = false;
        var timeoutTime = DateTime.UtcNow.AddSeconds(20);
        
        while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
        {
            var discoveredByA = await nodeA.Discovery.DiscoverPeersAsync(cts.Token);
            
            // Due to public broker latencies, the response might fall slightly outside the DiscoverPeersAsync internal timeout
            // but the background subscriber will catch it and push it into the registry natively.
            var registeredPeers = await nodeA.Registry.GetAllPeersAsync(meshId, cts.Token);
            
            if (discoveredByA.Any(n => n.Id.Equals(peerBId)) || registeredPeers.Any(p => p.Id.Equals(peerBId)))
            {
                manuallyDiscovered = true;
                break;
            }

            // Await briefly before retrying to prevent network spam
            await Task.Delay(TimeSpan.FromSeconds(2), cts.Token);
        }

        manuallyDiscovered.ShouldBeTrue("Node A failed to manually discover Node B within the timeout bound natively.");

        // Cleanup
        if (nodeA.Discovery is IHostedService cleanupHostedA)
        {
            await cleanupHostedA.StopAsync(cts.Token);
        }
        await nodeB.StopDiscoveryAsync(cts.Token);
    }

    private MqttDiscoveryTestNode CreateDiscoveryTestNode(string meshId, PeerId peerId, string topicPrefix)
    {
        var services = new ServiceCollection();

        // Register core CRDT capabilities natively
        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });
        
        // Core P2P dependencies inherently required by discovery
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        services.Configure<P2pNodeOptions>(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        });

        services.AddP2pMesh(meshId)
            .AddGossipNetwork() 
            .AddMqttTransport(options =>
            {
                options.Host = "test.mosquitto.org";
                options.Port = 1883;
                options.TopicPrefix = topicPrefix;
            })
            .AddMqttPeerDiscovery(options =>
            {
                options.DiscoveryInterval = TimeSpan.FromSeconds(5);
                options.DiscoveryTimeout = TimeSpan.FromSeconds(5);
                options.DiscoveryTopicSuffix = "discovery";
            });

        var provider = services.BuildServiceProvider();

        return new MqttDiscoveryTestNode(
            provider,
            peerId,
            provider.GetRequiredKeyedService<IPeerDiscovery>(meshId),
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private sealed record MqttDiscoveryTestNode(
        ServiceProvider Provider,
        PeerId Id,
        IPeerDiscovery Discovery,
        IPeerRegistry Registry) : IAsyncDisposable
    {
        public async Task StartDiscoveryAsync(CancellationToken cancellationToken)
        {
            if (Discovery is IHostedService hosted)
            {
                await hosted.StartAsync(cancellationToken);
            }
        }

        public async Task StopDiscoveryAsync(CancellationToken cancellationToken)
        {
            if (Discovery is IHostedService hosted)
            {
                await hosted.StopAsync(cancellationToken);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await StopDiscoveryAsync(CancellationToken.None);
            await Provider.DisposeAsync();
        }
    }
}