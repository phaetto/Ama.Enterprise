namespace Ama.Enterprise.P2p.WebRTC.AzureEventGrid.IntegrationTests.Services;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.AzureEventGrid.Extensions;
using Ama.Enterprise.P2p.WebRTC.Extensions;
using Ama.Enterprise.P2p.WebRTC.Models;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;

public sealed class MqttSignalingIntegrationTests
{
    private sealed record MqttLocalSettings(string Host, int Port, bool UseTls, string Username, string Password);

    private readonly ITestOutputHelper testOutputHelper;

    public MqttSignalingIntegrationTests(ITestOutputHelper testOutputHelper)
    {
        ArgumentNullException.ThrowIfNull(testOutputHelper);
        this.testOutputHelper = testOutputHelper;
    }

    [IntegrationFact]
    public async Task MqttSignaling_5Nodes_ConnectAndExchangeMessages_Succeeds()
    {
        // Arrange
        var meshId = "mqtt-integration-mesh";
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));

        var mqttConfig = await LoadMqttConfigAsync(cts.Token).ConfigureAwait(false);
        mqttConfig.ShouldNotBeNull("Failed to load MQTT configuration from local.settings.json");

        var nodeCount = 5;
        var nodes = new List<MqttWebRtcTestNode>();

        try
        {
            testOutputHelper.WriteLine("Initializing 5 DI Nodes...");
            for (var i = 0; i < nodeCount; i++)
            {
                var peerId = new PeerId(Guid.NewGuid());
                var node = CreateTestNode(meshId, peerId, mqttConfig!);
                nodes.Add(node);
            }

            testOutputHelper.WriteLine("Starting MQTT background signaling services...");
            foreach (var node in nodes)
            {
                await node.StartServicesAsync(cts.Token).ConfigureAwait(false);
            }

            // Act - Wait for all 5 nodes to fully discover each other via targeted decentralized WebRTC (Registry populated)
            testOutputHelper.WriteLine("Waiting for WebRTC data channels to open and peer discovery via presence-driven MQTT out-of-band signaling...");
            
            var allConnected = false;
            for (var i = 0; i < 30; i++)
            {
                var fullyConnectedNodes = 0;
                foreach (var node in nodes)
                {
                    var peers = await node.Registry.GetAllPeersAsync(meshId, cts.Token).ConfigureAwait(false);
                    // Each node should see the other 4 nodes
                    if (peers.Count() >= nodeCount - 1)
                    {
                        fullyConnectedNodes++;
                    }
                }

                if (fullyConnectedNodes == nodeCount)
                {
                    allConnected = true;
                    testOutputHelper.WriteLine("All nodes fully interconnected.");
                    break;
                }

                await Task.Delay(2000, cts.Token).ConfigureAwait(false);
            }

            allConnected.ShouldBeTrue("Nodes did not fully interconnect within the allocated time.");

            // Act - Setup Listeners explicitly mapping asynchronous messages
            var messageCompletionSources = nodes.Skip(1).Select(_ => new TaskCompletionSource<GossipMessage>()).ToList();
            for (var i = 1; i < nodes.Count; i++)
            {
                var targetNode = nodes[i];
                var sourceIndex = i - 1; 

                await targetNode.Listener.StartListeningAsync(msg =>
                {
                    if (msg is GossipMessage gossipMsg)
                    {
                        messageCompletionSources[sourceIndex].TrySetResult(gossipMsg);
                    }
                    return Task.CompletedTask;
                }, cts.Token).ConfigureAwait(false);
            }

            // Let listener bindings evaluate internal handlers
            await Task.Delay(TimeSpan.FromSeconds(2), cts.Token).ConfigureAwait(false);

            // Act - Node 0 sends targeted messages to all connected endpoints
            testOutputHelper.WriteLine("Sending targeted messages from Node 0 to all connected WebRTC endpoints...");
            var senderNode = nodes[0];
            var payloadBytes = System.Text.Encoding.UTF8.GetBytes("Decentralized MQTT Signaling Success");
            var messageToSend = new GossipMessage(meshId, Guid.NewGuid(), senderNode.Id, 10, payloadBytes);

            var connectedPeers = await senderNode.Registry.GetAllPeersAsync(meshId, cts.Token).ConfigureAwait(false);
            foreach (var peer in connectedPeers)
            {
                var endpoint = (WebRtcPeerEndpoint)peer.Endpoint;
                if (endpoint != null)
                {
                    await senderNode.Transport.SendAsync(endpoint, messageToSend, cts.Token).ConfigureAwait(false);
                }
            }

            // Assert
            testOutputHelper.WriteLine("Awaiting message handle blocks on all receiving nodes...");
            foreach (var tcs in messageCompletionSources)
            {
                var receivedMessage = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token).ConfigureAwait(false);
                var receivedText = System.Text.Encoding.UTF8.GetString(receivedMessage.Payload.ToArray());
                receivedText.ShouldBe("Decentralized MQTT Signaling Success");
            }

            testOutputHelper.WriteLine("Test completed successfully.");
        }
        finally
        {
            testOutputHelper.WriteLine("Cleaning up generic contexts...");
            foreach (var node in nodes)
            {
                await node.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private async Task<MqttLocalSettings?> LoadMqttConfigAsync(CancellationToken cancellationToken)
    {
        var settingsPath = "local.settings.json";
        if (!File.Exists(settingsPath))
        {
            return null;
        }

        var content = await File.ReadAllTextAsync(settingsPath, cancellationToken).ConfigureAwait(false);
        using var document = JsonDocument.Parse(content);
        
        if (document.RootElement.TryGetProperty("MqttSignaling", out var mqttElement))
        {
            return JsonSerializer.Deserialize<MqttLocalSettings>(
                mqttElement.GetRawText(), 
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        }

        return null;
    }

    private MqttWebRtcTestNode CreateTestNode(string meshId, PeerId peerId, MqttLocalSettings mqttConfig)
    {
        var services = new ServiceCollection();

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
            .AddGossipNetwork()
            .AddWebRtcTransport(options =>
            {
                options.IceServers = Array.Empty<string>();
                options.IceGatheringTimeout = TimeSpan.FromSeconds(2);
            })
            .AddMqttWebRtcSignaling(options =>
            {
                options.Host = mqttConfig.Host;
                options.Port = mqttConfig.Port;
                options.UseTls = mqttConfig.UseTls;
                options.Username = mqttConfig.Username ?? string.Empty;
                options.Password = mqttConfig.Password ?? string.Empty;
                options.ClientId = $"test-node-{peerId.Value:N}";
                options.OfferInterval = TimeSpan.FromSeconds(3);
                options.OfferExpiration = TimeSpan.FromSeconds(15);
            });

        var provider = services.BuildServiceProvider();

        return new MqttWebRtcTestNode(
            provider,
            peerId,
            provider.GetRequiredKeyedService<ITransport>(meshId),
            provider.GetRequiredKeyedService<ITransportListener>(meshId),
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private sealed record MqttWebRtcTestNode(
        ServiceProvider Provider,
        PeerId Id,
        ITransport Transport,
        ITransportListener Listener,
        IPeerRegistry Registry) : IAsyncDisposable
    {
        public async Task StartServicesAsync(CancellationToken cancellationToken)
        {
            var hostedServices = Provider.GetServices<IHostedService>();
            foreach (var hs in hostedServices)
            {
                await hs.StartAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public async ValueTask DisposeAsync()
        {
            var hostedServices = Provider.GetServices<IHostedService>();
            foreach (var hs in hostedServices.Reverse())
            {
                await hs.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }

            await Provider.DisposeAsync().ConfigureAwait(false);
        }
    }
}