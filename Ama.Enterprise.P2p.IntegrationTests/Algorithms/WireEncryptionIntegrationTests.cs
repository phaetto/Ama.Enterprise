namespace Ama.Enterprise.P2p.IntegrationTests.Algorithms;

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Handlers;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Ama.Enterprise.Project.Tests.Common.Networking;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

/// <summary>
/// Integration tests explicitly validating End-to-End wire encryption utilizing AES-GCM across mapped network bounds securely.
/// </summary>
public sealed class WireEncryptionIntegrationTests : IClassFixture<NetworkResourceManager>
{
    private readonly NetworkResourceManager resourceManager;
    private const string TestMeshId = "EncryptionIntegrationMesh";

    public WireEncryptionIntegrationTests(NetworkResourceManager resourceManager)
    {
        ArgumentNullException.ThrowIfNull(resourceManager);
        this.resourceManager = resourceManager;
    }

    [IntegrationFact]
    public async Task Network_ShouldPropagateMessage_WhenEncryptionKeysMatch()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var sharedKey = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));

        await using var nodeA = CreateTestNode(resourceManager.GetNextPort(), enableEncryption: true, sharedKey);
        await using var nodeB = CreateTestNode(resourceManager.GetNextPort(), enableEncryption: true, sharedKey);

        await RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await RegisterPeerAsync(nodeB, nodeA, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("EncryptedPayload123");
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeA.Handler.ReceivedMessages.Count.ShouldBe(1);
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(1);

        var messageB = nodeB.Handler.ReceivedMessages.First();
        Encoding.UTF8.GetString(messageB.Payload).ShouldBe("EncryptedPayload123");
    }

    [IntegrationFact]
    public async Task Network_ShouldDropMessage_WhenEncryptionKeysMismatch()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var keyA = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
        var keyB = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)); // Different key explicitly bridging failures

        await using var nodeA = CreateTestNode(resourceManager.GetNextPort(), enableEncryption: true, keyA);
        await using var nodeB = CreateTestNode(resourceManager.GetNextPort(), enableEncryption: true, keyB);

        await RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await RegisterPeerAsync(nodeB, nodeA, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("SecretData");
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Node A processed its own local broadcast, but Node B fails decryption natively rejecting structural limits.
        nodeA.Handler.ReceivedMessages.Count.ShouldBe(1);
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(0);
    }

    [IntegrationFact]
    public async Task Network_ShouldDropEncryptedMessage_WhenReceiverHasEncryptionDisabled()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var keyA = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));

        // Node A enforces encryption, Node B runs unencrypted configurations structurally.
        await using var nodeA = CreateTestNode(resourceManager.GetNextPort(), enableEncryption: true, keyA);
        await using var nodeB = CreateTestNode(resourceManager.GetNextPort(), enableEncryption: false, encryptionKey: null);

        await RegisterPeerAsync(nodeA, nodeB, cts.Token);
        await RegisterPeerAsync(nodeB, nodeA, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("StrictlyEncryptedData");
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeA.Handler.ReceivedMessages.Count.ShouldBe(1);
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(0); // Safely drops encrypted format natively.
    }

    private TestNode CreateTestNode(int port, bool enableEncryption, string? encryptionKey)
    {
        var peerId = new PeerId(Guid.NewGuid());
        var services = new ServiceCollection();

        services.AddCrdt();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Trace));

        services.AddP2pMesh(TestMeshId, options =>
            {
                options.LocalPeerId = peerId.Value;
            })
            .AddWireEncoder(options =>
            {
                options.IsEncryptionEnabled = enableEncryption;
                options.EncryptionKeyBase64 = encryptionKey;
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