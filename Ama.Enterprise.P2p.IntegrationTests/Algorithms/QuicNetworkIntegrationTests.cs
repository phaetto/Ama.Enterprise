namespace Ama.Enterprise.P2p.IntegrationTests.Algorithms;

using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Models;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Handlers;
using Ama.Enterprise.P2p.Models.Algorithms;
using Ama.Enterprise.Project.Tests.Common.Networking;
using Ama.Enterprise.Project.Tests.Common.Extensions;
using Ama.Enterprise.Project.Tests.Common.Attributes;

/// <summary>
/// Contains complex integration tests validating actual QUIC TLS 1.3 binding, multiplexed protocol cycles, and payload distributions.
/// </summary>
public sealed class QuicNetworkIntegrationTests : IClassFixture<NetworkResourceManager>, IDisposable
{
    private readonly ITestOutputHelper testOutputHelper;
    private readonly NetworkResourceManager resourceManager;
    private readonly X509Certificate2 testCertificate;
    private const string TestMeshId = "QuicIntegrationMesh";

    public QuicNetworkIntegrationTests(ITestOutputHelper testOutputHelper, NetworkResourceManager resourceManager)
    {
        this.testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
        this.resourceManager = resourceManager ?? throw new ArgumentNullException(nameof(resourceManager));
        this.testCertificate = GenerateSelfSignedCertificate();
    }

    [IntegrationFact]
    public async Task Network_ShouldPropagateMessage_ToAllConnectedNodes_ViaQuic()
    {
        if (!QuicListener.IsSupported)
        {
            testOutputHelper.WriteLine("QUIC is not supported natively on this platform. Skipping integration test safely.");
            return;
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();
        var portC = resourceManager.GetNextPort();

        // Create 3 independent nodes running locally on different dynamically assigned QUIC TLS endpoints
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

        var payload = Encoding.UTF8.GetBytes("QuicIntegrationTestPayload123");
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeA.Handler.ReceivedMessages.Count.ShouldBe(1);
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(1);
        nodeC.Handler.ReceivedMessages.Count.ShouldBe(1);

        var messageB = nodeB.Handler.ReceivedMessages.First();
        Encoding.UTF8.GetString(messageB.Payload).ShouldBe("QuicIntegrationTestPayload123");
    }

    [IntegrationFact]
    public async Task Network_ShouldDeduplicate_AlreadySeenMessages_ViaQuic()
    {
        if (!QuicListener.IsSupported)
        {
            testOutputHelper.WriteLine("QUIC is not supported natively on this platform. Skipping integration test safely.");
            return;
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        await using var nodeA = CreateTestNode(portA);
        await using var nodeB = CreateTestNode(portB);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("QuicDeduplicationTest");
        
        var messageId = Guid.NewGuid();
        var gossipMessage = new GossipMessage(TestMeshId, messageId, nodeA.Id, 5, payload);

        var encoder = nodeA.Provider.GetRequiredKeyedService<IMeshWireEncoder>(TestMeshId);
        var payloadBytes = encoder.Encode(gossipMessage);
        
        var lengthBytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, payloadBytes.Length);

        var clientOptions = new QuicClientConnectionOptions
        {
            RemoteEndPoint = new DnsEndPoint("127.0.0.1", portB),
            DefaultStreamErrorCode = 0x01,
            DefaultCloseErrorCode = 0x02,
            ClientAuthenticationOptions = new SslClientAuthenticationOptions
            {
                ApplicationProtocols = new List<SslApplicationProtocol> { new SslApplicationProtocol("ama-p2p-quic") },
                RemoteCertificateValidationCallback = (_, _, _, _) => true // Trust self-signed explicitly for testing
            }
        };

        // First manual QUIC multiplexed push
        await using var connection1 = await QuicConnection.ConnectAsync(clientOptions, cts.Token);
        await using var stream1 = await connection1.OpenOutboundStreamAsync(QuicStreamType.Unidirectional, cts.Token);
        await stream1.WriteAsync(lengthBytes, cts.Token);
        await stream1.WriteAsync(payloadBytes, cts.Token);
        stream1.CompleteWrites();

        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        // Second manual QUIC multiplexed push (echo duplication directly pushing the exact same message UUID envelope)
        await using var connection2 = await QuicConnection.ConnectAsync(clientOptions, cts.Token);
        await using var stream2 = await connection2.OpenOutboundStreamAsync(QuicStreamType.Unidirectional, cts.Token);
        await stream2.WriteAsync(lengthBytes, cts.Token);
        await stream2.WriteAsync(payloadBytes, cts.Token);
        stream2.CompleteWrites();

        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        // Assert Node B handled deduplication within IP2pProtocol natively avoiding parallel streams overlapping
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(1);
    }

    [IntegrationFact]
    public async Task Network_ShouldAdapt_WhenNodesJoinAndDrop_ViaQuic()
    {
        if (!QuicListener.IsSupported)
        {
            testOutputHelper.WriteLine("QUIC is not supported natively on this platform. Skipping integration test safely.");
            return;
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();
        var portC = resourceManager.GetNextPort();
        var portD = resourceManager.GetNextPort();

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
        var payload1 = Encoding.UTF8.GetBytes("Message_Phase1_Quic");
        await nodeA.Protocol.BroadcastAsync(payload1, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        HasPayload(nodeA, "Message_Phase1_Quic").ShouldBeTrue();
        HasPayload(nodeB, "Message_Phase1_Quic").ShouldBeTrue();
        HasPayload(nodeC, "Message_Phase1_Quic").ShouldBeTrue();

        // Phase 2: Node C crashes/drops out structurally
        await nodeC.HostedService.StopAsync(cts.Token);
        await nodeA.Registry.RemovePeerAsync(TestMeshId, nodeC.Id, cts.Token);
        await nodeB.Registry.RemovePeerAsync(TestMeshId, nodeC.Id, cts.Token);

        // Phase 3: Node D joins the network mid-flight seamlessly
        await using var nodeD = CreateTestNode(portD);
        
        await RegisterPeerAsync(nodeA, nodeD, cts.Token);
        await RegisterPeerAsync(nodeB, nodeD, cts.Token);
        await RegisterPeerAsync(nodeD, nodeA, cts.Token);
        await RegisterPeerAsync(nodeD, nodeB, cts.Token);
        
        await nodeD.HostedService.StartAsync(cts.Token);

        // Send Message 2 (From B)
        var payload2 = Encoding.UTF8.GetBytes("Message_Phase3_Quic");
        await nodeB.Protocol.BroadcastAsync(payload2, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Send Message 3 (From new Node D)
        var payload3 = Encoding.UTF8.GetBytes("Message_Phase3_FromD_Quic");
        await nodeD.Protocol.BroadcastAsync(payload3, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeA.Handler.ReceivedMessages.Count.ShouldBe(3);
        HasPayload(nodeA, "Message_Phase3_Quic").ShouldBeTrue();
        HasPayload(nodeA, "Message_Phase3_FromD_Quic").ShouldBeTrue();

        nodeB.Handler.ReceivedMessages.Count.ShouldBe(3);
        HasPayload(nodeB, "Message_Phase3_Quic").ShouldBeTrue();
        HasPayload(nodeB, "Message_Phase3_FromD_Quic").ShouldBeTrue();

        nodeC.Handler.ReceivedMessages.Count.ShouldBe(1);
        HasPayload(nodeC, "Message_Phase3_Quic").ShouldBeFalse();

        nodeD.Handler.ReceivedMessages.Count.ShouldBe(2);
        HasPayload(nodeD, "Message_Phase1_Quic").ShouldBeFalse(); // Missed phase 1
        HasPayload(nodeD, "Message_Phase3_Quic").ShouldBeTrue();
        HasPayload(nodeD, "Message_Phase3_FromD_Quic").ShouldBeTrue();
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
            .AddQuicTransport(options =>
            {
                options.ListenHost = "127.0.0.1";
                options.ListenPort = port;
                options.ServerCertificate = testCertificate;
                options.RemoteCertificateValidationCallback = (_, _, _, _) => true; // Bypass validation strictly for tests
            });

        var handler = new TestMessageHandler();
        services.AddSingleton(handler);
        
        services.AddKeyedSingleton<IApplicationPayloadHandler>(TestMeshId, (sp, key) => sp.GetRequiredService<TestMessageHandler>());

        var provider = services.BuildServiceProvider();

        var endpoint = new QuicPeerEndpoint("127.0.0.1", port);

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

    private static X509Certificate2 GenerateSelfSignedCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, false));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2") }, false));

        var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        
        // Exporting and re-importing fixes the private key association to ensure it binds natively on all OS platforms properly for libmsquic
        return new X509Certificate2(cert.Export(X509ContentType.Pfx)); 
    }

    public void Dispose()
    {
        testCertificate.Dispose();
    }
}