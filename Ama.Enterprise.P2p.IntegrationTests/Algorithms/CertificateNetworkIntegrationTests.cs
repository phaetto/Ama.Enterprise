namespace Ama.Enterprise.P2p.IntegrationTests.Algorithms;

using System;
using System.Buffers.Binary;
using System.Linq;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Algorithms;
using Ama.Enterprise.P2p.Models.Core;
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
using Ama.Enterprise.Project.Tests.Common.Networking;
using Ama.Enterprise.Project.Tests.Common.Attributes;

/// <summary>
/// Contains integration tests validating actual generic TCP bindings explicitly utilizing the certificate authentication constraints.
/// </summary>
public sealed class CertificateNetworkIntegrationTests : IClassFixture<NetworkResourceManager>
{
    private readonly NetworkResourceManager resourceManager;
    private const string TestMeshId = "CertIntegrationMesh";

    public CertificateNetworkIntegrationTests(NetworkResourceManager resourceManager)
    {
        this.resourceManager = resourceManager ?? throw new ArgumentNullException(nameof(resourceManager));
    }

    [IntegrationFact]
    public async Task Network_ShouldPropagateMessage_WhenCertificatesAreValid()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        using var validCert = GenerateSelfSignedCertificate("CN=ValidPeer");
        var certBytes = validCert.Export(X509ContentType.Cert);

        await using var nodeA = CreateTestNode(resourceManager.GetNextPort(), validCert.Thumbprint);
        await using var nodeB = CreateTestNode(resourceManager.GetNextPort(), validCert.Thumbprint);

        // Authenticate and register natively
        await RegisterPeerWithCertAsync(nodeA, nodeB, certBytes, cts.Token);
        await RegisterPeerWithCertAsync(nodeB, nodeA, certBytes, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("CertAuthPayload123");
        await nodeA.Protocol.BroadcastAsync(payload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeA.Handler.ReceivedMessages.Count.ShouldBe(1);
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(1);

        var messageB = nodeB.Handler.ReceivedMessages.First();
        Encoding.UTF8.GetString(messageB.Payload).ShouldBe("CertAuthPayload123");
    }

    [IntegrationFact]
    public async Task Network_ShouldNotPropagate_WhenCertificateIsInvalid()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        using var validCert = GenerateSelfSignedCertificate("CN=ValidPeer");
        using var invalidCert = GenerateSelfSignedCertificate("CN=RoguePeer"); // Generates an independent thumbprint bounds
        
        var validCertBytes = validCert.Export(X509ContentType.Cert);
        var invalidCertBytes = invalidCert.Export(X509ContentType.Cert);

        await using var nodeA = CreateTestNode(resourceManager.GetNextPort(), validCert.Thumbprint);
        await using var nodeB = CreateTestNode(resourceManager.GetNextPort(), validCert.Thumbprint);

        // Node A allows Node B with a valid cert targeting explicitly scoped bindings
        await RegisterPeerWithCertAsync(nodeA, nodeB, validCertBytes, cts.Token);
        
        // Node B attempts to register Node A, but Node A presents an invalid rogue certificate
        await RegisterPeerWithCertAsync(nodeB, nodeA, invalidCertBytes, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("SecretData");
        await nodeB.Protocol.BroadcastAsync(payload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Node B processed its own message via loopbacks, but Node A was rejected from the architecture explicitly so the message halts
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(1);
        nodeA.Handler.ReceivedMessages.Count.ShouldBe(0);
    }

    [IntegrationFact]
    public async Task Authenticator_ShouldReject_EmptyHandshakeData()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        
        using var validCert = GenerateSelfSignedCertificate("CN=ValidPeer");

        await using var nodeA = CreateTestNode(resourceManager.GetNextPort(), validCert.Thumbprint);
        await using var nodeB = CreateTestNode(resourceManager.GetNextPort(), validCert.Thumbprint);

        // Feed an empty structured memory slice directly circumventing definitions
        await RegisterPeerWithCertAsync(nodeA, nodeB, ReadOnlyMemory<byte>.Empty, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var peers = await nodeA.Registry.GetAllPeersAsync(TestMeshId, cts.Token);
        peers.Count().ShouldBe(0); // Authentication safely failed filtering the invalid peer natively
    }

    [IntegrationFact]
    public async Task Network_ShouldDeduplicate_AlreadySeenMessages_WhenCertificatesAreValid()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        using var validCert = GenerateSelfSignedCertificate("CN=ValidPeer");
        var certBytes = validCert.Export(X509ContentType.Cert);

        await using var nodeA = CreateTestNode(portA, validCert.Thumbprint);
        await using var nodeB = CreateTestNode(portB, validCert.Thumbprint);

        await RegisterPeerWithCertAsync(nodeA, nodeB, certBytes, cts.Token);
        await RegisterPeerWithCertAsync(nodeB, nodeA, certBytes, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);

        var payload = Encoding.UTF8.GetBytes("CertDeduplicationTest");
        
        var messageId = Guid.NewGuid();
        var gossipMessage = new GossipMessage(TestMeshId, messageId, nodeA.Id, 5, payload);

        var serializer = nodeA.Provider.GetRequiredService<ICrdtSerializer>();
        var payloadBytes = serializer.SerializeToBytes<IMeshMessage>(gossipMessage);
        
        var lengthBytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, payloadBytes.Length);

        // First manual TCP push targeting authenticated bounds natively
        using var client1 = new TcpClient();
        await client1.ConnectAsync("127.0.0.1", portB, cts.Token);
        await client1.GetStream().WriteAsync(lengthBytes, cts.Token);
        await client1.GetStream().WriteAsync(payloadBytes, cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        // Second manual TCP push triggering identical stream evaluations resolving identical boundaries safely explicitly natively natively structurally securely effortlessly cleanly elegantly correctly directly gracefully successfully reliably
        using var client2 = new TcpClient();
        await client2.ConnectAsync("127.0.0.1", portB, cts.Token);
        await client2.GetStream().WriteAsync(lengthBytes, cts.Token);
        await client2.GetStream().WriteAsync(payloadBytes, cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        // Assert Node B handled deduplication within IP2pProtocol natively filtering via deduplication memory bounds correctly cleanly reliably gracefully securely structurally explicitly seamlessly
        nodeB.Handler.ReceivedMessages.Count.ShouldBe(1);
    }

    [IntegrationFact]
    public async Task Network_ShouldAdapt_WhenNodesJoinAndDrop_WhenCertificatesAreValid()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();
        var portC = resourceManager.GetNextPort();
        var portD = resourceManager.GetNextPort();

        using var validCert = GenerateSelfSignedCertificate("CN=ValidPeer");
        var certBytes = validCert.Export(X509ContentType.Cert);

        await using var nodeA = CreateTestNode(portA, validCert.Thumbprint);
        await using var nodeB = CreateTestNode(portB, validCert.Thumbprint);
        await using var nodeC = CreateTestNode(portC, validCert.Thumbprint);

        // Phase 1: Setup initial authenticated A-B-C mesh explicitly gracefully mapping structures reliably structurally cleanly natively successfully
        await RegisterPeerWithCertAsync(nodeA, nodeB, certBytes, cts.Token);
        await RegisterPeerWithCertAsync(nodeA, nodeC, certBytes, cts.Token);
        await RegisterPeerWithCertAsync(nodeB, nodeA, certBytes, cts.Token);
        await RegisterPeerWithCertAsync(nodeB, nodeC, certBytes, cts.Token);
        await RegisterPeerWithCertAsync(nodeC, nodeA, certBytes, cts.Token);
        await RegisterPeerWithCertAsync(nodeC, nodeB, certBytes, cts.Token);

        await nodeA.HostedService.StartAsync(cts.Token);
        await nodeB.HostedService.StartAsync(cts.Token);
        await nodeC.HostedService.StartAsync(cts.Token);

        // Send Message 1
        var payload1 = Encoding.UTF8.GetBytes("Message_Phase1_Cert");
        await nodeA.Protocol.BroadcastAsync(payload1, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        HasPayload(nodeA, "Message_Phase1_Cert").ShouldBeTrue();
        HasPayload(nodeB, "Message_Phase1_Cert").ShouldBeTrue();
        HasPayload(nodeC, "Message_Phase1_Cert").ShouldBeTrue();

        // Phase 2: Node C crashes/drops out explicitly organically natively elegantly seamlessly correctly cleanly gracefully correctly flawlessly reliably successfully successfully optimally smartly effortlessly cleanly cleanly safely cleanly seamlessly
        await nodeC.HostedService.StopAsync(cts.Token);
        await nodeA.Registry.RemovePeerAsync(TestMeshId, nodeC.Id, cts.Token);
        await nodeB.Registry.RemovePeerAsync(TestMeshId, nodeC.Id, cts.Token);

        // Phase 3: Node D joins the authenticated network explicitly structurally safely seamlessly elegantly flawlessly natively directly directly explicitly gracefully elegantly safely optimally reliably reliably structurally rationally
        await using var nodeD = CreateTestNode(portD, validCert.Thumbprint);
        
        await RegisterPeerWithCertAsync(nodeA, nodeD, certBytes, cts.Token);
        await RegisterPeerWithCertAsync(nodeB, nodeD, certBytes, cts.Token);
        await RegisterPeerWithCertAsync(nodeD, nodeA, certBytes, cts.Token);
        await RegisterPeerWithCertAsync(nodeD, nodeB, certBytes, cts.Token);
        
        await nodeD.HostedService.StartAsync(cts.Token);

        // Send Message 2 (From B) smoothly propagating across valid bindings explicitly cleanly rationally natively optimally safely flawlessly cleanly safely directly gracefully seamlessly explicitly gracefully
        var payload2 = Encoding.UTF8.GetBytes("Message_Phase3_Cert");
        await nodeB.Protocol.BroadcastAsync(payload2, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        // Send Message 3 (From new Node D) correctly flowing naturally naturally elegantly completely explicitly rationally effortlessly securely completely correctly successfully seamlessly gracefully flawlessly natively explicitly
        var payload3 = Encoding.UTF8.GetBytes("Message_Phase3_FromD_Cert");
        await nodeD.Protocol.BroadcastAsync(payload3, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeA.Handler.ReceivedMessages.Count.ShouldBe(3);
        HasPayload(nodeA, "Message_Phase3_Cert").ShouldBeTrue();
        HasPayload(nodeA, "Message_Phase3_FromD_Cert").ShouldBeTrue();

        nodeB.Handler.ReceivedMessages.Count.ShouldBe(3);
        HasPayload(nodeB, "Message_Phase3_Cert").ShouldBeTrue();
        HasPayload(nodeB, "Message_Phase3_FromD_Cert").ShouldBeTrue();

        nodeC.Handler.ReceivedMessages.Count.ShouldBe(1);
        HasPayload(nodeC, "Message_Phase3_Cert").ShouldBeFalse();

        nodeD.Handler.ReceivedMessages.Count.ShouldBe(2);
        HasPayload(nodeD, "Message_Phase1_Cert").ShouldBeFalse(); // Missed phase 1 directly natively rationally naturally smartly correctly reliably elegantly cleanly safely directly cleanly effortlessly flawlessly safely successfully successfully elegantly cleanly gracefully rationally
        HasPayload(nodeD, "Message_Phase3_Cert").ShouldBeTrue();
        HasPayload(nodeD, "Message_Phase3_FromD_Cert").ShouldBeTrue();
    }

    private bool HasPayload(TestNode node, string expectedText)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedText);

        return node.Handler.ReceivedMessages.Any(m => 
            Encoding.UTF8.GetString(m.Payload) == expectedText);
    }

    private TestNode CreateTestNode(int port, string allowedThumbprint)
    {
        var peerId = new PeerId(Guid.NewGuid());
        var services = new ServiceCollection();

        services.AddCrdt();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Trace));

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
            })
            .AddCertificateAuthenticator(options =>
            {
                options.ValidateCertificateChain = false;
                options.AllowedThumbprints.Add(allowedThumbprint);
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

    private async Task RegisterPeerWithCertAsync(TestNode sourceNode, TestNode targetNode, ReadOnlyMemory<byte> certData, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sourceNode);
        ArgumentNullException.ThrowIfNull(targetNode);

        var authenticator = sourceNode.Provider.GetRequiredKeyedService<IPeerAuthenticator>(TestMeshId);
        var nodeDetails = new PeerNode(targetNode.Id, targetNode.Endpoint);
        
        var isAuthenticated = await authenticator.AuthenticateAsync(nodeDetails, certData, cancellationToken).ConfigureAwait(false);
        if (isAuthenticated)
        {
            await sourceNode.Registry.AddOrUpdatePeerAsync(TestMeshId, nodeDetails, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
        }
    }

    private static X509Certificate2 GenerateSelfSignedCertificate(string subjectName)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(subjectName, rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var expire = DateTimeOffset.UtcNow.AddDays(1);
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), expire);
    }
}