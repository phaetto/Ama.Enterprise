namespace Ama.Enterprise.P2p.IntegrationTests.Algorithms;

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Networking;
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