namespace Ama.Enterprise.P2p.IntegrationTests.Discovery;

using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Discovery;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Discovery;
using Ama.Enterprise.UnitTests.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Shouldly;

/// <summary>
/// Integration tests validating DNS peer discovery effectively resolving remote records and orchestrating active network handshakes.
/// </summary>
public sealed class DnsPeerDiscoveryIntegrationTests
{
    private const string TestMeshId = "DnsIntegrationMesh";
    private const string TestDomain = "integration-tests.internal.amasoft.dev";
    private const int TestPort = 8200;

    [IntegrationFact]
    public async Task DiscoverPeersAsync_ShouldResolveDomainAndHandshakeWithExpectedIps()
    {
        // Arrange
        var dnsOptions = new DnsDiscoveryOptions
        {
            Hostname = TestDomain,
            TargetPort = TestPort,
            HandshakeTimeout = TimeSpan.FromSeconds(2),
            UseSrvRecords = false
        };

        var nodeOptions = new P2pNodeOptions
        {
            LocalPeerId = Guid.NewGuid()
        };

        var mockDnsOptionsMonitor = new Mock<IOptionsMonitor<DnsDiscoveryOptions>>();
        mockDnsOptionsMonitor.Setup(m => m.Get(TestMeshId)).Returns(dnsOptions);

        var mockNodeOptionsMonitor = new Mock<IOptionsMonitor<P2pNodeOptions>>();
        mockNodeOptionsMonitor.Setup(m => m.Get(TestMeshId)).Returns(nodeOptions);

        var localEndpoint = new TcpPeerEndpoint("127.0.0.1", 8080);

        var mockHandshaker = new Mock<IPeerHandshaker>();
        var mockAuthenticator = new Mock<IPeerAuthenticator>();
        var mockFailureDetector = new Mock<IFailureDetector>();
        var mockPeerRegistry = new Mock<IPeerRegistry>();

        mockAuthenticator
            .Setup(a => a.AuthenticateAsync(It.IsAny<PeerNode>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        mockHandshaker
            .Setup(h => h.HandshakeAsync(It.IsAny<PeerNode>(), It.IsAny<IPEndPoint>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((PeerNode local, EndPoint endpoint, CancellationToken ct) =>
            {
                if (endpoint is IPEndPoint ipEndpoint)
                {
                    var remoteId = new PeerId(Guid.NewGuid());
                    var remoteEndpoint = new TcpPeerEndpoint(ipEndpoint.Address.ToString(), ipEndpoint.Port);
                    return new PeerNode(remoteId, remoteEndpoint);
                }
                return null;
            });

        using var discoveryService = new DnsPeerDiscovery(
            TestMeshId,
            mockDnsOptionsMonitor.Object,
            mockNodeOptionsMonitor.Object,
            localEndpoint,
            mockHandshaker.Object,
            NullLogger<DnsPeerDiscovery>.Instance,
            mockPeerRegistry.Object,
            mockAuthenticator.Object,
            mockFailureDetector.Object
        );

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        // Act
        var discoveredPeers = await discoveryService.DiscoverPeersAsync(cts.Token);

        // Assert
        var peersList = discoveredPeers.ToList();
        peersList.ShouldNotBeEmpty();

        var expectedIps = new[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" };

        foreach (var expectedIp in expectedIps)
        {
            mockHandshaker.Verify(
                h => h.HandshakeAsync(
                    It.IsAny<PeerNode>(),
                    It.Is<IPEndPoint>(ep => ep is IPEndPoint && ep.Address.ToString() == expectedIp && ep.Port == TestPort),
                    It.IsAny<CancellationToken>()),
                Times.Once,
                $"Expected handshake with {expectedIp}:{TestPort}");

            peersList.ShouldContain(p => p.Endpoint is TcpPeerEndpoint && ((TcpPeerEndpoint)p.Endpoint).Host == expectedIp && ((TcpPeerEndpoint)p.Endpoint).Port == TestPort);
        }
    }
}