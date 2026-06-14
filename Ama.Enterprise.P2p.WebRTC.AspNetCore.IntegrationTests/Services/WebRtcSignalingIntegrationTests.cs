namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.IntegrationTests.Services;

using System;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.Licensing.Services;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Extensions;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;
using Ama.Enterprise.P2p.WebRTC.Extensions;
using Ama.Enterprise.P2p.WebRTC.Models;
using Ama.Enterprise.P2p.WebRTC.Services;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Ama.Enterprise.Project.Tests.Common.Extensions;
using Ama.Enterprise.Project.Tests.Common.Networking;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

public sealed class WebRtcSignalingIntegrationTests(ITestOutputHelper testOutputHelper, NetworkResourceManager resourceManager) : IClassFixture<NetworkResourceManager>
{
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    private readonly NetworkResourceManager resourceManager = resourceManager ?? throw new ArgumentNullException(nameof(resourceManager));

    [IntegrationFact]
    public async Task WebRtcSignalingClient_StandaloneMode_NegotiateOffer_Succeeds()
    {
        // Arrange
        var meshId = "sig-mesh-req";
        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        await using var nodeA = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA);
        await using var nodeB = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portB);

        var uriB = new Uri($"http://127.0.0.1:{portB}");

        // Act
        testOutputHelper.WriteLine("Node A requesting SDP negotiation natively from Node B over WebSockets...");
        var connectionId = await nodeA.Client.NegotiateOfferAsync(uriB, meshId, null, async (remoteOffer, ct) =>
        {
            testOutputHelper.WriteLine("Node A dynamically answering remote SDP offer...");
            var localAnswer = await nodeA.InvitationService.AcceptInvitationAsync(remoteOffer.SdpOffer, ct).ConfigureAwait(false);
            return new WebRtcInvitationAnswer(remoteOffer.ConnectionId, localAnswer.SdpAnswer);
        }, CancellationToken.None);
        
        // Assert
        connectionId.ShouldNotBeNullOrWhiteSpace();
        testOutputHelper.WriteLine("WebRTC out-of-band signaling exchange negotiated explicitly over WebSockets.");
    }

    [IntegrationFact]
    public async Task WebRtcSignalingClient_StandaloneMode_Https_NegotiateOffer_Succeeds()
    {
        // Arrange
        var meshId = "sig-mesh-req-https";
        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();
        var certInfo = GenerateTempCertificate();

        try
        {
            await using var nodeA = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA, true, certInfo.Path, certInfo.Password);
            await using var nodeB = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portB, true, certInfo.Path, certInfo.Password);

            var uriB = new Uri($"https://127.0.0.1:{portB}");

            // Act
            testOutputHelper.WriteLine("Node A requesting SDP negotiation over HTTPS (WSS) natively from Standalone Node B...");
            var connectionId = await nodeA.Client.NegotiateOfferAsync(uriB, meshId, null, async (remoteOffer, ct) =>
            {
                testOutputHelper.WriteLine("Node A dynamically answering remote SDP offer over WSS...");
                var localAnswer = await nodeA.InvitationService.AcceptInvitationAsync(remoteOffer.SdpOffer, ct).ConfigureAwait(false);
                return new WebRtcInvitationAnswer(remoteOffer.ConnectionId, localAnswer.SdpAnswer);
            }, CancellationToken.None);
            
            // Assert
            connectionId.ShouldNotBeNullOrWhiteSpace();
            testOutputHelper.WriteLine("WebRTC out-of-band signaling exchange securely negotiated explicitly over Secure WebSockets (WSS).");
        }
        finally
        {
            CleanupTempCertificate(certInfo.Path);
        }
    }

    [IntegrationFact]
    public async Task WebRtcSignalingClient_IntegratedMode_NegotiateOffer_Succeeds()
    {
        // Arrange
        var meshId = "sig-mesh-integ-req";
        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        await using var nodeA = await CreateIntegratedTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA);
        await using var nodeB = await CreateIntegratedTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portB);

        var uriB = new Uri($"http://127.0.0.1:{portB}");

        // Act
        testOutputHelper.WriteLine("Node A requesting SDP negotiation explicitly from Integrated Node B over WebSockets...");
        var connectionId = await nodeA.Client.NegotiateOfferAsync(uriB, meshId, null, async (remoteOffer, ct) =>
        {
            testOutputHelper.WriteLine("Node A answering remote Integrated SDP offer natively...");
            var localAnswer = await nodeA.InvitationService.AcceptInvitationAsync(remoteOffer.SdpOffer, ct).ConfigureAwait(false);
            return new WebRtcInvitationAnswer(remoteOffer.ConnectionId, localAnswer.SdpAnswer);
        }, CancellationToken.None);
        
        // Assert
        connectionId.ShouldNotBeNullOrWhiteSpace();
        testOutputHelper.WriteLine("WebRTC out-of-band signaling exchange finalized across integrated WS endpoints.");
    }

    [IntegrationFact]
    public async Task WebRtcSignalingClient_IntegratedMode_Https_NegotiateOffer_Succeeds()
    {
        // Arrange
        var meshId = "sig-mesh-integ-req-https";
        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();
        var certInfo = GenerateTempCertificate();

        try
        {
            await using var nodeA = await CreateIntegratedTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA, true, certInfo.Path, certInfo.Password);
            await using var nodeB = await CreateIntegratedTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portB, true, certInfo.Path, certInfo.Password);

            var uriB = new Uri($"https://127.0.0.1:{portB}");

            // Act
            testOutputHelper.WriteLine("Node A requesting SDP negotiation over HTTPS (WSS) natively from Integrated Node B...");
            var connectionId = await nodeA.Client.NegotiateOfferAsync(uriB, meshId, null, async (remoteOffer, ct) =>
            {
                testOutputHelper.WriteLine("Node A dynamically answering remote SDP offer over WSS...");
                var localAnswer = await nodeA.InvitationService.AcceptInvitationAsync(remoteOffer.SdpOffer, ct).ConfigureAwait(false);
                return new WebRtcInvitationAnswer(remoteOffer.ConnectionId, localAnswer.SdpAnswer);
            }, CancellationToken.None);
            
            // Assert
            connectionId.ShouldNotBeNullOrWhiteSpace();
            testOutputHelper.WriteLine("WebRTC out-of-band signaling exchange securely finalized across integrated WSS endpoints.");
        }
        finally
        {
            CleanupTempCertificate(certInfo.Path);
        }
    }

    [IntegrationFact]
    public async Task WebRtcSignalingClient_OfflinePeer_ReturnsNull()
    {
        // Arrange
        var meshId = "sig-mesh-offline";
        var portA = resourceManager.GetNextPort();
        var offlinePort = resourceManager.GetNextPort();

        await using var nodeA = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA);

        var offlineUri = new Uri($"http://127.0.0.1:{offlinePort}");

        // Act
        testOutputHelper.WriteLine($"Node A requesting explicit WebSocket negotiation from offline endpoint {offlineUri}...");
        var connectionId = await nodeA.Client.NegotiateOfferAsync(offlineUri, meshId, null, (offer, ct) => 
        {
            return Task.FromResult(new WebRtcInvitationAnswer(offer.ConnectionId, "dummy"));
        }, CancellationToken.None);
        
        // Assert
        connectionId.ShouldBeNull();
        testOutputHelper.WriteLine("Client suppressed the WS connection failure structurally.");
    }

    [IntegrationFact]
    public async Task WebRtcHttpPeerDiscovery_StandaloneMode_DiscoversAndConnects_Succeeds()
    {
        // Arrange
        var meshId = "sig-mesh-discovery";
        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        await using var nodeA = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA);
        await using var nodeB = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portB);

        var uriB = new Uri($"http://127.0.0.1:{portB}");

        // Act
        testOutputHelper.WriteLine("Node A running explicit HTTP discovery connecting to Standalone Node B natively...");
        var result = await nodeA.Discovery.DiscoverPeerAsync(uriB, null, CancellationToken.None);

        // Assert
        result.ShouldBeTrue();
        testOutputHelper.WriteLine("WebRTC out-of-band signaling exchange automated explicitly using the WS discovery service.");
    }

    [IntegrationFact]
    public async Task WebRtcHttpPeerDiscovery_StandaloneMode_Https_DiscoversAndConnects_Succeeds()
    {
        // Arrange
        var meshId = "sig-mesh-discovery-https";
        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();
        var certInfo = GenerateTempCertificate();

        try
        {
            await using var nodeA = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA, true, certInfo.Path, certInfo.Password);
            await using var nodeB = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portB, true, certInfo.Path, certInfo.Password);

            var uriB = new Uri($"https://127.0.0.1:{portB}");

            // Act
            testOutputHelper.WriteLine("Node A running explicit HTTPS discovery securely connecting to Standalone Node B natively...");
            var result = await nodeA.Discovery.DiscoverPeerAsync(uriB, null, CancellationToken.None);

            // Assert
            result.ShouldBeTrue();
            testOutputHelper.WriteLine("WebRTC out-of-band signaling exchange automated explicitly using the WSS discovery service.");
        }
        finally
        {
            CleanupTempCertificate(certInfo.Path);
        }
    }

    [IntegrationFact]
    public async Task WebRtcHttpPeerDiscovery_IntegratedMode_DiscoversAndConnects_Succeeds()
    {
        // Arrange
        var meshId = "sig-mesh-discovery-integ";
        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        await using var nodeA = await CreateIntegratedTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA);
        await using var nodeB = await CreateIntegratedTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portB);

        var uriB = new Uri($"http://127.0.0.1:{portB}");

        // Act
        testOutputHelper.WriteLine("Node A running explicit HTTP discovery connecting to Integrated Node B natively...");
        var result = await nodeA.Discovery.DiscoverPeerAsync(uriB, null, CancellationToken.None);

        // Assert
        result.ShouldBeTrue();
        testOutputHelper.WriteLine("WebRTC out-of-band signaling exchange automated explicitly using the WS discovery service across integrated endpoints.");
    }

    [IntegrationFact]
    public async Task WebRtcHttpPeerDiscovery_IntegratedMode_Https_DiscoversAndConnects_Succeeds()
    {
        // Arrange
        var meshId = "sig-mesh-discovery-integ-https";
        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();
        var certInfo = GenerateTempCertificate();

        try
        {
            await using var nodeA = await CreateIntegratedTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA, true, certInfo.Path, certInfo.Password);
            await using var nodeB = await CreateIntegratedTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portB, true, certInfo.Path, certInfo.Password);

            var uriB = new Uri($"https://127.0.0.1:{portB}");

            // Act
            testOutputHelper.WriteLine("Node A running explicit HTTPS discovery securely connecting to Integrated Node B natively...");
            var result = await nodeA.Discovery.DiscoverPeerAsync(uriB, null, CancellationToken.None);

            // Assert
            result.ShouldBeTrue();
            testOutputHelper.WriteLine("WebRTC out-of-band signaling exchange automated explicitly using the WSS discovery service across integrated secure endpoints.");
        }
        finally
        {
            CleanupTempCertificate(certInfo.Path);
        }
    }

    [IntegrationFact]
    public async Task WebRtcHttpPeerDiscovery_OfflinePeer_ReturnsFalse()
    {
        // Arrange
        var meshId = "sig-mesh-discovery-offline";
        var portA = resourceManager.GetNextPort();
        var offlinePort = resourceManager.GetNextPort();

        await using var nodeA = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA);

        var offlineUri = new Uri($"http://127.0.0.1:{offlinePort}");

        // Act
        testOutputHelper.WriteLine($"Node A running explicit WS discovery connecting to offline peer {offlineUri}...");
        var result = await nodeA.Discovery.DiscoverPeerAsync(offlineUri, null, CancellationToken.None);

        // Assert
        result.ShouldBeFalse();
        testOutputHelper.WriteLine("Client suppressed the connection failure natively returning false.");
    }

    private async Task<WebRtcSignalingStandaloneTestNode> CreateStandaloneTestNodeAsync(string meshId, PeerId peerId, int port, bool useHttps = false, string? certPath = null, string? certPassword = null)
    {
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });
        
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        services.AddSingleton<ICertificateLoader, CertificateLoader>();

        services.Configure<P2pNodeOptions>(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        });

        services.AddP2pMesh(meshId)
            .AddWebRtcTransport(options =>
            {
                options.IceServers = Array.Empty<string>();
                options.IceGatheringTimeout = TimeSpan.FromSeconds(2);
            })
            .AddAspNetCoreWebRtcSignaling(options =>
            {
                options.HostingMode = AspNetCoreHostingMode.Standalone;
                options.StandaloneListenHost = "127.0.0.1";
                options.StandaloneListenPort = port;
                options.UseHttpsStandalone = useHttps;
                options.CertificateFilePath = certPath;
                options.CertificatePassword = certPassword;
                options.IgnoreOutboundSslErrors = true;
            })
            .AddWebRtcHttpPeerDiscovery();

        var provider = services.BuildServiceProvider();

        // Resolve dependencies BEFORE starting the hosted services to prevent leaking ports in case of misconfiguration crashes.
        var client = provider.GetRequiredService<IWebRtcSignalingClient>();
        var invitationService = provider.GetRequiredKeyedService<IWebRtcInvitationService>(meshId);
        var discovery = provider.GetRequiredKeyedService<IWebRtcHttpPeerDiscovery>(meshId);

        var hostedServices = provider.GetServices<IHostedService>();
        
        try
        {
            foreach (var svc in hostedServices)
            {
                await svc.StartAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch
        {
            foreach (var svc in hostedServices)
            {
                await svc.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
            await provider.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return new WebRtcSignalingStandaloneTestNode(
            provider,
            peerId,
            client,
            invitationService,
            discovery
        );
    }

    private async Task<WebRtcSignalingIntegratedTestNode> CreateIntegratedTestNodeAsync(string meshId, PeerId peerId, int port, bool useHttps = false, string? certPath = null, string? certPassword = null)
    {
        var builder = WebApplication.CreateBuilder();

        builder.Logging.AddXunit(testOutputHelper);
        builder.Logging.SetMinimumLevel(LogLevel.Trace);

        builder.Services.AddCrdt();
        builder.Services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        builder.Services.AddSingleton<ICertificateLoader, CertificateLoader>();

        builder.Services.Configure<P2pNodeOptions>(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        });

        builder.Services.AddP2pMesh(meshId)
            .AddWebRtcTransport(options =>
            {
                options.IceServers = Array.Empty<string>();
                options.IceGatheringTimeout = TimeSpan.FromSeconds(2);
            })
            .AddAspNetCoreWebRtcSignaling(options =>
            {
                options.HostingMode = AspNetCoreHostingMode.Integrated;
                options.IgnoreOutboundSslErrors = true;
            })
            .AddWebRtcHttpPeerDiscovery();

        builder.WebHost.ConfigureKestrel(options =>
        {
            if (useHttps && !string.IsNullOrWhiteSpace(certPath))
            {
                options.Listen(IPAddress.Parse("127.0.0.1"), port, listenOptions =>
                {
                    listenOptions.UseHttps(certPath, certPassword);
                });
            }
            else
            {
                options.Listen(IPAddress.Parse("127.0.0.1"), port);
            }
        });

        var app = builder.Build();
        
        // Ensure WebSockets are enabled so the WebRTC signaling mapped endpoint works cleanly securely
        app.UseWebSockets();

        // Map WebRTC signaling endpoints for Integrated mode
        app.MapP2pWebRtcSignalingEndpoints();

        await app.StartAsync(CancellationToken.None).ConfigureAwait(false);

        var client = app.Services.GetRequiredService<IWebRtcSignalingClient>();
        var invitationService = app.Services.GetRequiredKeyedService<IWebRtcInvitationService>(meshId);
        var discovery = app.Services.GetRequiredKeyedService<IWebRtcHttpPeerDiscovery>(meshId);

        return new WebRtcSignalingIntegratedTestNode(app, peerId, client, invitationService, discovery);
    }

    private static (string Path, string Password) GenerateTempCertificate()
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=127.0.0.1", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

        var sanBuilder = new SubjectAlternativeNameBuilder();
        sanBuilder.AddIpAddress(IPAddress.Parse("127.0.0.1"));
        sanBuilder.AddDnsName("localhost");
        req.CertificateExtensions.Add(sanBuilder.Build());

        req.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new Oid("1.3.6.1.5.5.7.3.1") }, false));
        
        var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".pfx");
        var password = "test_password";
        
        File.WriteAllBytes(path, cert.Export(X509ContentType.Pfx, password));
        return (path, password);
    }

    private static void CleanupTempCertificate(string path)
    {
        if (File.Exists(path))
        {
            try { File.Delete(path); } catch { /* Ignore cleanup errors in testing loops explicitly */ }
        }
    }

    private sealed record WebRtcSignalingStandaloneTestNode(
        ServiceProvider Provider,
        PeerId Id,
        IWebRtcSignalingClient Client,
        IWebRtcInvitationService InvitationService,
        IWebRtcHttpPeerDiscovery Discovery) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            var hostedServices = Provider.GetServices<IHostedService>();
            foreach (var svc in hostedServices)
            {
                await svc.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
            
            await Provider.DisposeAsync().ConfigureAwait(false);
        }
    }

    private sealed record WebRtcSignalingIntegratedTestNode(
        WebApplication App,
        PeerId Id,
        IWebRtcSignalingClient Client,
        IWebRtcInvitationService InvitationService,
        IWebRtcHttpPeerDiscovery Discovery) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await App.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await App.DisposeAsync().ConfigureAwait(false);
        }
    }
}