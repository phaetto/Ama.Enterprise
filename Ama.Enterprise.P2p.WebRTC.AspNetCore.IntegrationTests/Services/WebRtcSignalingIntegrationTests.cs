namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.IntegrationTests.Services;

using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Extensions;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Models;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;
using Ama.Enterprise.P2p.WebRTC.Extensions;
using Ama.Enterprise.P2p.WebRTC.Models;
using Ama.Enterprise.P2p.WebRTC.Services;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Ama.Enterprise.UnitTests.Networking;
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
    public async Task WebRtcSignalingClient_StandaloneMode_SendOfferAsync_Succeeds()
    {
        // Arrange
        var meshId = "sig-mesh-send";
        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        testOutputHelper.WriteLine($"Initializing standalone Node A on port {portA}...");
        await using var nodeA = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA);
        
        testOutputHelper.WriteLine($"Initializing standalone Node B on port {portB}...");
        await using var nodeB = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portB);

        var uriB = new Uri($"http://127.0.0.1:{portB}");

        // Act
        testOutputHelper.WriteLine("Node A creating internal WebRTC invitation...");
        var localOffer = await nodeA.InvitationService.CreateInvitationAsync(CancellationToken.None);

        testOutputHelper.WriteLine("Node A sending SDP offer to Node B explicit signaling endpoint...");
        var answer = await nodeA.Client.SendOfferAsync(uriB, meshId, null, localOffer, CancellationToken.None);

        // Assert
        answer.ShouldNotBeNull();
        answer.Value.SdpAnswer.ShouldNotBeNullOrWhiteSpace();
        testOutputHelper.WriteLine("Node B responded with explicit SDP answer successfully.");

        testOutputHelper.WriteLine("Node A finalizing local connection mapping natively.");
        await nodeA.InvitationService.FinalizeInvitationAsync(localOffer.ConnectionId, answer.Value.SdpAnswer, CancellationToken.None);
    }

    [IntegrationFact]
    public async Task WebRtcSignalingClient_StandaloneMode_RequestOfferAndFinalize_Succeeds()
    {
        // Arrange
        var meshId = "sig-mesh-req";
        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        await using var nodeA = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA);
        await using var nodeB = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portB);

        var uriB = new Uri($"http://127.0.0.1:{portB}");

        // Act
        testOutputHelper.WriteLine("Node A requesting SDP offer explicitly from Node B...");
        var remoteOffer = await nodeA.Client.RequestOfferAsync(uriB, meshId, null, CancellationToken.None);
        
        remoteOffer.ShouldNotBeNull();
        remoteOffer.Value.SdpOffer.ShouldNotBeNullOrWhiteSpace();

        testOutputHelper.WriteLine("Node A answering remote SDP offer natively...");
        var localAnswer = await nodeA.InvitationService.AcceptInvitationAsync(remoteOffer.Value.SdpOffer, CancellationToken.None);

        // Map the explicit Node B remote connection ID back so Node B successfully locates its pending WebRTC peer instance
        var answerForNodeB = new WebRtcInvitationAnswer(remoteOffer.Value.ConnectionId, localAnswer.SdpAnswer);

        testOutputHelper.WriteLine("Node A pushing active SDP answer to finalize Node B's connection state...");
        var finalizeResult = await nodeA.Client.FinalizeInvitationAsync(uriB, meshId, null, answerForNodeB, CancellationToken.None);
        
        // Assert
        finalizeResult.ShouldBeTrue();
        testOutputHelper.WriteLine("WebRTC out-of-band signaling exchange finalized cleanly.");
    }

    [IntegrationFact]
    public async Task WebRtcSignalingClient_IntegratedMode_SendOfferAsync_Succeeds()
    {
        // Arrange
        var meshId = "sig-mesh-integ-send";
        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        testOutputHelper.WriteLine($"Initializing Integrated WebApplication Node A on port {portA}...");
        await using var nodeA = await CreateIntegratedTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA);
        
        testOutputHelper.WriteLine($"Initializing Integrated WebApplication Node B on port {portB}...");
        await using var nodeB = await CreateIntegratedTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portB);

        var uriB = new Uri($"http://127.0.0.1:{portB}");

        // Act
        testOutputHelper.WriteLine("Node A creating internal WebRTC invitation...");
        var localOffer = await nodeA.InvitationService.CreateInvitationAsync(CancellationToken.None);

        testOutputHelper.WriteLine("Node A sending SDP offer to Node B explicit integrated endpoint...");
        var answer = await nodeA.Client.SendOfferAsync(uriB, meshId, null, localOffer, CancellationToken.None);

        // Assert
        answer.ShouldNotBeNull();
        answer.Value.SdpAnswer.ShouldNotBeNullOrWhiteSpace();
        testOutputHelper.WriteLine("Node B responded with explicit SDP answer successfully through integrated HTTP bounds.");

        testOutputHelper.WriteLine("Node A finalizing local connection mapping natively.");
        await nodeA.InvitationService.FinalizeInvitationAsync(localOffer.ConnectionId, answer.Value.SdpAnswer, CancellationToken.None);
    }

    [IntegrationFact]
    public async Task WebRtcSignalingClient_IntegratedMode_RequestOfferAndFinalize_Succeeds()
    {
        // Arrange
        var meshId = "sig-mesh-integ-req";
        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        await using var nodeA = await CreateIntegratedTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA);
        await using var nodeB = await CreateIntegratedTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portB);

        var uriB = new Uri($"http://127.0.0.1:{portB}");

        // Act
        testOutputHelper.WriteLine("Node A requesting SDP offer explicitly from Integrated Node B...");
        var remoteOffer = await nodeA.Client.RequestOfferAsync(uriB, meshId, null, CancellationToken.None);
        
        remoteOffer.ShouldNotBeNull();
        remoteOffer.Value.SdpOffer.ShouldNotBeNullOrWhiteSpace();

        testOutputHelper.WriteLine("Node A answering remote SDP offer natively...");
        var localAnswer = await nodeA.InvitationService.AcceptInvitationAsync(remoteOffer.Value.SdpOffer, CancellationToken.None);

        var answerForNodeB = new WebRtcInvitationAnswer(remoteOffer.Value.ConnectionId, localAnswer.SdpAnswer);

        testOutputHelper.WriteLine("Node A pushing active SDP answer to finalize Integrated Node B's connection state...");
        var finalizeResult = await nodeA.Client.FinalizeInvitationAsync(uriB, meshId, null, answerForNodeB, CancellationToken.None);
        
        // Assert
        finalizeResult.ShouldBeTrue();
        testOutputHelper.WriteLine("WebRTC out-of-band signaling exchange finalized cleanly across integrated endpoints.");
    }

    [IntegrationFact]
    public async Task WebRtcSignalingClient_OfflinePeer_ReturnsNullGracefully()
    {
        // Arrange
        var meshId = "sig-mesh-offline";
        var portA = resourceManager.GetNextPort();
        var offlinePort = resourceManager.GetNextPort();

        await using var nodeA = await CreateStandaloneTestNodeAsync(meshId, new PeerId(Guid.NewGuid()), portA);

        var offlineUri = new Uri($"http://127.0.0.1:{offlinePort}");

        // Act
        testOutputHelper.WriteLine($"Node A requesting explicit offer from completely offline endpoint {offlineUri}...");
        var offer = await nodeA.Client.RequestOfferAsync(offlineUri, meshId, null, CancellationToken.None);
        
        // Assert
        offer.ShouldBeNull();
        testOutputHelper.WriteLine("Client gracefully suppressed the connection failure structurally.");
    }

    private async Task<WebRtcSignalingStandaloneTestNode> CreateStandaloneTestNodeAsync(string meshId, PeerId peerId, int port)
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
            });

        var provider = services.BuildServiceProvider();

        // Resolve dependencies BEFORE starting the hosted services to prevent leaking ports in case of misconfiguration crashes.
        var client = provider.GetRequiredService<IWebRtcSignalingClient>();
        var invitationService = provider.GetRequiredKeyedService<IWebRtcInvitationService>(meshId);

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
            invitationService
        );
    }

    private async Task<WebRtcSignalingIntegratedTestNode> CreateIntegratedTestNodeAsync(string meshId, PeerId peerId, int port)
    {
        var builder = WebApplication.CreateBuilder();

        builder.Logging.AddXunit(testOutputHelper);
        builder.Logging.SetMinimumLevel(LogLevel.Trace);

        builder.Services.AddCrdt();
        builder.Services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

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
            });

        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(IPAddress.Parse("127.0.0.1"), port);
        });

        var app = builder.Build();
        
        // Map WebRTC signaling endpoints for Integrated mode
        app.MapP2pWebRtcSignalingEndpoints();

        await app.StartAsync(CancellationToken.None).ConfigureAwait(false);

        var client = app.Services.GetRequiredService<IWebRtcSignalingClient>();
        var invitationService = app.Services.GetRequiredKeyedService<IWebRtcInvitationService>(meshId);

        return new WebRtcSignalingIntegratedTestNode(app, peerId, client, invitationService);
    }

    private sealed record WebRtcSignalingStandaloneTestNode(
        ServiceProvider Provider,
        PeerId Id,
        IWebRtcSignalingClient Client,
        IWebRtcInvitationService InvitationService) : IAsyncDisposable
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
        IWebRtcInvitationService InvitationService) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await App.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await App.DisposeAsync().ConfigureAwait(false);
        }
    }
}