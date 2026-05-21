namespace Ama.Enterprise.P2p.AspNetCore.IntegrationTests.Services;

using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.AspNetCore.Extensions;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
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

public sealed class AspNetCorePeerDiscoveryIntegrationTests : IClassFixture<NetworkResourceManager>
{
    private readonly ITestOutputHelper testOutputHelper;
    private readonly NetworkResourceManager resourceManager;

    public AspNetCorePeerDiscoveryIntegrationTests(ITestOutputHelper testOutputHelper, NetworkResourceManager resourceManager)
    {
        this.testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
        this.resourceManager = resourceManager ?? throw new ArgumentNullException(nameof(resourceManager));
    }

    [IntegrationFact]
    public async Task AspNetCorePeerDiscovery_StandaloneMode_DiscoversAndRegistersPeer_Successfully()
    {
        // Arrange
        var meshId = $"asp-disc-std-{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var portAHandshake = resourceManager.GetNextPort();
        var portBHandshake = resourceManager.GetNextPort();
        var portADiscovery = resourceManager.GetNextPort();
        var portBDiscovery = resourceManager.GetNextPort();

        var discoveryUrlForA = $"http://127.0.0.1:{portBDiscovery}/ama-enterprise/p2p-discovery/{meshId}";
        var discoveryUrlForB = $"http://127.0.0.1:{portADiscovery}/ama-enterprise/p2p-discovery/{meshId}";

        using var topologySemaphore = new SemaphoreSlim(0);
        Action onTopologyChanged = () => 
        {
            try { topologySemaphore.Release(); } catch { }
        };

        testOutputHelper.WriteLine("Initializing Nodes for Standalone ASP.NET Core Discovery...");
        await using var nodeA = CreateStandaloneNode(meshId, peerAId, portAHandshake, portADiscovery, discoveryUrlForA, onTopologyChanged);
        await using var nodeB = CreateStandaloneNode(meshId, peerBId, portBHandshake, portBDiscovery, discoveryUrlForB, onTopologyChanged);

        // Act
        testOutputHelper.WriteLine("Starting P2P infrastructure spanning Standalone ASP.NET Core bounds...");
        await nodeA.StartAsync(cts.Token);
        await nodeB.StartAsync(cts.Token);

        // Assert
        testOutputHelper.WriteLine("Waiting for isolated HTTP polling and subsequent Phase 2 handshakes to complete natively...");
        
        bool discovered = false;
        var timeoutTime = DateTime.UtcNow.AddSeconds(30);

        while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
        {
            var peersA = await nodeA.Registry.GetAllPeersAsync(meshId, cts.Token);
            var peersB = await nodeB.Registry.GetAllPeersAsync(meshId, cts.Token);

            var aDiscoveredB = peersA.FirstOrDefault(p => p.Id.Equals(peerBId));
            var bDiscoveredA = peersB.FirstOrDefault(p => p.Id.Equals(peerAId));

            if (aDiscoveredB.Endpoint is not null && bDiscoveredA.Endpoint is not null)
            {
                aDiscoveredB.Endpoint.ShouldBeOfType<AspNetCorePeerEndpoint>();
                bDiscoveredA.Endpoint.ShouldBeOfType<AspNetCorePeerEndpoint>();

                var bEndpoint = (AspNetCorePeerEndpoint)aDiscoveredB.Endpoint;
                bEndpoint.Port.ShouldBe(portBHandshake);

                discovered = true;
                break;
            }

            await topologySemaphore.WaitAsync(TimeSpan.FromMilliseconds(500), cts.Token);
        }

        discovered.ShouldBeTrue("Nodes failed to discover each other via Standalone ASP.NET Core polling and HTTP handshakes cleanly efficiently mapping topologies explicitly.");
    }

    [IntegrationFact]
    public async Task AspNetCorePeerDiscovery_IntegratedMode_DiscoversAndRegistersPeer_Successfully()
    {
        // Arrange
        var meshId = $"asp-disc-int-{Guid.NewGuid():N}";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        var peerAId = new PeerId(Guid.NewGuid());
        var peerBId = new PeerId(Guid.NewGuid());

        var portA = resourceManager.GetNextPort();
        var portB = resourceManager.GetNextPort();

        var discoveryUrlForA = $"http://127.0.0.1:{portB}/test-discovery/{meshId}";
        var discoveryUrlForB = $"http://127.0.0.1:{portA}/test-discovery/{meshId}";

        using var topologySemaphore = new SemaphoreSlim(0);
        Action onTopologyChanged = () => 
        {
            try { topologySemaphore.Release(); } catch { }
        };

        testOutputHelper.WriteLine("Initializing WebApplications for Integrated ASP.NET Core Discovery...");
        await using var appA = CreateIntegratedTestNode(meshId, peerAId, portA, discoveryUrlForA, onTopologyChanged);
        await using var appB = CreateIntegratedTestNode(meshId, peerBId, portB, discoveryUrlForB, onTopologyChanged);

        // Act
        testOutputHelper.WriteLine("Starting Integrated ASP.NET Core applications spanning single-port bindings...");
        await appA.StartAsync(cts.Token);
        await appB.StartAsync(cts.Token);

        // Assert
        testOutputHelper.WriteLine("Waiting for HTTP polling routes to hit mapping constraints natively...");
        
        bool discovered = false;
        var timeoutTime = DateTime.UtcNow.AddSeconds(30);

        var registryA = appA.Services.GetRequiredService<IPeerRegistry>();
        var registryB = appB.Services.GetRequiredService<IPeerRegistry>();

        while (DateTime.UtcNow < timeoutTime && !cts.Token.IsCancellationRequested)
        {
            var peersA = await registryA.GetAllPeersAsync(meshId, cts.Token);
            var peersB = await registryB.GetAllPeersAsync(meshId, cts.Token);

            var aDiscoveredB = peersA.FirstOrDefault(p => p.Id.Equals(peerBId));
            var bDiscoveredA = peersB.FirstOrDefault(p => p.Id.Equals(peerAId));

            if (aDiscoveredB.Endpoint is not null && bDiscoveredA.Endpoint is not null)
            {
                var bEndpoint = (AspNetCorePeerEndpoint)aDiscoveredB.Endpoint;
                bEndpoint.Port.ShouldBe(portB);

                discovered = true;
                break;
            }

            await topologySemaphore.WaitAsync(TimeSpan.FromMilliseconds(500), cts.Token);
        }

        discovered.ShouldBeTrue("Nodes failed to discover each other natively evaluating Kestrel shared routing pipelines efficiently reliably.");
        
        await appA.StopAsync(cts.Token);
        await appB.StopAsync(cts.Token);
    }

    

    private AspNetCoreTestNode CreateStandaloneNode(string meshId, PeerId peerId, int handshakePort, int discoveryPort, string? discoveryUrl, Action onTopologyChanged)
    {
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });
        
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        services.AddSingleton<IPeerTopologyObserver>(new TestTopologyObserver(onTopologyChanged));

        services.AddKeyedSingleton<PeerEndpoint>(meshId, new AspNetCorePeerEndpoint("127.0.0.1", handshakePort));

        services.Configure<FailureDetectorOptions>(meshId, options => 
        {
            options.HeartbeatInterval = TimeSpan.FromSeconds(120);
        });

        services.AddP2pMesh(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        })
        .AddGossipNetwork()
        .AddAspNetCorePeerHandshake(options =>
        {
            options.HostingMode = AspNetCoreHostingMode.Standalone;
            options.StandaloneListenHost = "+";
            options.StandaloneListenPort = handshakePort;
            options.AdvertisedHandshakePort = handshakePort;
            options.HandshakeTimeout = TimeSpan.FromSeconds(10);
        })
        .AddAspNetCorePeerDiscovery(options =>
        {
            options.HostingMode = AspNetCoreHostingMode.Standalone;
            options.StandaloneListenHost = "+";
            options.StandaloneListenPort = discoveryPort;
            options.DiscoveryUrl = discoveryUrl ?? string.Empty;
            options.DiscoveryInterval = TimeSpan.FromSeconds(2);
            options.DiscoveryTimeout = TimeSpan.FromSeconds(5);
        });

        var provider = services.BuildServiceProvider();

        return new AspNetCoreTestNode(
            provider,
            peerId,
            provider.GetRequiredService<IPeerRegistry>()
        );
    }

    private WebApplication CreateIntegratedTestNode(string meshId, PeerId peerId, int listenPort, string? discoveryUrl, Action onTopologyChanged)
    {
        var builder = WebApplication.CreateBuilder();

        builder.Logging.AddXunit(testOutputHelper);
        builder.Logging.SetMinimumLevel(LogLevel.Trace);

        builder.Services.AddCrdt();
        builder.Services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        builder.Services.AddSingleton<IPeerTopologyObserver>(new TestTopologyObserver(onTopologyChanged));

        builder.Services.AddKeyedSingleton<PeerEndpoint>(meshId, new AspNetCorePeerEndpoint("127.0.0.1", listenPort));

        builder.Services.Configure<FailureDetectorOptions>(meshId, options => 
        {
            options.HeartbeatInterval = TimeSpan.FromSeconds(120);
        });

        builder.Services.AddP2pMesh(meshId, options =>
        {
            options.LocalPeerId = peerId.Value;
        })
        .AddGossipNetwork()
        .AddAspNetCorePeerHandshake(options =>
        {
            options.HostingMode = AspNetCoreHostingMode.Integrated;
            options.AdvertisedHandshakePort = listenPort;
            options.HandshakeTimeout = TimeSpan.FromSeconds(10);
            options.PathPrefix = "/test-handshake/";
        })
        .AddAspNetCorePeerDiscovery(options =>
        {
            options.HostingMode = AspNetCoreHostingMode.Integrated;
            options.DiscoveryUrl = discoveryUrl ?? string.Empty;
            options.DiscoveryInterval = TimeSpan.FromSeconds(2);
            options.DiscoveryTimeout = TimeSpan.FromSeconds(5);
            options.PathPrefix = "/test-discovery/";
        });

        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(IPAddress.Parse("127.0.0.1"), listenPort);
        });

        var app = builder.Build();
        app.MapP2pMeshHandshakes("/test-handshake");
        app.MapP2pMeshDiscovery("/test-discovery");

        return app;
    }

    private sealed record AspNetCoreTestNode(
        ServiceProvider Provider,
        PeerId Id,
        IPeerRegistry Registry) : IAsyncDisposable
    {
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            var hostedServices = Provider.GetServices<IHostedService>();
            foreach (var hostedService in hostedServices)
            {
                await hostedService.StartAsync(cancellationToken);
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            var hostedServices = Provider.GetServices<IHostedService>();
            foreach (var hostedService in hostedServices.Reverse())
            {
                await hostedService.StopAsync(cancellationToken);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await StopAsync(CancellationToken.None);
            await Provider.DisposeAsync();
        }
    }

    private sealed class TestTopologyObserver : IPeerTopologyObserver
    {
        private readonly Action onTopologyChanged;

        public TestTopologyObserver(Action onTopologyChanged)
        {
            this.onTopologyChanged = onTopologyChanged ?? throw new ArgumentNullException(nameof(onTopologyChanged));
        }

        public Task OnPeerJoinedAsync(string meshId, PeerNode node, CancellationToken cancellationToken)
        {
            try { onTopologyChanged(); } catch { }
            return Task.CompletedTask;
        }

        public Task OnPeerDepartedAsync(string meshId, PeerId peerId, CancellationToken cancellationToken)
        {
            try { onTopologyChanged(); } catch { }
            return Task.CompletedTask;
        }

        public Task OnPeerStatusChangedAsync(string meshId, PeerId peerId, PeerStatus newStatus, CancellationToken cancellationToken)
        {
            try { onTopologyChanged(); } catch { }
            return Task.CompletedTask;
        }
    }
}