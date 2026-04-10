namespace Ama.Enterprise.P2p.IntegrationTests.Gossip;

using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.IntegrationTests.Attributes;
using Ama.Enterprise.P2p.IntegrationTests.Extensions;
using Ama.Enterprise.P2p.IntegrationTests.Gossip.Handlers;
using Ama.Enterprise.P2p.IntegrationTests.Gossip.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using System;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Integration tests verifying backwards compatibility and protocol versioning constraints.
/// </summary>
public sealed class P2pVersioningIntegrationTests
{
    private readonly ITestOutputHelper testOutputHelper;
    private const string TestMeshId = "VersioningIntegrationMesh";

    public P2pVersioningIntegrationTests(ITestOutputHelper testOutputHelper)
    {
        this.testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    }

    [IntegrationFact]
    public async Task Network_ShouldRejectMessages_WithIncompatibleMajorProtocolVersion()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var node = CreateTestNode(8210);
        await node.HostedService.StartAsync(cts.Token);

        var localVersion = Version.Parse(Constants.ProtocolVersion);
        var incompatibleVersion = new Version(localVersion.Major + 1, 0, 0);

        var services = new ServiceCollection();
        services.AddHttpClient();
        await using var sp = services.BuildServiceProvider();
        var clientFactory = sp.GetRequiredService<IHttpClientFactory>();
        using var client = clientFactory.CreateClient();

        using var request = new HttpRequestMessage(HttpMethod.Post, "http://localhost:8210/p2p/gossip/");
        request.Headers.Add("X-P2P-Protocol-Version", incompatibleVersion.ToString());
        request.Content = new StringContent("{}", Encoding.UTF8, "application/json");

        using var response = await client.SendAsync(request, cts.Token);

        response.StatusCode.ShouldBe(System.Net.HttpStatusCode.HttpVersionNotSupported);
    }

    [IntegrationFact]
    public async Task Network_ShouldAcceptMessages_WithCompatibleProtocolVersion_ForBackwardsCompatibility()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var node = CreateTestNode(8211);
        await node.HostedService.StartAsync(cts.Token);

        var localVersion = Version.Parse(Constants.ProtocolVersion);
        // Ensure same major version, but completely different minor and patch version to guarantee backwards compatibility success
        var compatibleVersion = new Version(localVersion.Major, localVersion.Minor + 99, 99);

        var services = new ServiceCollection();
        services.AddHttpClient();
        await using var sp = services.BuildServiceProvider();
        var clientFactory = sp.GetRequiredService<IHttpClientFactory>();
        using var client = clientFactory.CreateClient();

        using var request = new HttpRequestMessage(HttpMethod.Post, "http://localhost:8211/p2p/gossip/");
        request.Headers.Add("X-P2P-Protocol-Version", compatibleVersion.ToString());
        
        // We submit an empty JSON object payload. Since it successfully bypasses the 505 HttpVersionNotSupported check,
        // the generic endpoint will accept it and return 202 Accepted.
        request.Content = new StringContent("{}", Encoding.UTF8, "application/json");

        using var response = await client.SendAsync(request, cts.Token);

        // Verify it was NOT rejected for protocol version mismatch
        response.StatusCode.ShouldBe(System.Net.HttpStatusCode.Accepted);
    }

    [IntegrationFact]
    [TestedProtocolVersion(0, 1)]
    public async Task Network_ShouldAcceptMessages_WithCurrentYamlProtocolVersion_0_1()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var node = CreateTestNode(8212);
        await node.HostedService.StartAsync(cts.Token);

        // This explicit test directly maps to the deployed YAML version. 
        var deployedVersion = new Version(0, 1, 0);

        var services = new ServiceCollection();
        services.AddHttpClient();
        await using var sp = services.BuildServiceProvider();
        var clientFactory = sp.GetRequiredService<IHttpClientFactory>();
        using var client = clientFactory.CreateClient();

        using var request = new HttpRequestMessage(HttpMethod.Post, "http://localhost:8212/p2p/gossip/");
        request.Headers.Add("X-P2P-Protocol-Version", deployedVersion.ToString());
        
        // We submit an empty JSON object payload. Since it successfully bypasses the 505 HttpVersionNotSupported check,
        // the generic endpoint will accept it and return 202 Accepted.
        request.Content = new StringContent("{}", Encoding.UTF8, "application/json");

        using var response = await client.SendAsync(request, cts.Token);

        // Verify it passed the protocol verification and was accepted
        response.StatusCode.ShouldBe(System.Net.HttpStatusCode.Accepted);
    }

    private TestNode CreateTestNode(int port)
    {
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        services.AddP2pMesh(TestMeshId)
            .AddGossipNetwork(options =>
            {
                options.GossipInterval = TimeSpan.FromMilliseconds(500); 
                options.Fanout = 2;
                options.DefaultTimeToLive = 5;
            })
            .AddHttpTransport<GossipMessage>(options =>
            {
                options.ListenHost = "localhost";
                options.ListenPort = port;
                options.PathPrefix = "/p2p/gossip/";
            });

        var handler = new TestMessageHandler();
        services.AddSingleton<TestMessageHandler>(handler);
        
        services.AddKeyedSingleton<IMessageHandler<GossipMessage>>(TestMeshId, (sp, key) => sp.GetRequiredService<TestMessageHandler>());

        var provider = services.BuildServiceProvider();

        var endpoint = new HttpPeerEndpoint("localhost", port);
        var peerId = new PeerId(Guid.NewGuid());

        return new TestNode(
            provider,
            peerId,
            endpoint,
            handler,
            provider.GetServices<IHostedService>().OfType<P2pHostedService>().First(),
            provider.GetRequiredService<IP2pProtocol>(),
            provider.GetRequiredService<IPeerRegistry>()
        );
    }
}