namespace Ama.Enterprise.P2p.IntegrationTests.Gossip;

using System;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.IntegrationTests.Attributes;
using Ama.Enterprise.P2p.IntegrationTests.Extensions;
using Ama.Enterprise.P2p.IntegrationTests.Gossip.Handlers;
using Ama.Enterprise.P2p.IntegrationTests.Gossip.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Gossip;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

/// <summary>
/// Integration tests verifying backwards compatibility and protocol versioning constraints.
/// </summary>
public sealed class P2pVersioningIntegrationTests
{
    private readonly ITestOutputHelper testOutputHelper;

    public P2pVersioningIntegrationTests(ITestOutputHelper testOutputHelper)
    {
        this.testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    }

    [IntegrationFact]
    public async Task Network_ShouldRejectMessages_WithIncompatibleMajorProtocolVersion()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var node = this.CreateTestNode(8210);
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
        await using var node = this.CreateTestNode(8211);
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
        
        // We submit an empty JSON object payload. Since deserializing "{}" will yield a default Guid for the mandatory MessageId,
        // the endpoint will return a 400 BadRequest. This confirms we bypassed the 505 HttpVersionNotSupported check successfully.
        request.Content = new StringContent("{}", Encoding.UTF8, "application/json");

        using var response = await client.SendAsync(request, cts.Token);

        // Verify it was NOT rejected for protocol version mismatch
        response.StatusCode.ShouldBe(System.Net.HttpStatusCode.BadRequest);
    }

    [IntegrationFact]
    [TestedProtocolVersion(0, 1)]
    public async Task Network_ShouldAcceptMessages_WithCurrentYamlProtocolVersion_0_1()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var node = this.CreateTestNode(8212);
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
        
        request.Content = new StringContent("{}", Encoding.UTF8, "application/json");

        using var response = await client.SendAsync(request, cts.Token);

        // Verify it passed the protocol verification and rejected just the dummy payload
        response.StatusCode.ShouldBe(System.Net.HttpStatusCode.BadRequest);
    }

    private TestNode CreateTestNode(int port)
    {
        var services = new ServiceCollection();
        
        services.AddLogging(builder => 
        {
            builder.AddXunit(this.testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        services.AddP2pGossipNetwork(options =>
        {
            options.ListenHost = "localhost";
            options.ListenPort = port;
            options.GossipInterval = TimeSpan.FromMilliseconds(500); 
            options.Fanout = 2;
            options.DefaultTimeToLive = 5;
        });

        var handler = new TestMessageHandler();
        services.AddSingleton<TestMessageHandler>(handler);
        
        services.AddSingleton<IMessageHandler<GossipMessage>>(sp => sp.GetRequiredService<TestMessageHandler>());

        var provider = services.BuildServiceProvider();

        var endpoint = new PeerEndpoint("localhost", port);
        var peerId = new PeerId(Guid.NewGuid());

        return new TestNode(
            provider,
            peerId,
            endpoint,
            handler,
            provider.GetRequiredService<IHostedService>(),
            provider.GetRequiredService<IGossipProtocol>(),
            provider.GetRequiredService<IPeerRegistry>()
        );
    }
}