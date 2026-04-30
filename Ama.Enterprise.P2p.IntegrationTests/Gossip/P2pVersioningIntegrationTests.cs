namespace Ama.Enterprise.P2p.IntegrationTests.Gossip;

using Ama.CRDT.Extensions;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Ama.Enterprise.P2p.IntegrationTests.Gossip.Handlers;
using Ama.Enterprise.P2p.IntegrationTests.Gossip.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Shouldly;
using System;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Ama.Enterprise.P2p.Models.Transports;

/// <summary>
/// Integration tests verifying backwards compatibility and protocol versioning constraints.
/// </summary>
public sealed class P2pVersioningIntegrationTests(ITestOutputHelper testOutputHelper)
{
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    private const string TestMeshId = "VersioningIntegrationMesh";

    [IntegrationFact]
    public async Task Network_ShouldRejectMessages_WithIncompatibleMajorProtocolVersion()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var node = CreateTestNode(8210);
        await node.HostedService.StartAsync(cts.Token);

        var localVersion = Version.Parse(Constants.ProtocolVersion);
        var incompatibleVersion = new Version(localVersion.Major + 1, 0, 0);

        var clientFactory = node.Provider.GetRequiredService<IHttpClientFactory>();
        var serializer = node.Provider.GetRequiredService<ICrdtSerializer>();
        using var client = clientFactory.CreateClient();

        using var request = new HttpRequestMessage(HttpMethod.Post, "http://localhost:8210/p2p/gossip/");
        
        var dummyMessage = new GossipMessage(TestMeshId, incompatibleVersion.ToString(), Guid.NewGuid(), new PeerId(Guid.NewGuid()), 5, ReadOnlyMemory<byte>.Empty);
        var payloadBytes = serializer.SerializeToBytes<IMeshMessage>(dummyMessage);
        
        request.Content = new ByteArrayContent(payloadBytes);

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

        var clientFactory = node.Provider.GetRequiredService<IHttpClientFactory>();
        var serializer = node.Provider.GetRequiredService<ICrdtSerializer>();
        
        using var client = clientFactory.CreateClient();

        using var request = new HttpRequestMessage(HttpMethod.Post, "http://localhost:8211/p2p/gossip/");
        
        // We submit a valid serialized model payload targeting the specific mesh to bypass HTTP listener isolation verification checks gracefully
        var dummyMessage = new GossipMessage(TestMeshId, compatibleVersion.ToString(), Guid.NewGuid(), new PeerId(Guid.NewGuid()), 5, ReadOnlyMemory<byte>.Empty);
        
        // Use STJ polymorphism mapping explicitly identifying the generic interface container
        var payloadBytes = serializer.SerializeToBytes<IMeshMessage>(dummyMessage);
        
        request.Content = new ByteArrayContent(payloadBytes);

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

        var clientFactory = node.Provider.GetRequiredService<IHttpClientFactory>();
        var serializer = node.Provider.GetRequiredService<ICrdtSerializer>();
        
        using var client = clientFactory.CreateClient();

        using var request = new HttpRequestMessage(HttpMethod.Post, "http://localhost:8212/p2p/gossip/");
        
        var dummyMessage = new GossipMessage(TestMeshId, deployedVersion.ToString(), Guid.NewGuid(), new PeerId(Guid.NewGuid()), 5, ReadOnlyMemory<byte>.Empty);
        var payloadBytes = serializer.SerializeToBytes<IMeshMessage>(dummyMessage);
        
        request.Content = new ByteArrayContent(payloadBytes);

        using var response = await client.SendAsync(request, cts.Token);

        // Verify it passed the protocol verification and was accepted
        response.StatusCode.ShouldBe(System.Net.HttpStatusCode.Accepted);
    }

    private TestNode CreateTestNode(int port)
    {
        var services = new ServiceCollection();

        services.AddCrdt();
        services.AddHttpClient();

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
            .AddHttpTransport(options =>
            {
                options.ListenHost = "localhost";
                options.ListenPort = port;
                options.PathPrefix = "/p2p/gossip/";
            });

        // Ensure newly mapped keyed interfaces natively resolve bounds explicitly safely
        services.AddKeyedSingleton<IFailureDetector>(TestMeshId, (sp, key) => new TimeBasedFailureDetector((string)key!, sp.GetRequiredService<IOptionsMonitor<FailureDetectorOptions>>(), sp.GetRequiredService<ILogger<TimeBasedFailureDetector>>()));
        services.AddKeyedSingleton<IPeerAuthenticator>(TestMeshId, (sp, key) => new PassThroughPeerAuthenticator((string)key!, sp.GetRequiredService<ILogger<PassThroughPeerAuthenticator>>()));

        var handler = new TestMessageHandler();
        services.AddSingleton(handler);
        
        services.AddKeyedSingleton<IApplicationPayloadHandler>(TestMeshId, (sp, key) => sp.GetRequiredService<TestMessageHandler>());

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