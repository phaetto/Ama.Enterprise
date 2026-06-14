namespace Ama.Enterprise.P2p.IntegrationTests.Algorithms;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Shouldly;
using Xunit;
using System.Linq;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Models;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Handlers;
using Ama.Enterprise.P2p.Models.Algorithms;
using Ama.Enterprise.Project.Tests.Common.Networking;
using Ama.Enterprise.Project.Tests.Common.Extensions;
using Ama.Enterprise.Project.Tests.Common.Attributes;

/// <summary>
/// Integration tests verifying backwards compatibility and protocol versioning constraints natively using explicit TCP topologies.
/// </summary>
public sealed class P2pVersioningIntegrationTests(ITestOutputHelper testOutputHelper, NetworkResourceManager resourceManager) : IClassFixture<NetworkResourceManager>
{
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    private readonly NetworkResourceManager resourceManager = resourceManager ?? throw new ArgumentNullException(nameof(resourceManager));
    private const string TestMeshId = "VersioningIntegrationMesh";

    [IntegrationFact]
    public async Task Network_ShouldRejectMessages_WithIncompatibleMajorProtocolVersion()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var node = CreateTestNode(resourceManager.GetNextPort());
        await node.HostedService.StartAsync(cts.Token);

        var localVersion = Version.Parse(Constants.ProtocolVersion);
        var incompatibleVersion = new Version(localVersion.Major + 1, 0, 0);

        var router = node.Provider.GetRequiredKeyedService<ITransportRouter>(TestMeshId);
        
        var dummyMessage = new GossipMessage(TestMeshId, incompatibleVersion.ToString(), Guid.NewGuid(), new PeerId(Guid.NewGuid()), 5, ReadOnlyMemory<byte>.Empty);
        
        // Push the custom invalid message explicitly via the isolated loopback routing natively testing dropping logic structurally
        await router.SendAsync(node.Endpoint, dummyMessage, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(2), cts.Token);

        // Assert that the listener aggressively detected the invalid major version and forcefully dropped it before domain handlers triggered
        node.Handler.ReceivedMessages.ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task Network_ShouldAcceptMessages_WithCompatibleProtocolVersion_ForBackwardsCompatibility()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var node = CreateTestNode(resourceManager.GetNextPort());
        await node.HostedService.StartAsync(cts.Token);

        var localVersion = Version.Parse(Constants.ProtocolVersion);
        // Ensure same major version, but completely different minor and patch version to guarantee backwards compatibility success
        var compatibleVersion = new Version(localVersion.Major, localVersion.Minor + 99, 99);

        var router = node.Provider.GetRequiredKeyedService<ITransportRouter>(TestMeshId);
        
        var dummyMessage = new GossipMessage(TestMeshId, compatibleVersion.ToString(), Guid.NewGuid(), new PeerId(Guid.NewGuid()), 5, ReadOnlyMemory<byte>.Empty);
        
        await router.SendAsync(node.Endpoint, dummyMessage, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(2), cts.Token);

        // Verify it was NOT rejected for protocol version mismatch
        node.Handler.ReceivedMessages.Count.ShouldBe(1);
    }

    [IntegrationFact]
    [TestedProtocolVersion(0, 1)]
    public async Task Network_ShouldAcceptMessages_WithCurrentYamlProtocolVersion_0_1()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var node = CreateTestNode(resourceManager.GetNextPort());
        await node.HostedService.StartAsync(cts.Token);

        // This explicit test directly maps to the deployed YAML version natively covering explicit bound regressions gracefully.
        var deployedVersion = new Version(0, 1, 0);

        var router = node.Provider.GetRequiredKeyedService<ITransportRouter>(TestMeshId);
        
        var dummyMessage = new GossipMessage(TestMeshId, deployedVersion.ToString(), Guid.NewGuid(), new PeerId(Guid.NewGuid()), 5, ReadOnlyMemory<byte>.Empty);
        
        await router.SendAsync(node.Endpoint, dummyMessage, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(2), cts.Token);

        // Verify it passed the protocol verification and was accepted seamlessly
        node.Handler.ReceivedMessages.Count.ShouldBe(1);
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
            .AddTcpTransport(options =>
            {
                options.ListenHost = "127.0.0.1";
                options.ListenPort = port;
            });

        services.AddKeyedSingleton<IFailureDetector>(TestMeshId, (sp, key) => new TimeBasedFailureDetector((string)key!, sp.GetRequiredService<IOptionsMonitor<FailureDetectorOptions>>(), sp.GetRequiredService<ILogger<TimeBasedFailureDetector>>()));
        services.AddKeyedSingleton<IPeerAuthenticator>(TestMeshId, (sp, key) => new PassThroughPeerAuthenticator((string)key!, sp.GetRequiredService<ILogger<PassThroughPeerAuthenticator>>()));

        var handler = new TestMessageHandler();
        services.AddSingleton(handler);
        
        services.AddKeyedSingleton<IApplicationPayloadHandler>(TestMeshId, (sp, key) => sp.GetRequiredService<TestMessageHandler>());

        var provider = services.BuildServiceProvider();

        var endpoint = new TcpPeerEndpoint("127.0.0.1", port);
        var peerId = new PeerId(Guid.NewGuid());

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
}