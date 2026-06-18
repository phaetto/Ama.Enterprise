namespace Ama.Enterprise.P2p.IntegrationTests.Algorithms;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Handlers;
using Ama.Enterprise.P2p.Models.Algorithms;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Ama.Enterprise.Project.Tests.Common.Extensions;
using Ama.Enterprise.Project.Tests.Common.Networking;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

/// <summary>
/// Verifies the structural mapping and execution of the Zero-Trust multi-mesh explicitly mapping session constraints inherently tracking outbound limits.
/// </summary>
public sealed class SessionRoutingIntegrationTests(ITestOutputHelper testOutputHelper, NetworkResourceManager resourceManager) : IClassFixture<NetworkResourceManager>
{
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    private readonly NetworkResourceManager resourceManager = resourceManager ?? throw new ArgumentNullException(nameof(resourceManager));
    private const string TestMeshId = "ZeroTrustMesh";

    [IntegrationFact]
    public async Task Session_ShouldAuthorizeAndRoute_WhenTokenIsValid_AndPolicyPasses()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await using var nodeSender = CreateSessionNode(resourceManager.GetNextPort(), "admin");
        await using var nodeReceiver = CreateSessionNode(resourceManager.GetNextPort(), "admin");

        await StartAndHandshakeAsync(nodeSender, nodeReceiver, cts.Token);

        var payload = Encoding.UTF8.GetBytes("SecretData");
        await nodeSender.Protocol.BroadcastAsync(payload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeReceiver.Handler.ReceivedMessages.Count.ShouldBe(1);
        var receivedPayload = Encoding.UTF8.GetString(nodeReceiver.Handler.ReceivedMessages.First().Payload);
        receivedPayload.ShouldBe("SecretData");
    }

    [IntegrationFact]
    public async Task Session_ShouldDropOutbound_WhenPolicyDenies()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await using var nodeAdmin = CreateSessionNode(resourceManager.GetNextPort(), "admin");
        await using var nodeUser = CreateSessionNode(resourceManager.GetNextPort(), "user");

        await StartAndHandshakeAsync(nodeAdmin, nodeUser, cts.Token);

        var secretPayload = Encoding.UTF8.GetBytes("SecretData");
        await nodeAdmin.Protocol.BroadcastAsync(secretPayload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeUser.Handler.ReceivedMessages.Count.ShouldBe(0);

        var publicPayload = Encoding.UTF8.GetBytes("PublicData");
        await nodeAdmin.Protocol.BroadcastAsync(publicPayload, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeUser.Handler.ReceivedMessages.Count.ShouldBe(1);
    }

    [IntegrationFact]
    public async Task Session_ShouldDropInbound_WhenPolicyDenies()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await using var nodeAdmin = CreateSessionNode(resourceManager.GetNextPort(), "admin");
        await using var nodeUser = CreateSessionNode(resourceManager.GetNextPort(), "user");

        await StartAndHandshakeAsync(nodeUser, nodeAdmin, cts.Token);

        var maliciousPayload = Encoding.UTF8.GetBytes("SecretData");
        var gossipMessage = new GossipMessage(TestMeshId, Constants.ProtocolVersion, Guid.NewGuid(), nodeUser.Id, 5, maliciousPayload);
        
        await nodeUser.Router.SendAsync(nodeAdmin.Endpoint, gossipMessage, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);

        nodeAdmin.Handler.ReceivedMessages.Count.ShouldBe(0);
    }

    [IntegrationFact]
    public async Task Session_ShouldRejectHandshake_WhenTokenIsInvalid()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await using var nodeValid = CreateSessionNode(resourceManager.GetNextPort(), "admin");
        await using var nodeHacker = CreateSessionNode(resourceManager.GetNextPort(), "invalid");

        await nodeValid.HostedService.StartAsync(cts.Token);
        await nodeHacker.HostedService.StartAsync(cts.Token);

        var hackerToken = await nodeHacker.TokenValidator.GetLocalTokenBytesAsync(TestMeshId, cts.Token);
        
        var isAuthenticated = await nodeValid.Authenticator.AuthenticateAsync(new PeerNode(nodeHacker.Id, nodeHacker.Endpoint), hackerToken, cts.Token);
        isAuthenticated.ShouldBeFalse();

        var session = await nodeValid.SessionRegistry.GetSessionAsync(TestMeshId, nodeHacker.Id, cts.Token);
        session.ShouldBeNull();
    }

    private async Task StartAndHandshakeAsync(SessionTestNode source, SessionTestNode target, CancellationToken cancellationToken)
    {
        await source.HostedService.StartAsync(cancellationToken);
        await target.HostedService.StartAsync(cancellationToken);

        var targetToken = await target.TokenValidator.GetLocalTokenBytesAsync(TestMeshId, cancellationToken);
        var isTargetAuthenticated = await source.Authenticator.AuthenticateAsync(new PeerNode(target.Id, target.Endpoint), targetToken, cancellationToken);
        isTargetAuthenticated.ShouldBeTrue();

        var sourceToken = await source.TokenValidator.GetLocalTokenBytesAsync(TestMeshId, cancellationToken);
        var isSourceAuthenticated = await target.Authenticator.AuthenticateAsync(new PeerNode(source.Id, source.Endpoint), sourceToken, cancellationToken);
        isSourceAuthenticated.ShouldBeTrue();

        await source.Registry.AddOrUpdatePeerAsync(TestMeshId, new PeerNode(target.Id, target.Endpoint), PeerStatus.Active, cancellationToken);
        await target.Registry.AddOrUpdatePeerAsync(TestMeshId, new PeerNode(source.Id, source.Endpoint), PeerStatus.Active, cancellationToken);
    }

    private SessionTestNode CreateSessionNode(int port, string role)
    {
        var peerId = new PeerId(Guid.NewGuid());
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

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
            .AddSessionAuthentication<TestTokenValidator>()
            .EnableZeroTrustRouting()
            .AddRoutingPolicy<TestRoutingPolicy>();

        services.AddSingleton(new TestTokenValidator { ConfiguredRole = role });

        var handler = new TestMessageHandler();
        services.AddSingleton(handler);
        
        services.AddKeyedSingleton<IApplicationPayloadHandler>(TestMeshId, (sp, key) => sp.GetRequiredService<TestMessageHandler>());

        var provider = services.BuildServiceProvider();

        var endpoint = new TcpPeerEndpoint("127.0.0.1", port);

        return new SessionTestNode(
            provider,
            peerId,
            endpoint,
            handler,
            provider.GetServices<IHostedService>().OfType<P2pHostedService>().First(),
            provider.GetRequiredService<IP2pAlgorithm>(),
            provider.GetRequiredService<IPeerRegistry>(),
            provider.GetRequiredService<TestTokenValidator>(),
            provider.GetRequiredKeyedService<IPeerAuthenticator>(TestMeshId),
            provider.GetRequiredService<IPeerSessionRegistry>(),
            provider.GetRequiredKeyedService<ITransportRouter>(TestMeshId)
        );
    }

    public sealed record SessionTestNode(
        ServiceProvider Provider,
        PeerId Id,
        PeerEndpoint Endpoint,
        TestMessageHandler Handler,
        P2pHostedService HostedService,
        IP2pAlgorithm Protocol,
        IPeerRegistry Registry,
        TestTokenValidator TokenValidator,
        IPeerAuthenticator Authenticator,
        IPeerSessionRegistry SessionRegistry,
        ITransportRouter Router) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => Provider.DisposeAsync();
    }

    public sealed class TestTokenValidator : ISessionTokenValidator
    {
        public string ConfiguredRole { get; set; } = "user";

        public Task<ReadOnlyMemory<byte>> GetLocalTokenBytesAsync(string meshId, CancellationToken cancellationToken)
        {
            return Task.FromResult(new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(ConfiguredRole)));
        }

        public Task<SessionContext?> ValidateTokenAsync(string meshId, ReadOnlyMemory<byte> tokenData, CancellationToken cancellationToken)
        {
            var role = Encoding.UTF8.GetString(tokenData.Span);
            if (role == "invalid")
            {
                return Task.FromResult<SessionContext?>(null);
            }

            var session = new SessionContext(
                Guid.NewGuid().ToString(),
                new Dictionary<string, string> { { "role", role } },
                DateTimeOffset.UtcNow.AddHours(1)
            );
            return Task.FromResult<SessionContext?>(session);
        }
    }

    public sealed class TestRoutingPolicy : IMeshRoutingPolicy
    {
        public Task<bool> EvaluateOutboundAsync(string meshId, SessionContext session, IMeshMessage message, CancellationToken cancellationToken)
        {
            var payloadString = Encoding.UTF8.GetString(message.Payload.Span);
            if (session.Claims.TryGetValue("role", out var role) && role == "admin")
            {
                return Task.FromResult(true);
            }

            return Task.FromResult(payloadString.StartsWith("Public"));
        }

        public Task<bool> EvaluateInboundAsync(string meshId, SessionContext session, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
        {
            var payloadString = Encoding.UTF8.GetString(payload.Span);
            if (session.Claims.TryGetValue("role", out var role) && role == "admin")
            {
                return Task.FromResult(true);
            }

            return Task.FromResult(payloadString.StartsWith("Public"));
        }
    }
}