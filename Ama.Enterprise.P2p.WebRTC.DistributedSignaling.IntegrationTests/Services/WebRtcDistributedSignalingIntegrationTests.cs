namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.IntegrationTests.Services;

using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Algorithms;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Extensions;
using Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Models;
using Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Services;
using Ama.Enterprise.P2p.WebRTC.Extensions;
using Ama.Enterprise.P2p.WebRTC.Models;
using Ama.Enterprise.Project.Tests.Common.Attributes;
using Ama.Enterprise.Project.Tests.Common.Extensions;
using Ama.Enterprise.Project.Tests.Common.Networking;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;
using Xunit;

public sealed class WebRtcDistributedSignalingIntegrationTests(ITestOutputHelper testOutputHelper, NetworkResourceManager resourceManager) : IClassFixture<NetworkResourceManager>
{
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    private readonly NetworkResourceManager resourceManager = resourceManager ?? throw new ArgumentNullException(nameof(resourceManager));

    [IntegrationFact]
    public async Task SignalingClient_JoinIntents_Lifecycle_Succeeds()
    {
        // Arrange
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var hubPort = resourceManager.GetNextPort();
        
        var meshId = "integration-mesh-intents";
        var replicaId = "hub-replica";
        var peerId = Guid.NewGuid();
        var documentId = "test-doc-intents";

        await using var server = await CreateSignalingHubAppAsync(hubPort, meshId, replicaId, documentId);
        await server.StartAsync(cts.Token);

        await using var nodeProvider = CreateTestNode(meshId, peerId, hubPort);
        var client = nodeProvider.Provider.GetRequiredService<IWebRtcDistributedSignalingClient>();

        // Act - Set
        testOutputHelper.WriteLine("Setting Join Intent...");
        await client.SetJoinIntentAsync(replicaId, peerId, documentId, cts.Token);

        // Assert - Get
        testOutputHelper.WriteLine("Retrieving Join Intents...");
        var intents = await client.GetJoinIntentsAsync(replicaId, documentId, cts.Token);
        intents.ShouldContainKey(peerId.ToString());

        // Act - Remove
        testOutputHelper.WriteLine("Removing Join Intent...");
        await client.RemoveJoinIntentAsync(replicaId, peerId, documentId, cts.Token);

        // Assert - Empty
        testOutputHelper.WriteLine("Verifying Join Intents are cleared...");
        var emptyIntents = await client.GetJoinIntentsAsync(replicaId, documentId, cts.Token);
        emptyIntents.ShouldNotContainKey(peerId.ToString());

        await server.StopAsync(cts.Token);
    }

    [IntegrationFact]
    public async Task SignalingClient_OffersAndAnswers_Lifecycle_Succeeds()
    {
        // Arrange
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var hubPort = resourceManager.GetNextPort();
        
        var meshId = "integration-mesh-sdp";
        var replicaId = "hub-replica";
        var peerId = Guid.NewGuid();
        var targetId = Guid.NewGuid();
        var documentId = "test-doc-sdp";
        
        var offerRoutingKey = $"{targetId}:{peerId}";
        var answerRoutingKey = $"{peerId}:{targetId}";

        await using var server = await CreateSignalingHubAppAsync(hubPort, meshId, replicaId, documentId);
        await server.StartAsync(cts.Token);

        await using var nodeProvider = CreateTestNode(meshId, peerId, hubPort);
        var client = nodeProvider.Provider.GetRequiredService<IWebRtcDistributedSignalingClient>();

        var dummyOffer = new WebRtcInvitationOffer { ConnectionId = Guid.NewGuid(), SdpOffer = "v=0\r\noffer-data" };
        var dummyAnswer = new WebRtcInvitationAnswer { ConnectionId = dummyOffer.ConnectionId, SdpAnswer = "v=0\r\nanswer-data" };

        // Act - Set Offer
        testOutputHelper.WriteLine("Setting WebRTC Offer...");
        await client.SetOfferAsync(replicaId, offerRoutingKey, dummyOffer, documentId, cts.Token);
        var offers = await client.GetOffersAsync(replicaId, documentId, cts.Token);
        
        // Assert Offer
        offers.ShouldContainKey(offerRoutingKey);
        offers[offerRoutingKey].SdpOffer.ShouldBe(dummyOffer.SdpOffer);

        // Act - Set Answer
        testOutputHelper.WriteLine("Setting WebRTC Answer...");
        await client.SetAnswerAsync(replicaId, answerRoutingKey, dummyAnswer, documentId, cts.Token);
        var answers = await client.GetAnswersAsync(replicaId, documentId, cts.Token);

        // Assert Answer
        answers.ShouldContainKey(answerRoutingKey);
        answers[answerRoutingKey].SdpAnswer.ShouldBe(dummyAnswer.SdpAnswer);

        // Act - Remove Both
        testOutputHelper.WriteLine("Cleaning up specific routing bounds...");
        await client.RemoveOfferAsync(replicaId, offerRoutingKey, documentId, cts.Token);
        await client.RemoveAnswerAsync(replicaId, answerRoutingKey, documentId, cts.Token);

        // Assert Removed
        var emptyOffers = await client.GetOffersAsync(replicaId, documentId, cts.Token);
        var emptyAnswers = await client.GetAnswersAsync(replicaId, documentId, cts.Token);

        emptyOffers.ShouldNotContainKey(offerRoutingKey);
        emptyAnswers.ShouldNotContainKey(answerRoutingKey);

        await server.StopAsync(cts.Token);
    }

    [IntegrationFact]
    public async Task Orchestrator_EndToEnd_PresenceAndSdpExchange_Succeeds()
    {
        // Arrange
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var hubPort = resourceManager.GetNextPort();
        
        var meshId = "integration-mesh-orchestrator";
        var replicaId = "hub-replica";
        var documentId = "orchestrator-doc";

        await using var server = await CreateSignalingHubAppAsync(hubPort, meshId, replicaId, documentId);
        await server.StartAsync(cts.Token);
        
        // Use predictable deterministic IDs to guarantee PeerA generates the offer (Lexicographical logic natively)
        var peerAId = Guid.Parse("00000000-0000-0000-0000-000000000001");
        var peerBId = Guid.Parse("00000000-0000-0000-0000-000000000002");

        await using var nodeA = CreateTestNode(meshId, peerAId, hubPort);
        await using var nodeB = CreateTestNode(meshId, peerBId, hubPort);

        var scopeManager = server.Services.GetRequiredService<DistributedCrdtScopeManager>();
        var hubScope = scopeManager.GetOrCreateScope(replicaId);
        var signalingManager = hubScope.ServiceProvider.GetRequiredService<ICrdtSignalingManager>();

        // Act 1: Publish Intents
        testOutputHelper.WriteLine("Publishing Join Intents from Node A and Node B...");
        await nodeA.Orchestrator.PublishJoinIntentAsync(meshId, replicaId, documentId, cts.Token);
        await nodeB.Orchestrator.PublishJoinIntentAsync(meshId, replicaId, documentId, cts.Token);

        await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);

        // Act 2: Process Intents
        testOutputHelper.WriteLine("Processing Join Intents generating targeted localized offers...");
        await nodeA.Orchestrator.ProcessJoinIntentsAsync(meshId, replicaId, documentId, cts.Token);
        await nodeB.Orchestrator.ProcessJoinIntentsAsync(meshId, replicaId, documentId, cts.Token);

        await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);

        // Verify Offer exists in the hub (Peer A -> Peer B)
        var hubOffers = signalingManager.GetOffers(documentId);
        hubOffers.Count.ShouldBe(1, "Exactly one offer should be generated by the lexicographically smaller peer.");

        // Act 3: Process Pending Offers
        testOutputHelper.WriteLine("Processing mapped Pending Offers...");
        await nodeA.Orchestrator.ProcessPendingOffersAsync(meshId, replicaId, documentId, cts.Token);
        await nodeB.Orchestrator.ProcessPendingOffersAsync(meshId, replicaId, documentId, cts.Token);

        await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);

        // Verify Answer exists
        var hubAnswers = signalingManager.GetAnswers(documentId);
        hubAnswers.Count.ShouldBe(1, "Exactly one answer should be generated natively matching the offer.");

        // Act 4: Process Pending Answers
        testOutputHelper.WriteLine("Processing mapped Pending Answers finalizing structural WebRTC connections...");
        await nodeA.Orchestrator.ProcessPendingAnswersAsync(meshId, replicaId, documentId, cts.Token);
        await nodeB.Orchestrator.ProcessPendingAnswersAsync(meshId, replicaId, documentId, cts.Token);

        await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);

        // Assert: Everything should be consumed/cleared gracefully
        hubOffers = signalingManager.GetOffers(documentId);
        hubAnswers = signalingManager.GetAnswers(documentId);
        
        hubOffers.Count.ShouldBe(0, "Offers should be natively consumed and deleted by the answering peer.");
        hubAnswers.Count.ShouldBe(0, "Answers should be natively consumed and deleted by the offering peer finalizing constraints.");

        await server.StopAsync(cts.Token);
    }

    private async Task<WebApplication> CreateSignalingHubAppAsync(int listenPort, string meshId, string replicaId, string documentId)
    {
        var builder = WebApplication.CreateBuilder();

        builder.Logging.AddXunit(testOutputHelper);
        builder.Logging.SetMinimumLevel(LogLevel.Trace);

        // Register DCRDT exactly mirroring standard initialization constraints
        builder.Services.AddDistributedCrdtCore(opt =>
        {
            opt.CheckpointIntervalSeconds = 1;
            opt.AntiEntropyIntervalSeconds = 1;
            opt.AntiEntropyInitialDelaySeconds = 0;
            opt.ActiveSyncEnabled = false;
        });

        builder.Services.AddDistributedCrdtReplica(replicaId);
        
        builder.Services.AddCrdt();

        // Inject specific DCRDT P2P boundaries alongside dummy Mocks tracking network payloads naturally isolating limits
        builder.Services.AddDistributedCrdtP2p(meshId, replicaId);
        builder.Services.AddSingleton(Mock.Of<IP2pAlgorithm>());
        builder.Services.AddSingleton(Mock.Of<IDirectMessageSender>());

        // Register the distributed signaling hub
        builder.Services.AddWebRtcDistributedSignaling();

        // Configure realistic network binding natively
        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(IPAddress.Parse("127.0.0.1"), listenPort);
        });

        var app = builder.Build();

        // Map signaling HTTP minimal APIs
        app.MapWebRtcDistributedSignalingEndpoints();

        // Manually initialize DCRDT Document Orchestrator resolving logical background execution matrices
        var scopeManager = app.Services.GetRequiredService<DistributedCrdtScopeManager>();
        var scope = scopeManager.GetOrCreateScope(replicaId);
        var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
        
        await orchestrator.InitializeAsync(CancellationToken.None).ConfigureAwait(false);
        await orchestrator.CreateDocumentAsync(documentId, Constants.SignalingDocumentTypeAlias, CancellationToken.None).ConfigureAwait(false);
        await orchestrator.SyncDocumentsAsync(CancellationToken.None).ConfigureAwait(false);

        return app;
    }

    private TestNode CreateTestNode(string meshId, Guid peerId, int hubPort)
    {
        var services = new ServiceCollection();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        // Core CRDT mapping dependencies
        services.AddCrdt();
        services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        services.Configure<P2pNodeOptions>(meshId, options =>
        {
            options.LocalPeerId = peerId;
        });

        services.AddP2pMesh(meshId)
            .AddWebRtcTransport(options =>
            {
                // Disable external STUN lookup to accelerate local integration tests efficiently
                options.IceServers = Array.Empty<string>();
                options.IceGatheringTimeout = TimeSpan.FromSeconds(2);
            });

        // Setup the specific HTTP Client mapped against the real WebApplication port natively
        services.AddWebRtcDistributedSignalingClient(client =>
        {
            client.BaseAddress = new Uri($"http://127.0.0.1:{hubPort}");
        });

        services.AddSingleton<IWebRtcSignalingOrchestrator, WebRtcSignalingOrchestrator>();

        var provider = services.BuildServiceProvider();
        var orchestrator = provider.GetRequiredService<IWebRtcSignalingOrchestrator>();

        return new TestNode(provider, orchestrator);
    }

    private sealed record TestNode(ServiceProvider Provider, IWebRtcSignalingOrchestrator Orchestrator) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Provider.DisposeAsync();
        }
    }
}