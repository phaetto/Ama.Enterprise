namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.Models;
using Ama.Enterprise.P2p.WebRTC.Services;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implements programmatic WebRTC discovery automating the explicit signaling workflow out-of-band natively.
/// </summary>
public sealed class WebRtcHttpPeerDiscovery : IWebRtcHttpPeerDiscovery, IDisposable
{
    private readonly string meshId;
    private readonly IWebRtcSignalingClient signalingClient;
    private readonly IWebRtcInvitationService invitationService;
    private readonly IPeerRegistry peerRegistry;
    private readonly IPeerAuthenticator authenticator;
    private readonly IFailureDetector failureDetector;
    private readonly ILogger<WebRtcHttpPeerDiscovery> logger;

    private readonly Meter meter;
    private readonly Counter<long> discoveryAttemptsCounter;

    public WebRtcHttpPeerDiscovery(
        string meshId,
        IWebRtcSignalingClient signalingClient,
        IWebRtcInvitationService invitationService,
        IPeerRegistry peerRegistry,
        IPeerAuthenticator authenticator,
        IFailureDetector failureDetector,
        ILogger<WebRtcHttpPeerDiscovery> logger,
        IMeterFactory? meterFactory = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(signalingClient);
        ArgumentNullException.ThrowIfNull(invitationService);
        ArgumentNullException.ThrowIfNull(peerRegistry);
        ArgumentNullException.ThrowIfNull(authenticator);
        ArgumentNullException.ThrowIfNull(failureDetector);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.signalingClient = signalingClient;
        this.invitationService = invitationService;
        this.peerRegistry = peerRegistry;
        this.authenticator = authenticator;
        this.failureDetector = failureDetector;
        this.logger = logger;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.WebRtcHttpPeerDiscovery") ?? new Meter("Ama.Enterprise.P2p.WebRtcHttpPeerDiscovery");
        this.discoveryAttemptsCounter = this.meter.CreateCounter<long>(
            "p2p.webrtc.discovery.attempts", 
            "attempts", 
            "Total WebRTC isolated HTTP peer discovery bounds explicitly orchestrating negotiations natively");
    }

    public async Task<bool> DiscoverPeerAsync(Uri peerUri, string? pathPrefix = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(peerUri);

        logger.LogTrace("[{MeshId}] Initiating explicit out-of-band WebRTC discovery for peer URI: {PeerUri}", meshId, peerUri);
        bool success = false;

        try
        {
            var remoteOffer = await signalingClient.RequestOfferAsync(peerUri, meshId, pathPrefix, cancellationToken).ConfigureAwait(false);
            if (remoteOffer == null)
            {
                logger.LogWarning("[{MeshId}] Failed to retrieve explicit WebRTC offer from {PeerUri}.", meshId, peerUri);
                return false;
            }

            var localAnswer = await invitationService.AcceptInvitationAsync(remoteOffer.Value.SdpOffer, cancellationToken).ConfigureAwait(false);
            var answerDto = new WebRtcInvitationAnswer(remoteOffer.Value.ConnectionId, localAnswer.SdpAnswer);

            var isFinalized = await signalingClient.FinalizeInvitationAsync(peerUri, meshId, pathPrefix, answerDto, cancellationToken).ConfigureAwait(false);
            if (!isFinalized)
            {
                logger.LogWarning("[{MeshId}] Remote peer {PeerUri} rejected the WebRTC finalization explicitly.", meshId, peerUri);
                return false;
            }

            var remoteEndpoint = new WebRtcPeerEndpoint(remoteOffer.Value.ConnectionId);
            var remoteNode = new PeerNode(new PeerId(remoteOffer.Value.ConnectionId), remoteEndpoint);

            var isAuthenticated = await authenticator.AuthenticateAsync(remoteNode, ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
            if (isAuthenticated)
            {
                await failureDetector.RecordHeartbeatAsync(remoteNode.Id, cancellationToken).ConfigureAwait(false);
                await peerRegistry.AddOrUpdatePeerAsync(meshId, remoteNode, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
            }

            logger.LogInformation("[{MeshId}] Orchestrated WebRTC signaling mapping connection natively to {PeerUri}.", meshId, peerUri);
            success = true;
            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Exception encountered evaluating WebRTC peer discovery natively for {PeerUri}.", meshId, peerUri);
            return false;
        }
        finally
        {
            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("success", success) };
            discoveryAttemptsCounter.Add(1, tags);
        }
    }

    public void Dispose()
    {
        meter.Dispose();
    }
}