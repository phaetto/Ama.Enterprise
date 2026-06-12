namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Models;
using Ama.Enterprise.P2p.WebRTC.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements the signaling client explicitly interacting with remote isolated WebRTC out-of-band handshakes over full-duplex WebSockets natively.
/// </summary>
public sealed class WebRtcSignalingClient : IWebRtcSignalingClient, IDisposable
{
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<WebRtcSignalingClient> logger;
    private readonly IOptionsMonitor<WebRtcSignalingOptions> optionsMonitor;

    private readonly Meter meter;
    private readonly Counter<long> requestsCounter;
    private readonly Histogram<long> payloadOutHistogram;
    private readonly Histogram<long> payloadInHistogram;

    public WebRtcSignalingClient(
        ICrdtSerializer serializer,
        ILogger<WebRtcSignalingClient> logger,
        IOptionsMonitor<WebRtcSignalingOptions> optionsMonitor,
        IMeterFactory? meterFactory = null)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(optionsMonitor);

        this.serializer = serializer;
        this.logger = logger;
        this.optionsMonitor = optionsMonitor;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.WebRtcSignalingClient") ?? new Meter("Ama.Enterprise.P2p.WebRtcSignalingClient");
        this.requestsCounter = this.meter.CreateCounter<long>(
            "p2p.webrtc.signaling.client.requests", 
            "requests", 
            "Total WebRTC out-of-band WebSocket signaling sessions sent natively");
        this.payloadOutHistogram = this.meter.CreateHistogram<long>(
            "p2p.webrtc.signaling.client.outbound_bytes", 
            "bytes", 
            "Size of outbound WebRTC signaling WebSocket payload bounds explicitly in bytes");
        this.payloadInHistogram = this.meter.CreateHistogram<long>(
            "p2p.webrtc.signaling.client.inbound_bytes", 
            "bytes", 
            "Size of inbound WebRTC signaling WebSocket payload bounds explicitly in bytes");
    }

    public async Task<string?> NegotiateOfferAsync(Uri peerUri, string meshId, string? pathPrefix, Func<WebRtcInvitationOffer, CancellationToken, Task<WebRtcInvitationAnswer>> answerFactory, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(peerUri);
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(answerFactory);

        var options = optionsMonitor.Get(meshId);
        var actualPathPrefix = string.IsNullOrWhiteSpace(pathPrefix) ? options.PathPrefix : pathPrefix;
        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("transport", "websocket") };
        var wsUri = BuildWebSocketUri(peerUri, actualPathPrefix, meshId);
        
        using var webSocket = new ClientWebSocket();
        
        if (options.IgnoreOutboundSslErrors)
        {
            webSocket.Options.RemoteCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;
        }

        try
        {
            await webSocket.ConnectAsync(wsUri, cancellationToken).ConfigureAwait(false);
            
            await WebRtcSignalingWsHelper.SendMessageAsync(webSocket, WebRtcSignalingAction.RequestOffer, Array.Empty<byte>(), cancellationToken).ConfigureAwait(false);
            payloadOutHistogram.Record(0, tags);

            var (action, payload) = await WebRtcSignalingWsHelper.ReceiveMessageAsync(webSocket, cancellationToken).ConfigureAwait(false);
            payloadInHistogram.Record(payload.Length, tags);

            if (action != WebRtcSignalingAction.Offer) 
            {
                return null;
            }

            var offer = serializer.DeserializeFromBytes<WebRtcInvitationOffer>(payload);
            var answer = await answerFactory(offer, cancellationToken).ConfigureAwait(false);
            
            var answerPayload = serializer.SerializeToBytes(answer);
            await WebRtcSignalingWsHelper.SendMessageAsync(webSocket, WebRtcSignalingAction.Answer, answerPayload, cancellationToken).ConfigureAwait(false);
            payloadOutHistogram.Record(answerPayload.Length, tags);

            var (ackAction, ackPayload) = await WebRtcSignalingWsHelper.ReceiveMessageAsync(webSocket, cancellationToken).ConfigureAwait(false);
            payloadInHistogram.Record(ackPayload.Length, tags);

            if (ackAction == WebRtcSignalingAction.FinalizeAck)
            {
                if (webSocket.State == WebSocketState.Open || webSocket.State == WebSocketState.CloseReceived)
                {
                    await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Negotiation Complete", cancellationToken).ConfigureAwait(false);
                }

                requestsCounter.Add(1, new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("success", true) });
                return offer.ConnectionId.ToString();
            }

            return null;
        }
        catch (Exception ex)
        {
            requestsCounter.Add(1, new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("success", false) });
            logger.LogWarning(ex, "[{MeshId}] Failed to negotiate explicit WebRTC out-of-band WebSockets signaling against {PeerUri}.", meshId, peerUri);
            return null;
        }
    }

    public void Dispose()
    {
        meter.Dispose();
    }

    private static Uri BuildWebSocketUri(Uri baseUri, string pathPrefix, string meshId)
    {
        var scheme = baseUri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase) ? "wss" : "ws";
        var basePath = string.IsNullOrWhiteSpace(pathPrefix) ? "/ama-enterprise/webrtc-signaling" : pathPrefix.TrimEnd('/');
        if (!basePath.StartsWith("/", StringComparison.Ordinal))
        {
            basePath = "/" + basePath;
        }
        
        var relativePath = $"{basePath}/{meshId}/ws";
        return new Uri($"{scheme}://{baseUri.Host}:{baseUri.Port}{relativePath}");
    }
}