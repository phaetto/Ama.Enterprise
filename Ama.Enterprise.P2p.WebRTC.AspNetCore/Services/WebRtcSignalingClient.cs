namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.WebRTC.Models;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implements the signaling client explicitly interacting with remote isolated WebRTC out-of-band handshakes over HTTP pipelines natively.
/// </summary>
public sealed class WebRtcSignalingClient : IWebRtcSignalingClient, IDisposable
{
    private readonly IHttpClientFactory httpClientFactory;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<WebRtcSignalingClient> logger;

    private readonly Meter meter;
    private readonly Counter<long> requestsCounter;
    private readonly Histogram<long> payloadOutHistogram;
    private readonly Histogram<long> payloadInHistogram;

    public WebRtcSignalingClient(
        IHttpClientFactory httpClientFactory,
        ICrdtSerializer serializer,
        ILogger<WebRtcSignalingClient> logger,
        IMeterFactory? meterFactory = null)
    {
        ArgumentNullException.ThrowIfNull(httpClientFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);

        this.httpClientFactory = httpClientFactory;
        this.serializer = serializer;
        this.logger = logger;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.WebRtcSignalingClient") ?? new Meter("Ama.Enterprise.P2p.WebRtcSignalingClient");
        this.requestsCounter = this.meter.CreateCounter<long>(
            "p2p.webrtc.signaling.client.requests", 
            "requests", 
            "Total WebRTC out-of-band signaling requests sent natively");
        this.payloadOutHistogram = this.meter.CreateHistogram<long>(
            "p2p.webrtc.signaling.client.outbound_bytes", 
            "bytes", 
            "Size of outbound WebRTC signaling payload bounds explicitly in bytes");
        this.payloadInHistogram = this.meter.CreateHistogram<long>(
            "p2p.webrtc.signaling.client.inbound_bytes", 
            "bytes", 
            "Size of inbound WebRTC signaling payload bounds explicitly in bytes");
    }

    public async Task<WebRtcInvitationOffer?> RequestOfferAsync(Uri peerUri, string meshId, string? pathPrefix = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(peerUri);
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);

        var url = BuildUrl(peerUri, pathPrefix, meshId, "offer");
        
        using var client = httpClientFactory.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, url);
        
        try
        {
            using var response = await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            var responseBytes = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
            
            payloadInHistogram.Record(responseBytes.Length, new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("action", "offer") });
            requestsCounter.Add(1, new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("action", "offer"), new("success", true) });

            return serializer.DeserializeFromBytes<WebRtcInvitationOffer>(responseBytes);
        }
        catch (Exception ex)
        {
            requestsCounter.Add(1, new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("action", "offer"), new("success", false) });
            logger.LogWarning(ex, "[{MeshId}] Failed to request WebRTC offer from explicit peer URI: {PeerUri}.", meshId, peerUri);
            return null;
        }
    }

    public async Task<WebRtcInvitationAnswer?> SendOfferAsync(Uri peerUri, string meshId, string? pathPrefix, WebRtcInvitationOffer offer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(peerUri);
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);

        var url = BuildUrl(peerUri, pathPrefix, meshId, "answer");
        var payloadBytes = serializer.SerializeToBytes(offer);
        
        payloadOutHistogram.Record(payloadBytes.Length, new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("action", "answer") });

        using var client = httpClientFactory.CreateClient();
        using var content = new ByteArrayContent(payloadBytes);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

        using var request = new HttpRequestMessage(HttpMethod.Post, url);
        request.Content = content;

        try
        {
            using var response = await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            var responseBytes = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
            
            payloadInHistogram.Record(responseBytes.Length, new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("action", "answer") });
            requestsCounter.Add(1, new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("action", "answer"), new("success", true) });

            return serializer.DeserializeFromBytes<WebRtcInvitationAnswer>(responseBytes);
        }
        catch (Exception ex)
        {
            requestsCounter.Add(1, new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("action", "answer"), new("success", false) });
            logger.LogWarning(ex, "[{MeshId}] Failed to send WebRTC offer mapping answer explicitly to peer URI: {PeerUri}.", meshId, peerUri);
            return null;
        }
    }

    public async Task<bool> FinalizeInvitationAsync(Uri peerUri, string meshId, string? pathPrefix, WebRtcInvitationAnswer answer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(peerUri);
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);

        var url = BuildUrl(peerUri, pathPrefix, meshId, "finalize");
        var payloadBytes = serializer.SerializeToBytes(answer);

        payloadOutHistogram.Record(payloadBytes.Length, new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("action", "finalize") });

        using var client = httpClientFactory.CreateClient();
        using var content = new ByteArrayContent(payloadBytes);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

        using var request = new HttpRequestMessage(HttpMethod.Post, url);
        request.Content = content;

        try
        {
            using var response = await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
            var success = response.IsSuccessStatusCode;
            
            requestsCounter.Add(1, new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("action", "finalize"), new("success", success) });
            return success;
        }
        catch (Exception ex)
        {
            requestsCounter.Add(1, new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("action", "finalize"), new("success", false) });
            logger.LogWarning(ex, "[{MeshId}] Failed to finalize WebRTC invitation evaluating peer URI: {PeerUri}.", meshId, peerUri);
            return false;
        }
    }

    public void Dispose()
    {
        meter.Dispose();
    }

    private static string BuildUrl(Uri baseUri, string? pathPrefix, string meshId, string action)
    {
        var basePath = string.IsNullOrWhiteSpace(pathPrefix) ? "/ama-enterprise/webrtc-signaling" : pathPrefix.TrimEnd('/');
        if (!basePath.StartsWith("/", StringComparison.Ordinal))
        {
            basePath = "/" + basePath;
        }
        
        var relativePath = $"{basePath}/{meshId}/{action}";
        return new Uri(baseUri, relativePath).ToString();
    }
}