namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;

using System;
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
public sealed class WebRtcSignalingClient : IWebRtcSignalingClient
{
    private readonly IHttpClientFactory httpClientFactory;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<WebRtcSignalingClient> logger;

    public WebRtcSignalingClient(
        IHttpClientFactory httpClientFactory,
        ICrdtSerializer serializer,
        ILogger<WebRtcSignalingClient> logger)
    {
        ArgumentNullException.ThrowIfNull(httpClientFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);

        this.httpClientFactory = httpClientFactory;
        this.serializer = serializer;
        this.logger = logger;
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
            return serializer.DeserializeFromBytes<WebRtcInvitationOffer>(responseBytes);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to request WebRTC offer from explicit peer URI: {PeerUri}.", meshId, peerUri);
            return null;
        }
    }

    public async Task<WebRtcInvitationAnswer?> SendOfferAsync(Uri peerUri, string meshId, string? pathPrefix, WebRtcInvitationOffer offer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(peerUri);
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(offer);

        var url = BuildUrl(peerUri, pathPrefix, meshId, "answer");
        var payloadBytes = serializer.SerializeToBytes(offer);

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
            return serializer.DeserializeFromBytes<WebRtcInvitationAnswer>(responseBytes);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to send WebRTC offer mapping answer explicitly to peer URI: {PeerUri}.", meshId, peerUri);
            return null;
        }
    }

    public async Task<bool> FinalizeInvitationAsync(Uri peerUri, string meshId, string? pathPrefix, WebRtcInvitationAnswer answer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(peerUri);
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(answer);

        var url = BuildUrl(peerUri, pathPrefix, meshId, "finalize");
        var payloadBytes = serializer.SerializeToBytes(answer);

        using var client = httpClientFactory.CreateClient();
        using var content = new ByteArrayContent(payloadBytes);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

        using var request = new HttpRequestMessage(HttpMethod.Post, url);
        request.Content = content;

        try
        {
            using var response = await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to finalize WebRTC invitation evaluating peer URI: {PeerUri}.", meshId, peerUri);
            return false;
        }
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