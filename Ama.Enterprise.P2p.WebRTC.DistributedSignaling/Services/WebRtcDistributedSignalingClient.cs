namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Services;

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.WebRTC.Models;

/// <summary>
/// Implementation of the HTTP client that abstracts interaction mapping signaling drop-boxes across active meshes natively via ICrdtSerializer boundaries.
/// </summary>
public sealed class WebRtcDistributedSignalingClient : IWebRtcDistributedSignalingClient
{
    private readonly IHttpClientFactory httpClientFactory;
    private readonly ICrdtSerializer serializer;
    private const string BaseRoutePrefix = "/ama-enterprise/webrtc-signaling";

    /// <summary>
    /// Initializes a new instance of the <see cref="WebRtcDistributedSignalingClient"/> class.
    /// </summary>
    /// <param name="httpClientFactory">The factory to create HTTP clients.</param>
    /// <param name="serializer">The centralized generic CRDT serializer tracking native byte mappings.</param>
    public WebRtcDistributedSignalingClient(IHttpClientFactory httpClientFactory, ICrdtSerializer serializer)
    {
        this.httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, long>> GetJoinIntentsAsync(string meshId, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);

        var url = BuildUrl(meshId, "intents", documentId);
        using var httpClient = httpClientFactory.CreateClient(nameof(IWebRtcDistributedSignalingClient));
        using var response = await httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        var bytes = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
        if (bytes.Length == 0) return new Dictionary<string, long>();

        return serializer.DeserializeFromBytes<IReadOnlyDictionary<string, long>>(bytes)
               ?? new Dictionary<string, long>();
    }

    /// <inheritdoc />
    public async Task SetJoinIntentAsync(string meshId, Guid peerId, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);

        var url = BuildUrl(meshId, $"intents/{peerId}", documentId);
        using var httpClient = httpClientFactory.CreateClient(nameof(IWebRtcDistributedSignalingClient));
        using var response = await httpClient.PostAsync(url, null, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
    }

    /// <inheritdoc />
    public async Task RemoveJoinIntentAsync(string meshId, Guid peerId, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);

        var url = BuildUrl(meshId, $"intents/{peerId}", documentId);
        using var httpClient = httpClientFactory.CreateClient(nameof(IWebRtcDistributedSignalingClient));
        using var response = await httpClient.DeleteAsync(url, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, WebRtcInvitationOffer>> GetOffersAsync(string meshId, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);

        var url = BuildUrl(meshId, "offers", documentId);
        using var httpClient = httpClientFactory.CreateClient(nameof(IWebRtcDistributedSignalingClient));
        using var response = await httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        var bytes = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
        if (bytes.Length == 0) return new Dictionary<string, WebRtcInvitationOffer>();

        return serializer.DeserializeFromBytes<IReadOnlyDictionary<string, WebRtcInvitationOffer>>(bytes)
               ?? new Dictionary<string, WebRtcInvitationOffer>();
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, WebRtcInvitationAnswer>> GetAnswersAsync(string meshId, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);

        var url = BuildUrl(meshId, "answers", documentId);
        using var httpClient = httpClientFactory.CreateClient(nameof(IWebRtcDistributedSignalingClient));
        using var response = await httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        var bytes = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
        if (bytes.Length == 0) return new Dictionary<string, WebRtcInvitationAnswer>();

        return serializer.DeserializeFromBytes<IReadOnlyDictionary<string, WebRtcInvitationAnswer>>(bytes)
               ?? new Dictionary<string, WebRtcInvitationAnswer>();
    }

    /// <inheritdoc />
    public async Task SetOfferAsync(string meshId, string routingKey, WebRtcInvitationOffer offer, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);

        var url = BuildUrl(meshId, "offers", documentId, routingKey);
        var bytes = serializer.SerializeToBytes(offer);
        
        using var content = new ByteArrayContent(bytes);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        
        using var httpClient = httpClientFactory.CreateClient(nameof(IWebRtcDistributedSignalingClient));
        using var response = await httpClient.PostAsync(url, content, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
    }

    /// <inheritdoc />
    public async Task RemoveOfferAsync(string meshId, string routingKey, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);

        var url = BuildUrl(meshId, $"offers/{Uri.EscapeDataString(routingKey)}", documentId);
        using var httpClient = httpClientFactory.CreateClient(nameof(IWebRtcDistributedSignalingClient));
        using var response = await httpClient.DeleteAsync(url, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
    }

    /// <inheritdoc />
    public async Task SetAnswerAsync(string meshId, string routingKey, WebRtcInvitationAnswer answer, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);

        var url = BuildUrl(meshId, "answers", documentId, routingKey);
        var bytes = serializer.SerializeToBytes(answer);
        
        using var content = new ByteArrayContent(bytes);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        
        using var httpClient = httpClientFactory.CreateClient(nameof(IWebRtcDistributedSignalingClient));
        using var response = await httpClient.PostAsync(url, content, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
    }

    /// <inheritdoc />
    public async Task RemoveAnswerAsync(string meshId, string routingKey, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);

        var url = BuildUrl(meshId, $"answers/{Uri.EscapeDataString(routingKey)}", documentId);
        using var httpClient = httpClientFactory.CreateClient(nameof(IWebRtcDistributedSignalingClient));
        using var response = await httpClient.DeleteAsync(url, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
    }

    private static string BuildUrl(string meshId, string path, string documentId, string? routingKey = null)
    {
        var url = $"{BaseRoutePrefix}/{Uri.EscapeDataString(meshId)}/{path}?documentId={Uri.EscapeDataString(documentId)}";
        if (!string.IsNullOrWhiteSpace(routingKey))
        {
            url += $"&routingKey={Uri.EscapeDataString(routingKey)}";
        }
        return url;
    }
}