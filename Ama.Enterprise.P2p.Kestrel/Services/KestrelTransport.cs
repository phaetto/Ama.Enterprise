namespace Ama.Enterprise.P2p.Kestrel.Services;

using System;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p;
using Ama.Enterprise.P2p.Kestrel.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements isolated outbound transport using HTTP POST requests mapped specifically to a target Kestrel mesh listener.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="KestrelTransport"/> class.
/// </remarks>
public sealed class KestrelTransport(
    string meshId,
    IOptionsMonitor<KestrelTransportOptions> optionsMonitor,
    IHttpClientFactory httpClientFactory,
    ICrdtSerializer serializer,
    IPeerRegistry peerRegistry,
    ILogger<KestrelTransport> logger) : ITransport
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IOptionsMonitor<KestrelTransportOptions> optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly IHttpClientFactory httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
    private readonly ICrdtSerializer serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly IPeerRegistry peerRegistry = peerRegistry ?? throw new ArgumentNullException(nameof(peerRegistry));
    private readonly ILogger<KestrelTransport> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint)
    {
        if (endpoint == null)
        {
            throw new ArgumentNullException(nameof(endpoint));
        }

        return endpoint is KestrelPeerEndpoint;
    }

    /// <inheritdoc />
    public async Task SendAsync(PeerEndpoint endpoint, IMeshMessage message, CancellationToken cancellationToken)
    {
        if (endpoint == null)
        {
            throw new ArgumentNullException(nameof(endpoint));
        }

        if (message == null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        if (!string.Equals(message.MeshId, meshId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Message MeshId '{message.MeshId}' does not match Transport MeshId '{meshId}'.");
        }

        if (endpoint is not KestrelPeerEndpoint kestrelEndpoint)
        {
            logger.LogWarning("[{MeshId}] Cannot send message via Kestrel. Target endpoint is not an KestrelPeerEndpoint: {Type}", meshId, endpoint.GetType().Name);
            return;
        }

        if (string.IsNullOrWhiteSpace(kestrelEndpoint.Host))
        {
            throw new ArgumentException("Endpoint host cannot be null or empty.", nameof(endpoint));
        }

        if (kestrelEndpoint.Port is <= 0 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(endpoint), "Endpoint port must be between 1 and 65535.");
        }

        var options = optionsMonitor.Get(meshId);
        var path = options.PathPrefix?.TrimStart('/') ?? string.Empty;
        var url = $"http://{kestrelEndpoint.Host}:{kestrelEndpoint.Port}/{path}";
        
        using var client = httpClientFactory.CreateClient("P2pKestrelTransport");
        client.Timeout = TimeSpan.FromSeconds(5);

        var payload = serializer.SerializeToBytes(message);
        using var content = new ByteArrayContent(payload);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

        using var request = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = content
        };
        request.Headers.TryAddWithoutValidation("X-P2P-Protocol-Version", Constants.ProtocolVersion);

        logger.LogTrace("[{MeshId}] Sending message via Kestrel transport to {Url}", meshId, url);

        try
        {
            using var response = await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or TimeoutException)
        {
            logger.LogWarning(ex, "[{MeshId}] Kestrel Transport failure when communicating with {Url}. Removing peer from registry.", meshId, url);
            
            var allPeers = await peerRegistry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
            var deadPeer = allPeers.FirstOrDefault(p => p.Endpoint.Equals(endpoint));

            if (deadPeer.Id.Value != Guid.Empty)
            {
                logger.LogInformation("[{MeshId}] Automatically removing unreachable Kestrel peer {PeerId}.", meshId, deadPeer.Id);
                await peerRegistry.RemovePeerAsync(meshId, deadPeer.Id, cancellationToken).ConfigureAwait(false);
            }

            throw;
        }
    }
}