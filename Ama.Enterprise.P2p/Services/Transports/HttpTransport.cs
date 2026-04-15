namespace Ama.Enterprise.P2p.Services.Transports;

using System;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements generalized outbound transport using HTTP POST requests correctly decoupling explicitly securely.
/// </summary>
public sealed class HttpTransport : ITransport
{
    private readonly IOptionsMonitor<HttpTransportOptions> optionsMonitor;
    private readonly IHttpClientFactory httpClientFactory;
    private readonly ICrdtSerializer serializer;
    private readonly IPeerRegistry peerRegistry;
    private readonly ILogger<HttpTransport> logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="HttpTransport"/> class.
    /// </summary>
    public HttpTransport(
        IOptionsMonitor<HttpTransportOptions> optionsMonitor,
        IHttpClientFactory httpClientFactory,
        ICrdtSerializer serializer,
        IPeerRegistry peerRegistry,
        ILogger<HttpTransport> logger)
    {
        this.optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
        this.httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.peerRegistry = peerRegistry ?? throw new ArgumentNullException(nameof(peerRegistry));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint)
    {
        if (endpoint == null)
        {
            throw new ArgumentNullException(nameof(endpoint));
        }

        return endpoint is HttpPeerEndpoint;
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

        if (endpoint is not HttpPeerEndpoint httpEndpoint)
        {
            logger.LogWarning("Cannot send HTTP message. Target endpoint is not an HttpPeerEndpoint: {Type}", endpoint.GetType().Name);
            return;
        }

        if (string.IsNullOrWhiteSpace(httpEndpoint.Host))
        {
            throw new ArgumentException("Endpoint host cannot be null or empty.", nameof(endpoint));
        }

        if (httpEndpoint.Port is <= 0 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(endpoint), "Endpoint port must be between 1 and 65535.");
        }

        var options = optionsMonitor.Get(message.MeshId);
        var path = options.PathPrefix?.TrimStart('/') ?? string.Empty;
        var url = $"http://{httpEndpoint.Host}:{httpEndpoint.Port}/{path}";
        
        using var client = httpClientFactory.CreateClient("P2pTransport");
        client.Timeout = TimeSpan.FromSeconds(5);

        var payload = serializer.SerializeToBytes(message);
        using var content = new ByteArrayContent(payload);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

        using var request = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = content
        };

        logger.LogTrace("[{MeshId}] Sending generalized mapped explicitly wrapped message to {Url}", message.MeshId, url);

        try
        {
            using var response = await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or TimeoutException)
        {
            logger.LogWarning(ex, "[{MeshId}] Transport failure when communicating with {Url}. Removing peer from registry organically.", message.MeshId, url);
            
            var allPeers = await peerRegistry.GetAllPeersAsync(cancellationToken).ConfigureAwait(false);
            var deadPeer = allPeers.FirstOrDefault(p => p.Endpoint.Equals(endpoint));

            if (deadPeer.Id.Value != Guid.Empty)
            {
                logger.LogInformation("[{MeshId}] Automatically removing unreachable explicitly targeted peer {PeerId}.", message.MeshId, deadPeer.Id);
                await peerRegistry.RemovePeerAsync(deadPeer.Id, cancellationToken).ConfigureAwait(false);
            }

            throw;
        }
    }
}