namespace Ama.Enterprise.P2p.Services.Gossip;

using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implements outbound gossip transport using HTTP POST requests for a specific mesh context.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="HttpTransport"/> class.
/// </remarks>
public sealed class HttpTransport(
    string meshId,
    IHttpClientFactory httpClientFactory,
    ICrdtSerializer serializer,
    ILogger<HttpTransport> logger) : ITransport<GossipMessage>
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IHttpClientFactory httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
    private readonly ICrdtSerializer serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly ILogger<HttpTransport> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task SendAsync(PeerEndpoint endpoint, GossipMessage message, CancellationToken cancellationToken)
    {
        if (endpoint is not HttpPeerEndpoint httpEndpoint)
        {
            logger.LogWarning("[{MeshId}] Cannot send HTTP message. Target endpoint is not an HttpPeerEndpoint: {Type}", meshId, endpoint.GetType().Name);
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

        var url = $"http://{httpEndpoint.Host}:{httpEndpoint.Port}/p2p/gossip";
        
        using var client = httpClientFactory.CreateClient("P2pTransport");
        client.Timeout = TimeSpan.FromSeconds(5);

        var payload = serializer.SerializeToBytes(message);
        using var content = new ByteArrayContent(payload);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

        using var request = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = content
        };

        request.Headers.Add("X-P2P-Protocol-Version", Constants.ProtocolVersion);

        logger.LogTrace("[{MeshId}] Sending message {MessageId} to {Url}", meshId, message.MessageId, url);

        using var response = await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
    }
}