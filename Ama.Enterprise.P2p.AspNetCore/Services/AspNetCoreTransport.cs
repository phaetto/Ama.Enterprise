namespace Ama.Enterprise.P2p.AspNetCore.Services;

using System;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements outbound transport using HTTP POST requests mapped specifically to a target ASP.NET Core mesh listener.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="AspNetCoreTransport"/> class.
/// </remarks>
public sealed class AspNetCoreTransport(
    string meshId,
    IOptionsMonitor<AspNetCoreTransportOptions> optionsMonitor,
    IHttpClientFactory httpClientFactory,
    ICrdtSerializer serializer,
    IPeerRegistry peerRegistry,
    ILogger<AspNetCoreTransport> logger) : ITransport
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IOptionsMonitor<AspNetCoreTransportOptions> optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly IHttpClientFactory httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
    private readonly ICrdtSerializer serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly IPeerRegistry peerRegistry = peerRegistry ?? throw new ArgumentNullException(nameof(peerRegistry));
    private readonly ILogger<AspNetCoreTransport> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint)
    {
        if (endpoint == null)
        {
            throw new ArgumentNullException(nameof(endpoint));
        }

        return endpoint is AspNetCorePeerEndpoint;
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

        if (endpoint is not AspNetCorePeerEndpoint targetEndpoint)
        {
            logger.LogWarning("[{MeshId}] Cannot send message via ASP.NET Core. Target endpoint is not an AspNetCorePeerEndpoint: {Type}", meshId, endpoint.GetType().Name);
            return;
        }

        if (string.IsNullOrWhiteSpace(targetEndpoint.Host))
        {
            throw new ArgumentException("Endpoint host cannot be null or empty.", nameof(endpoint));
        }

        if (targetEndpoint.Port is <= 0 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(endpoint), "Endpoint port must be between 1 and 65535.");
        }

        var options = optionsMonitor.Get(meshId);
        if (!options.IsEnabled)
        {
            return;
        }

        var path = string.IsNullOrWhiteSpace(options.PathPrefix) ? "/p2p-mesh" : options.PathPrefix.TrimEnd('/');
        var scheme = options.UseHttps ? "https" : "http";
        
        // Construct the full path expecting the EndpointRouteBuilder mapping: {routePrefix}/{meshId}
        var url = $"{scheme}://{targetEndpoint.Host}:{targetEndpoint.Port}{path}/{meshId}";

        using var client = httpClientFactory.CreateClient("P2pAspNetCoreTransport");
        client.Timeout = TimeSpan.FromSeconds(5);

        var payload = serializer.SerializeToBytes(message);
        using var content = new ByteArrayContent(payload);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

        using var request = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = content
        };
        request.Headers.TryAddWithoutValidation("X-P2P-Protocol-Version", Constants.ProtocolVersion);

        logger.LogTrace("[{MeshId}] Sending message via ASP.NET Core transport to {Url}", meshId, url);

        try
        {
            using var response = await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or TimeoutException)
        {
            logger.LogWarning(ex, "[{MeshId}] ASP.NET Core Transport failure when communicating with {Url}. Removing peer from registry.", meshId, url);
            
            var allPeers = await peerRegistry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
            var deadPeer = allPeers.FirstOrDefault(p => p.Endpoint.Equals(endpoint));

            if (deadPeer.Id.Value != Guid.Empty)
            {
                logger.LogInformation("[{MeshId}] Automatically removing unreachable ASP.NET Core peer {PeerId}.", meshId, deadPeer.Id);
                await peerRegistry.RemovePeerAsync(meshId, deadPeer.Id, cancellationToken).ConfigureAwait(false);
            }

            throw;
        }
    }
}