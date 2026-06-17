namespace Ama.Enterprise.P2p.AspNetCore.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements outbound transport using HTTP POST requests mapped specifically to a target ASP.NET Core mesh listener.
/// </summary>
public sealed class AspNetCoreTransport : ITransport, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<AspNetCoreTransportOptions> optionsMonitor;
    private readonly IHttpClientFactory httpClientFactory;
    private readonly IMeshWireEncoder wireEncoder;
    private readonly IPeerRegistry peerRegistry;
    private readonly ILogger<AspNetCoreTransport> logger;

    private readonly Meter meter;
    private readonly Counter<long> messagesSentCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    /// <summary>
    /// Initializes a new instance of the <see cref="AspNetCoreTransport"/> class.
    /// </summary>
    public AspNetCoreTransport(
        string meshId,
        IOptionsMonitor<AspNetCoreTransportOptions> optionsMonitor,
        IHttpClientFactory httpClientFactory,
        IMeshWireEncoder wireEncoder,
        IPeerRegistry peerRegistry,
        ILogger<AspNetCoreTransport> logger,
        IMeterFactory? meterFactory = null)
    {
        ArgumentNullException.ThrowIfNull(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(httpClientFactory);
        ArgumentNullException.ThrowIfNull(wireEncoder);
        ArgumentNullException.ThrowIfNull(peerRegistry);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.httpClientFactory = httpClientFactory;
        this.wireEncoder = wireEncoder;
        this.peerRegistry = peerRegistry;
        this.logger = logger;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.AspNetCoreTransport") ?? new Meter("Ama.Enterprise.P2p.AspNetCoreTransport");
        this.messagesSentCounter = this.meter.CreateCounter<long>(
            "p2p.transport.aspnetcore.messages_sent", 
            "messages", 
            "Total messages sent via ASP.NET Core transport");
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>(
            "p2p.transport.aspnetcore.outbound_payload_bytes", 
            "bytes", 
            "Size of outbound ASP.NET Core payload in bytes");
    }

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        return endpoint is AspNetCorePeerEndpoint;
    }

    /// <inheritdoc />
    public async Task SendAsync(PeerEndpoint endpoint, IMeshMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(message);

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

        var basePath = string.IsNullOrWhiteSpace(options.PathPrefix) ? "/ama-enterprise/p2p-mesh" : options.PathPrefix.TrimEnd('/');
        if (!basePath.StartsWith("/", StringComparison.Ordinal)) basePath = "/" + basePath;
        
        var scheme = options.UseHttps ? "https" : "http";
        var url = $"{scheme}://{targetEndpoint.Host}:{targetEndpoint.Port}{basePath}/{meshId}";

        using var client = httpClientFactory.CreateClient($"{meshId}_P2pAspNetCoreTransport");
        client.Timeout = TimeSpan.FromSeconds(5); // TODO: Add/Use to options

        var payload = wireEncoder.Encode(message);

        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
        messagesSentCounter.Add(1, tags);
        payloadBytesHistogram.Record(payload.Length, tags);

        using var content = new ByteArrayContent(payload);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

        using var request = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = content
        };
        request.Headers.TryAddWithoutValidation("X-P2P-Protocol-Version", Constants.ProtocolVersion);

        logger.LogTrace("[{MeshId}] Sending message via ASP.NET Core transport to explicitly isolated path {Url}", meshId, url);

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

    /// <inheritdoc />
    public void Dispose()
    {
        meter.Dispose();
    }
}