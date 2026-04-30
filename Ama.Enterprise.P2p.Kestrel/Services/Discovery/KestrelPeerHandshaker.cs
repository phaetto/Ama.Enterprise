namespace Ama.Enterprise.P2p.Kestrel.Services.Discovery;

using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Kestrel.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation of IPeerHandshaker managing isolated Kestrel HTTP probes.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="KestrelPeerHandshaker"/> class.
/// </remarks>
public sealed class KestrelPeerHandshaker(
    string meshId,
    IOptionsMonitor<KestrelHandshakeOptions> optionsMonitor,
    IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
    PeerEndpoint localEndpoint,
    IHttpClientFactory httpClientFactory,
    ICrdtSerializer serializer,
    ILogger<KestrelPeerHandshaker> logger) : IPeerHandshaker, IHostedService, IDisposable
{
    private IHost? webHost;
    private bool isDisposed;

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        var options = optionsMonitor.Get(meshId);

        var builder = new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder.UseKestrel(serverOptions =>
                {
                    if (string.IsNullOrWhiteSpace(options.ListenHost) || options.ListenHost == "+" || options.ListenHost == "0.0.0.0")
                    {
                        serverOptions.ListenAnyIP(options.ListenPort);
                    }
                    else if (options.ListenHost.Equals("localhost", StringComparison.OrdinalIgnoreCase))
                    {
                        serverOptions.ListenLocalhost(options.ListenPort);
                    }
                    else if (IPAddress.TryParse(options.ListenHost, out var ipAddress))
                    {
                        serverOptions.Listen(ipAddress, options.ListenPort);
                    }
                    else
                    {
                        logger.LogWarning("[{MeshId}] Invalid Kestrel ListenHost '{Host}', falling back to Any IP.", meshId, options.ListenHost);
                        serverOptions.ListenAnyIP(options.ListenPort);
                    }
                });
                
                webBuilder.Configure(app =>
                {
                    app.Run(async context =>
                    {
                        if (context.Request.Path == $"/{meshId}/handshake" && context.Request.Method == HttpMethods.Post)
                        {
                            await HandleHandshakeRequestAsync(context).ConfigureAwait(false);
                        }
                        else
                        {
                            context.Response.StatusCode = StatusCodes.Status404NotFound;
                        }
                    });
                });
            });

        webHost = builder.Build();
        await webHost.StartAsync(cancellationToken).ConfigureAwait(false);
        logger.LogInformation("[{MeshId}] Kestrel Peer Handshaker started listening on {Host}:{Port}", meshId, options.ListenHost, options.ListenPort);
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (webHost is not null)
        {
            await webHost.StopAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<PeerNode?> HandshakeAsync(PeerNode localNode, EndPoint targetEndpoint, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        ArgumentNullException.ThrowIfNull(targetEndpoint);

        string host;
        int port;

        if (targetEndpoint is DnsEndPoint dnsEndPoint)
        {
            host = dnsEndPoint.Host;
            port = dnsEndPoint.Port;
        }
        else if (targetEndpoint is IPEndPoint ipEndPoint)
        {
            host = ipEndPoint.Address.ToString();
            port = ipEndPoint.Port;
        }
        else
        {
            logger.LogWarning("[{MeshId}] Target endpoint must be DnsEndPoint or IPEndPoint for Kestrel handshakes.", meshId);
            return null;
        }

        var options = optionsMonitor.Get(meshId);
        var client = httpClientFactory.CreateClient("P2pKestrelHandshaker");
        
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(options.HandshakeTimeout);

        try
        {
            var uri = new Uri($"http://{host}:{port}/{meshId}/handshake");
            var requestBytes = serializer.SerializeToBytes(localNode);
            using var content = new ByteArrayContent(requestBytes);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

            using var response = await client.PostAsync(uri, content, timeoutCts.Token).ConfigureAwait(false);
            if (response.IsSuccessStatusCode)
            {
                var responseBytes = await response.Content.ReadAsByteArrayAsync(timeoutCts.Token).ConfigureAwait(false);
                return serializer.DeserializeFromBytes<PeerNode>(responseBytes);
            }
            
            logger.LogTrace("[{MeshId}] Kestrel handshake returned status {StatusCode} for {Target}.", meshId, response.StatusCode, uri);
            return null;
        }
        catch (OperationCanceledException)
        {
            logger.LogTrace("[{MeshId}] Kestrel handshake timed out for {Target}.", meshId, targetEndpoint);
            return null;
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "[{MeshId}] Kestrel handshake failed for {Target}.", meshId, targetEndpoint);
            return null;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (isDisposed)
        {
            return;
        }

        webHost?.Dispose();
        isDisposed = true;
    }

    private async Task HandleHandshakeRequestAsync(HttpContext context)
    {
        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        try
        {
            using var ms = new MemoryStream();
            await context.Request.Body.CopyToAsync(ms).ConfigureAwait(false);
            var remoteNode = serializer.DeserializeFromBytes<PeerNode>(ms.ToArray());

            if (remoteNode.Id.Value != nodeOptions.LocalPeerId && remoteNode.Id.Value != Guid.Empty)
            {
                var localNode = new PeerNode(new PeerId(nodeOptions.LocalPeerId), localEndpoint);
                var responseBytes = serializer.SerializeToBytes(localNode);
                
                context.Response.StatusCode = StatusCodes.Status200OK;
                context.Response.ContentType = "application/octet-stream";
                context.Response.ContentLength = responseBytes.Length;
                await context.Response.Body.WriteAsync(responseBytes, context.RequestAborted).ConfigureAwait(false);
            }
            else
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
            }
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "[{MeshId}] Failed to process inbound Kestrel handshake request.", meshId);
            context.Response.StatusCode = StatusCodes.Status500InternalServerError;
        }
    }
}