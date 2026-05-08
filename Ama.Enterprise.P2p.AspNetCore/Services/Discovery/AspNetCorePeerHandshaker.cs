namespace Ama.Enterprise.P2p.AspNetCore.Services.Discovery;

using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.AspNetCore.Models;
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
/// Implementation of IPeerHandshaker managing isolated ASP.NET Core HTTP probes explicitly supporting Integrated and Standalone modes natively decoupled structurally.
/// </summary>
public sealed class AspNetCorePeerHandshaker(
    string meshId,
    IOptionsMonitor<AspNetCoreHandshakeOptions> optionsMonitor,
    IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
    PeerEndpoint localEndpoint,
    IHttpClientFactory httpClientFactory,
    ICrdtSerializer serializer,
    ILogger<AspNetCorePeerHandshaker> logger) : IPeerHandshaker, IHostedService, IDisposable
{
    private IHost? webHost;
    private bool isDisposed;

    /// <inheritdoc />
    public int LocalHandshakePort => optionsMonitor.Get(meshId).AdvertisedHandshakePort;

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        var options = optionsMonitor.Get(meshId);

        if (options.HostingMode == AspNetCoreHostingMode.Standalone)
        {
            var builder = new HostBuilder()
                .ConfigureWebHost(webBuilder =>
                {
                    webBuilder.UseKestrel(serverOptions =>
                    {
                        if (string.IsNullOrWhiteSpace(options.StandaloneListenHost) || options.StandaloneListenHost == "+" || options.StandaloneListenHost == "0.0.0.0")
                        {
                            serverOptions.ListenAnyIP(options.StandaloneListenPort);
                        }
                        else if (options.StandaloneListenHost.Equals("localhost", StringComparison.OrdinalIgnoreCase))
                        {
                            serverOptions.ListenLocalhost(options.StandaloneListenPort);
                        }
                        else if (IPAddress.TryParse(options.StandaloneListenHost, out var ipAddress))
                        {
                            serverOptions.Listen(ipAddress, options.StandaloneListenPort);
                        }
                        else
                        {
                            logger.LogWarning("[{MeshId}] Invalid Kestrel ListenHost '{Host}', falling back to Any IP.", meshId, options.StandaloneListenHost);
                            serverOptions.ListenAnyIP(options.StandaloneListenPort);
                        }
                    });
                    
                    webBuilder.Configure(app =>
                    {
                        var path = options.PathPrefix.TrimEnd('/') + $"/{meshId}";
                        app.Run(async context =>
                        {
                            if (context.Request.Path == path && context.Request.Method == HttpMethods.Post)
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
            logger.LogInformation("[{MeshId}] ASP.NET Core Standalone Peer Handshaker started explicitly listening natively on {Host}:{Port}", meshId, options.StandaloneListenHost, options.StandaloneListenPort);
        }
        else
        {
            logger.LogInformation("[{MeshId}] ASP.NET Core Integrated Peer Handshaker bound implicitly. Relying on host application pipeline invoking MapP2pMeshHandshakes() safely.", meshId);
        }
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
    public async Task<PeerNode?> HandshakeAsync(PeerNode localNode, IPEndPoint endpoint, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        ArgumentNullException.ThrowIfNull(endpoint);

        var options = optionsMonitor.Get(meshId);
        var client = httpClientFactory.CreateClient("P2pAspNetCoreHandshaker");
        
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(options.HandshakeTimeout);

        try
        {
            var path = options.PathPrefix.TrimEnd('/') + $"/{meshId}";
            var uri = new Uri($"http://{endpoint.Address}:{endpoint.Port}{path}");
            
            var requestBytes = serializer.SerializeToBytes(localNode);
            using var content = new ByteArrayContent(requestBytes);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

            using var response = await client.PostAsync(uri, content, timeoutCts.Token).ConfigureAwait(false);
            if (response.IsSuccessStatusCode)
            {
                var responseBytes = await response.Content.ReadAsByteArrayAsync(timeoutCts.Token).ConfigureAwait(false);
                return serializer.DeserializeFromBytes<PeerNode>(responseBytes);
            }
            
            logger.LogTrace("[{MeshId}] ASP.NET Core handshake returned status {StatusCode} routing {Target}.", meshId, response.StatusCode, uri);
            return null;
        }
        catch (OperationCanceledException)
        {
            logger.LogTrace("[{MeshId}] ASP.NET Core handshake disconnected securely by timeout isolating {Target}.", meshId, endpoint);
            return null;
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "[{MeshId}] ASP.NET Core handshake strictly aborted dispatching {Target}.", meshId, endpoint);
            return null;
        }
    }

    /// <summary>
    /// Processes inbound HTTP requests extracting generic discovery profiles mapped actively securely globally natively.
    /// </summary>
    /// <param name="context">The HTTP context executing decoupled standard inbound bounds.</param>
    public async Task HandleHandshakeRequestAsync(HttpContext context)
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
            logger.LogTrace(ex, "[{MeshId}] Failed to process inbound ASP.NET Core explicitly routed handshake request robustly.", meshId);
            context.Response.StatusCode = StatusCodes.Status500InternalServerError;
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
}