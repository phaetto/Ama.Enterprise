namespace Ama.Enterprise.P2p.AspNetCore.Services.Discovery;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.Licensing.Services;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation of IPeerHandshaker managing isolated ASP.NET Core HTTP probes.
/// </summary>
public sealed class AspNetCorePeerHandshaker : IPeerHandshaker, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<AspNetCoreHandshakeOptions> optionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly PeerEndpoint localEndpoint;
    private readonly IHttpClientFactory httpClientFactory;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<AspNetCorePeerHandshaker> logger;
    private readonly IPeerAuthenticator authenticator;
    private readonly IPeerRegistry peerRegistry;
    private readonly IFailureDetector failureDetector;
    private readonly ICertificateLoader? certificateLoader;

    private readonly Meter meter;
    private readonly Counter<long> handshakesSentCounter;
    private readonly Counter<long> handshakesReceivedCounter;

    private IHost? webHost;
    private bool isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="AspNetCorePeerHandshaker"/> class.
    /// </summary>
    public AspNetCorePeerHandshaker(
        string meshId,
        IOptionsMonitor<AspNetCoreHandshakeOptions> optionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        PeerEndpoint localEndpoint,
        IHttpClientFactory httpClientFactory,
        ICrdtSerializer serializer,
        ILogger<AspNetCorePeerHandshaker> logger,
        IPeerAuthenticator authenticator,
        IPeerRegistry peerRegistry,
        IFailureDetector failureDetector,
        IMeterFactory? meterFactory = null,
        ICertificateLoader? certificateLoader = null)
    {
        ArgumentNullException.ThrowIfNull(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(nodeOptionsMonitor);
        ArgumentNullException.ThrowIfNull(localEndpoint);
        ArgumentNullException.ThrowIfNull(httpClientFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(authenticator);
        ArgumentNullException.ThrowIfNull(peerRegistry);
        ArgumentNullException.ThrowIfNull(failureDetector);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.nodeOptionsMonitor = nodeOptionsMonitor;
        this.localEndpoint = localEndpoint;
        this.httpClientFactory = httpClientFactory;
        this.serializer = serializer;
        this.logger = logger;
        this.authenticator = authenticator;
        this.peerRegistry = peerRegistry;
        this.failureDetector = failureDetector;
        this.certificateLoader = certificateLoader;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.AspNetCorePeerHandshaker") ?? new Meter("Ama.Enterprise.P2p.AspNetCorePeerHandshaker");
        this.handshakesSentCounter = this.meter.CreateCounter<long>(
            "p2p.discovery.aspnetcore.handshakes_sent", 
            "handshakes", 
            "Total handshakes sent via ASP.NET Core");
        this.handshakesReceivedCounter = this.meter.CreateCounter<long>(
            "p2p.discovery.aspnetcore.handshakes_received", 
            "handshakes", 
            "Total handshakes received via ASP.NET Core");
    }

    /// <inheritdoc />
    public int LocalHandshakePort => optionsMonitor.Get(meshId).AdvertisedHandshakePort;

    /// <inheritdoc />
    public async Task StartListeningAsync(CancellationToken cancellationToken)
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
                        X509Certificate2? cert = null;
                        if (options.UseHttpsStandalone)
                        {
                            cert = LoadCertificate(options.CertificateFilePath, options.CertificatePassword, options.CertificateThumbprint);
                        }

                        Action<Microsoft.AspNetCore.Server.Kestrel.Core.ListenOptions> configureListen = listenOptions =>
                        {
                            if (options.UseHttpsStandalone && cert != null) listenOptions.UseHttps(cert);
                        };

                        if (string.IsNullOrWhiteSpace(options.StandaloneListenHost) || options.StandaloneListenHost == "+" || options.StandaloneListenHost == "0.0.0.0")
                        {
                            serverOptions.ListenAnyIP(options.StandaloneListenPort, configureListen);
                        }
                        else if (options.StandaloneListenHost.Equals("localhost", StringComparison.OrdinalIgnoreCase))
                        {
                            serverOptions.ListenLocalhost(options.StandaloneListenPort, configureListen);
                        }
                        else if (IPAddress.TryParse(options.StandaloneListenHost, out var ipAddress))
                        {
                            serverOptions.Listen(ipAddress, options.StandaloneListenPort, configureListen);
                        }
                        else
                        {
                            logger.LogWarning("[{MeshId}] Invalid Kestrel ListenHost '{Host}', falling back to Any IP.", meshId, options.StandaloneListenHost);
                            serverOptions.ListenAnyIP(options.StandaloneListenPort, configureListen);
                        }
                    });
                    
                    webBuilder.Configure(app =>
                    {
                        var basePath = string.IsNullOrWhiteSpace(options.PathPrefix) ? "/ama-enterprise/p2p-handshake" : options.PathPrefix.TrimEnd('/');
                        if (!basePath.StartsWith("/", StringComparison.Ordinal)) basePath = "/" + basePath;
                        var path = $"{basePath}/{meshId}";

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
            logger.LogInformation("[{MeshId}] ASP.NET Core Standalone Peer Handshaker started listening on {Host}:{Port}", meshId, options.StandaloneListenHost, options.StandaloneListenPort);
        }
        else
        {
            logger.LogInformation("[{MeshId}] ASP.NET Core Integrated Peer Handshaker bound. Relying on host application pipeline invoking MapP2pMeshHandshakes().", meshId);
        }
    }

    /// <inheritdoc />
    public async Task StopListeningAsync(CancellationToken cancellationToken)
    {
        if (webHost is not null)
        {
            await webHost.StopAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<PeerHandshakePayload?> HandshakeAsync(PeerHandshakePayload localPayload, IPEndPoint endpoint, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        ArgumentNullException.ThrowIfNull(endpoint);

        var options = optionsMonitor.Get(meshId);
        var client = httpClientFactory.CreateClient($"{meshId}_P2pAspNetCoreHandshaker");
        
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(options.HandshakeTimeout);

        try
        {
            var basePath = string.IsNullOrWhiteSpace(options.PathPrefix) ? "/ama-enterprise/p2p-handshake" : options.PathPrefix.TrimEnd('/');
            if (!basePath.StartsWith("/", StringComparison.Ordinal)) basePath = "/" + basePath;
            var path = $"{basePath}/{meshId}";

            var scheme = options.UseHttps ? "https" : "http";
            var uri = new Uri($"{scheme}://{endpoint.Address}:{endpoint.Port}{path}");
            
            var requestBytes = serializer.SerializeToBytes(localPayload);
            using var content = new ByteArrayContent(requestBytes);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

            using var response = await client.PostAsync(uri, content, timeoutCts.Token).ConfigureAwait(false);
            if (response.IsSuccessStatusCode)
            {
                var responseBytes = await response.Content.ReadAsByteArrayAsync(timeoutCts.Token).ConfigureAwait(false);
                var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
                handshakesSentCounter.Add(1, tags);
                return serializer.DeserializeFromBytes<PeerHandshakePayload>(responseBytes);
            }
            
            logger.LogTrace("[{MeshId}] ASP.NET Core handshake returned status {StatusCode} routing {Target}.", meshId, response.StatusCode, uri);
            return null;
        }
        catch (OperationCanceledException)
        {
            logger.LogTrace("[{MeshId}] ASP.NET Core handshake disconnected by timeout for {Target}.", meshId, endpoint);
            return null;
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "[{MeshId}] ASP.NET Core handshake aborted for {Target}.", meshId, endpoint);
            return null;
        }
    }

    /// <summary>
    /// Processes inbound HTTP requests extracting generic discovery profiles.
    /// </summary>
    /// <param name="context">The HTTP context executing decoupled standard inbound bounds.</param>
    public async Task HandleHandshakeRequestAsync(HttpContext context)
    {
        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        try
        {
            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
            handshakesReceivedCounter.Add(1, tags);

            using var ms = new MemoryStream();
            await context.Request.Body.CopyToAsync(ms).ConfigureAwait(false);
            var remotePayload = serializer.DeserializeFromBytes<PeerHandshakePayload>(ms.ToArray());

            if (remotePayload.Node.Id.Value != nodeOptions.LocalPeerId && remotePayload.Node.Id.Value != Guid.Empty)
            {
                var localNode = new PeerNode(new PeerId(nodeOptions.LocalPeerId), localEndpoint);
                var localHandshakeData = await authenticator.GetLocalHandshakeDataAsync(context.RequestAborted).ConfigureAwait(false);
                var responsePayloadStruct = new PeerHandshakePayload
                {
                    Node = localNode,
                    HandshakeData = localHandshakeData.ToArray()
                };
                
                var responseBytes = serializer.SerializeToBytes(responsePayloadStruct);
                
                context.Response.StatusCode = StatusCodes.Status200OK;
                context.Response.ContentType = "application/octet-stream";
                context.Response.ContentLength = responseBytes.Length;
                await context.Response.Body.WriteAsync(responseBytes, context.RequestAborted).ConfigureAwait(false);

                try
                {
                    var isAuthenticated = await authenticator.AuthenticateAsync(remotePayload.Node, remotePayload.HandshakeData, context.RequestAborted).ConfigureAwait(false);

                    if (isAuthenticated)
                    {
                        await failureDetector.RecordHeartbeatAsync(remotePayload.Node.Id, context.RequestAborted).ConfigureAwait(false);
                        await peerRegistry.AddOrUpdatePeerAsync(meshId, remotePayload.Node, PeerStatus.Active, context.RequestAborted).ConfigureAwait(false);
                    }
                }
                catch (Exception authEx)
                {
                    logger.LogWarning(authEx, "[{MeshId}] Inbound ASP.NET Core handshake authentication failed for {PeerId}.", meshId, remotePayload.Node.Id.Value);
                }
            }
            else
            {
                context.Response.StatusCode = StatusCodes.Status409Conflict;
            }
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "[{MeshId}] Failed to process inbound ASP.NET Core handshake request.", meshId);
            context.Response.StatusCode = StatusCodes.Status500InternalServerError;
        }
    }

    private X509Certificate2? LoadCertificate(string? path, string? password, string? thumbprint)
    {
        if (certificateLoader == null)
        {
            logger.LogWarning("[{MeshId}] ICertificateLoader is not registered. Cannot configure HTTPS.", meshId);
            return null;
        }

        if (!string.IsNullOrWhiteSpace(thumbprint))
        {
            var cert = certificateLoader.LoadFromStore(thumbprint);
            if (cert != null) return cert;
        }

        if (!string.IsNullOrWhiteSpace(path))
        {
            var cert = certificateLoader.LoadFromFile(path, password);
            if (cert != null) return cert;
        }

        logger.LogWarning("[{MeshId}] Failed to resolve valid X509 certificate configurations for HTTPS binding.", meshId);
        return null;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (isDisposed)
        {
            return;
        }

        meter.Dispose();
        webHost?.Dispose();
        isDisposed = true;
    }
}