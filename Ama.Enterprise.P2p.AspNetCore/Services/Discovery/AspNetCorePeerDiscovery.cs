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
/// Implementation of IPeerDiscovery using HTTP(S) polling targeting specifically mapped URIs effectively isolating standard bounds correctly natively explicitly properly gracefully rationally intelligently seamlessly.
/// </summary>
public sealed class AspNetCorePeerDiscovery : IPeerDiscovery, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<AspNetCoreDiscoveryOptions> optionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly PeerEndpoint localEndpoint;
    private readonly IHttpClientFactory httpClientFactory;
    private readonly ICrdtSerializer serializer;
    private readonly IPeerAuthenticator authenticator;
    private readonly IFailureDetector failureDetector;
    private readonly ILogger<AspNetCorePeerDiscovery> logger;
    private readonly ICertificateLoader? certificateLoader;

    private readonly Meter meter;
    private readonly Counter<long> discoveryRequestsSentCounter;
    private readonly Counter<long> discoveryRequestsReceivedCounter;
    private readonly Counter<long> peersFoundCounter;

    private IHost? webHost;
    private bool isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="AspNetCorePeerDiscovery"/> class.
    /// </summary>
    public AspNetCorePeerDiscovery(
        string meshId,
        IOptionsMonitor<AspNetCoreDiscoveryOptions> optionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        PeerEndpoint localEndpoint,
        IHttpClientFactory httpClientFactory,
        ICrdtSerializer serializer,
        IPeerAuthenticator authenticator,
        IFailureDetector failureDetector,
        ILogger<AspNetCorePeerDiscovery> logger,
        IMeterFactory? meterFactory = null,
        ICertificateLoader? certificateLoader = null)
    {
        ArgumentNullException.ThrowIfNull(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(nodeOptionsMonitor);
        ArgumentNullException.ThrowIfNull(localEndpoint);
        ArgumentNullException.ThrowIfNull(httpClientFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(authenticator);
        ArgumentNullException.ThrowIfNull(failureDetector);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.nodeOptionsMonitor = nodeOptionsMonitor;
        this.localEndpoint = localEndpoint;
        this.httpClientFactory = httpClientFactory;
        this.serializer = serializer;
        this.authenticator = authenticator;
        this.failureDetector = failureDetector;
        this.logger = logger;
        this.certificateLoader = certificateLoader;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.AspNetCorePeerDiscovery") ?? new Meter("Ama.Enterprise.P2p.AspNetCorePeerDiscovery");
        this.discoveryRequestsSentCounter = this.meter.CreateCounter<long>("p2p.discovery.aspnetcore.requests_sent", "requests", "Total ASP.NET Core discovery requests sent");
        this.discoveryRequestsReceivedCounter = this.meter.CreateCounter<long>("p2p.discovery.aspnetcore.requests_received", "requests", "Total ASP.NET Core discovery requests received");
        this.peersFoundCounter = this.meter.CreateCounter<long>("p2p.discovery.aspnetcore.peers_found", "peers", "Total peers successfully found via ASP.NET Core polling");
    }

    /// <inheritdoc />
    public TimeSpan DiscoveryInterval => optionsMonitor.Get(meshId).DiscoveryInterval;

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
                        var basePath = string.IsNullOrWhiteSpace(options.PathPrefix) ? "/ama-enterprise/p2p-discovery" : options.PathPrefix.TrimEnd('/');
                        if (!basePath.StartsWith("/", StringComparison.Ordinal)) basePath = "/" + basePath;
                        var path = $"{basePath}/{meshId}";

                        app.Run(async context =>
                        {
                            if (context.Request.Path == path && context.Request.Method == HttpMethods.Post)
                            {
                                await HandleDiscoveryRequestAsync(context).ConfigureAwait(false);
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
            logger.LogInformation("[{MeshId}] ASP.NET Core Standalone Discovery started explicitly listening natively on {Host}:{Port}", meshId, options.StandaloneListenHost, options.StandaloneListenPort);
        }
        else
        {
            logger.LogInformation("[{MeshId}] ASP.NET Core Integrated Discovery bound implicitly. Relying on host application pipeline invoking MapP2pMeshDiscovery() safely.", meshId);
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
    public async Task<IEnumerable<PeerNode>> DiscoverPeersAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        var options = optionsMonitor.Get(meshId);
        if (string.IsNullOrWhiteSpace(options.DiscoveryUrl))
        {
            logger.LogTrace("[{MeshId}] ASP.NET Core discovery URL is not configured. Skipping active polling.", meshId);
            return Array.Empty<PeerNode>();
        }

        if (!Uri.TryCreate(options.DiscoveryUrl, UriKind.Absolute, out var uri))
        {
            logger.LogTrace("[{MeshId}] ASP.NET Core discovery URL '{Url}' is invalid.", meshId, options.DiscoveryUrl);
            return Array.Empty<PeerNode>();
        }

        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        var localNode = new PeerNode(new PeerId(nodeOptions.LocalPeerId), localEndpoint);

        var client = httpClientFactory.CreateClient($"{meshId}_P2pAspNetCoreDiscovery");
        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(options.DiscoveryTimeout);

        try
        {
            var requestBytes = serializer.SerializeToBytes(localNode);
            using var content = new ByteArrayContent(requestBytes);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

            var response = await client.PostAsync(uri, content, timeoutCts.Token).ConfigureAwait(false);
            discoveryRequestsSentCounter.Add(1, tags);

            if (response.IsSuccessStatusCode)
            {
                var responseBytes = await response.Content.ReadAsByteArrayAsync(timeoutCts.Token).ConfigureAwait(false);
                var remoteNode = serializer.DeserializeFromBytes<PeerNode>(responseBytes);

                if (remoteNode.Id.Value != nodeOptions.LocalPeerId && remoteNode.Id.Value != Guid.Empty)
                {
                    var isAuthenticated = await authenticator.AuthenticateAsync(remoteNode, ReadOnlyMemory<byte>.Empty, timeoutCts.Token).ConfigureAwait(false);
                    if (isAuthenticated)
                    {
                        await failureDetector.RecordHeartbeatAsync(remoteNode.Id, timeoutCts.Token).ConfigureAwait(false);
                        peersFoundCounter.Add(1, tags);
                        return new[] { remoteNode };
                    }
                }
            }
            else
            {
                logger.LogTrace("[{MeshId}] ASP.NET Core discovery request to {Url} returned status {StatusCode}.", meshId, options.DiscoveryUrl, response.StatusCode);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogTrace("[{MeshId}] ASP.NET Core discovery request to {Url} timed out.", meshId, options.DiscoveryUrl);
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "[{MeshId}] ASP.NET Core discovery request to {Url} failed during execution.", meshId, options.DiscoveryUrl);
        }

        return Array.Empty<PeerNode>();
    }

    /// <summary>
    /// Processes inbound HTTP requests extracting generic discovery profiles mapped actively securely.
    /// </summary>
    /// <param name="context">The HTTP context executing decoupled standard inbound bounds.</param>
    public async Task HandleDiscoveryRequestAsync(HttpContext context)
    {
        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        try
        {
            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
            discoveryRequestsReceivedCounter.Add(1, tags);

            using var ms = new MemoryStream();
            await context.Request.Body.CopyToAsync(ms).ConfigureAwait(false);
            var remoteNode = serializer.DeserializeFromBytes<PeerNode>(ms.ToArray());

            if (remoteNode.Id.Value != nodeOptions.LocalPeerId && remoteNode.Id.Value != Guid.Empty)
            {
                var isAuthenticated = await authenticator.AuthenticateAsync(remoteNode, ReadOnlyMemory<byte>.Empty, context.RequestAborted).ConfigureAwait(false);
                if (isAuthenticated)
                {
                    await failureDetector.RecordHeartbeatAsync(remoteNode.Id, context.RequestAborted).ConfigureAwait(false);
                }

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
            logger.LogTrace(ex, "[{MeshId}] Failed to process inbound ASP.NET Core explicitly routed discovery request robustly.", meshId);
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