namespace Ama.Enterprise.P2p.AspNetCore.Services;

using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.Licensing.Services;
using Ama.Enterprise.P2p.AspNetCore.Extensions;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Bridge implementation connecting decentralized internal generic processing logic exposing mesh subscriptions directly evaluating hosting modes reliably natively.
/// </summary>
public sealed class AspNetCoreTransportListener : ITransportListener, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<AspNetCoreTransportOptions> optionsMonitor;
    private readonly IHttpInboundDispatcher dispatcher;
    private readonly ILogger<AspNetCoreTransportListener> logger;
    private readonly ICertificateLoader? certificateLoader;

    private WebApplication? app;

    /// <summary>
    /// Initializes a new instance of the <see cref="AspNetCoreTransportListener"/> class.
    /// </summary>
    public AspNetCoreTransportListener(
        string meshId,
        IOptionsMonitor<AspNetCoreTransportOptions> optionsMonitor,
        IHttpInboundDispatcher dispatcher,
        ILogger<AspNetCoreTransportListener> logger,
        ICertificateLoader? certificateLoader = null)
    {
        ArgumentNullException.ThrowIfNull(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(dispatcher);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.dispatcher = dispatcher;
        this.logger = logger;
        this.certificateLoader = certificateLoader;
    }

    /// <inheritdoc />
    public async Task StartListeningAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(onMessageReceived);

        var options = optionsMonitor.Get(meshId);
        dispatcher.RegisterListener(meshId, onMessageReceived);

        if (options.HostingMode == AspNetCoreHostingMode.Standalone)
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();

            X509Certificate2? cert = null;
            if (options.UseHttpsStandalone)
            {
                cert = LoadCertificate(options.CertificateFilePath, options.CertificatePassword, options.CertificateThumbprint);
            }

            var host = string.IsNullOrWhiteSpace(options.StandaloneListenHost) ? "+" : options.StandaloneListenHost;
            var scheme = options.UseHttpsStandalone && cert != null ? "https" : "http";
            var listenUrl = $"{scheme}://{host}:{options.StandaloneListenPort}";

            builder.WebHost.ConfigureKestrel(serverOptions =>
            {
                Action<Microsoft.AspNetCore.Server.Kestrel.Core.ListenOptions> configureListen = listenOptions =>
                {
                    if (options.UseHttpsStandalone && cert != null) listenOptions.UseHttps(cert);
                };

                if (host == "+" || host == "0.0.0.0")
                {
                    serverOptions.ListenAnyIP(options.StandaloneListenPort, configureListen);
                }
                else if (host.Equals("localhost", StringComparison.OrdinalIgnoreCase))
                {
                    serverOptions.ListenLocalhost(options.StandaloneListenPort, configureListen);
                }
                else if (System.Net.IPAddress.TryParse(host, out var ipAddress))
                {
                    serverOptions.Listen(ipAddress, options.StandaloneListenPort, configureListen);
                }
                else
                {
                    logger.LogWarning("[{MeshId}] Invalid Kestrel ListenHost '{Host}', falling back to Any IP.", meshId, host);
                    serverOptions.ListenAnyIP(options.StandaloneListenPort, configureListen);
                }
            });

            builder.Services.AddSingleton(dispatcher);

            app = builder.Build();

            var basePath = string.IsNullOrWhiteSpace(options.PathPrefix) ? "/ama-enterprise/p2p-mesh" : options.PathPrefix.TrimEnd('/');
            if (!basePath.StartsWith("/", StringComparison.Ordinal)) basePath = "/" + basePath;

            app.MapP2pMeshEndpoints(basePath);

            try
            {
                await app.StartAsync(cancellationToken).ConfigureAwait(false);
                logger.LogInformation("[{MeshId}] ASP.NET Core Standalone listener globally bounding P2P traffic on explicitly mapped route {Url}{Path}", meshId, listenUrl, basePath);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Failed to start ASP.NET Core Standalone listener natively. Ensure proper port bindings and permissions.", meshId);
                throw;
            }
        }
        else
        {
            logger.LogInformation("[{MeshId}] ASP.NET Core Integrated listener bridge registered successfully tracking explicitly configured endpoints natively.", meshId);
        }
    }

    /// <inheritdoc />
    public async Task StopListeningAsync(CancellationToken cancellationToken)
    {
        dispatcher.UnregisterListener(meshId);

        if (app is not null)
        {
            try
            {
                await app.StopAsync(cancellationToken).ConfigureAwait(false);
                await app.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "[{MeshId}] Graceful shutdown of ASP.NET Core Standalone listener encountered a minor delay or exception.", meshId);
            }
            finally
            {
                app = null;
            }
        }
        
        logger.LogInformation("[{MeshId}] ASP.NET Core listener bridge explicitly detached standard bounds.", meshId);
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
        dispatcher.UnregisterListener(meshId);
        if (app is not null)
        {
            _ = app.DisposeAsync().AsTask();
            app = null;
        }
    }
}