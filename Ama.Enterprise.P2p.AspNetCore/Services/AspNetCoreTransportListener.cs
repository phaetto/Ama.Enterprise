namespace Ama.Enterprise.P2p.AspNetCore.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
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

    private WebApplication? app;

    /// <summary>
    /// Initializes a new instance of the <see cref="AspNetCoreTransportListener"/> class.
    /// </summary>
    public AspNetCoreTransportListener(
        string meshId,
        IOptionsMonitor<AspNetCoreTransportOptions> optionsMonitor,
        IHttpInboundDispatcher dispatcher,
        ILogger<AspNetCoreTransportListener> logger)
    {
        ArgumentNullException.ThrowIfNull(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(dispatcher);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.dispatcher = dispatcher;
        this.logger = logger;
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

            var host = string.IsNullOrWhiteSpace(options.StandaloneListenHost) ? "+" : options.StandaloneListenHost;
            var listenUrl = $"http://{host}:{options.StandaloneListenPort}";
            
            builder.WebHost.UseUrls(listenUrl);
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