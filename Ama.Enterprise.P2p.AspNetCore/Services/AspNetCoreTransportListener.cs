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
public sealed class AspNetCoreTransportListener(
    string meshId,
    IOptionsMonitor<AspNetCoreTransportOptions> optionsMonitor,
    IHttpInboundDispatcher dispatcher,
    ILogger<AspNetCoreTransportListener> logger) : ITransportListener, IDisposable
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IOptionsMonitor<AspNetCoreTransportOptions> optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly IHttpInboundDispatcher dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
    private readonly ILogger<AspNetCoreTransportListener> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    private WebApplication? app;

    /// <inheritdoc />
    public async Task StartListeningAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (onMessageReceived is null)
        {
            throw new ArgumentNullException(nameof(onMessageReceived));
        }

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

            var path = options.PathPrefix ?? string.Empty;
            if (!path.StartsWith("/", StringComparison.Ordinal))
            {
                path = "/" + path;
            }

            app.MapP2pMeshEndpoints(path);

            try
            {
                await app.StartAsync(cancellationToken).ConfigureAwait(false);
                logger.LogInformation("[{MeshId}] ASP.NET Core Standalone listener globally bounding P2P traffic on {Url}{Path}", meshId, listenUrl, path);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Failed to start ASP.NET Core Standalone listener natively. Ensure proper port bindings and permissions.", meshId);
                throw;
            }
        }
        else
        {
            logger.LogInformation("[{MeshId}] ASP.NET Core Integrated listener bridge registered successfully tracking explicit bounds.", meshId);
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
        
        logger.LogInformation("[{MeshId}] ASP.NET Core listener bridge explicitly detached bounds.", meshId);
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