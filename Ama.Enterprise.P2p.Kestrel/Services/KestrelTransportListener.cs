namespace Ama.Enterprise.P2p.Kestrel.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Http.Core.Models;
using Ama.Enterprise.P2p.Http.Core.Services;
using Ama.Enterprise.P2p.Kestrel.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements isolated inbound network listener, strictly binding configurations specific to a single mesh via ASP.NET Core Kestrel efficiently.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="KestrelTransportListener"/> class.
/// </remarks>
public sealed class KestrelTransportListener(
    string meshId,
    IOptionsMonitor<KestrelTransportOptions> optionsMonitor,
    IHttpInboundDispatcher dispatcher,
    ILogger<KestrelTransportListener> logger) : ITransportListener, IDisposable
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IOptionsMonitor<KestrelTransportOptions> optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly IHttpInboundDispatcher dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
    private readonly ILogger<KestrelTransportListener> logger = logger ?? throw new ArgumentNullException(nameof(logger));
    
    private WebApplication? app;

    /// <inheritdoc />
    public async Task StartListeningAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (onMessageReceived is null)
        {
            throw new ArgumentNullException(nameof(onMessageReceived));
        }

        var options = optionsMonitor.Get(meshId);
        if (!options.IsEnabled)
        {
            logger.LogInformation("[{MeshId}] Kestrel Transport Listener is disabled for this mesh.", meshId);
            return;
        }

        dispatcher.RegisterListener(meshId, onMessageReceived);

        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();

        var host = string.IsNullOrWhiteSpace(options.ListenHost) ? "+" : options.ListenHost;
        var listenUrl = $"http://{host}:{options.ListenPort}";
        
        builder.WebHost.UseUrls(listenUrl);
        
        app = builder.Build();

        var path = options.PathPrefix ?? string.Empty;
        if (!path.StartsWith("/", StringComparison.Ordinal))
        {
            path = "/" + path;
        }
        if (path.EndsWith("/", StringComparison.Ordinal) && path.Length > 1)
        {
            path = path.TrimEnd('/');
        }

        app.MapPost(path, async (HttpContext context) => await ProcessRequestAsync(context, cancellationToken));

        try
        {
            await app.StartAsync(cancellationToken).ConfigureAwait(false);
            logger.LogInformation("[{MeshId}] Listening globally for Kestrel P2P traffic on {Url}{Path}", meshId, listenUrl, path);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to start Kestrel listener natively. Ensure proper port bindings and permissions.", meshId);
            throw;
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
                logger.LogWarning(ex, "[{MeshId}] Graceful shutdown of Kestrel transport listener encountered a minor delay or exception.", meshId);
            }
            finally
            {
                app = null;
            }
        }
        
        logger.LogInformation("[{MeshId}] Kestrel transport listener explicitly stopped.", meshId);
    }

    private async Task ProcessRequestAsync(HttpContext context, CancellationToken cancellationToken)
    {
        var incomingVersion = context.Request.Headers["X-P2P-Protocol-Version"].ToString();
        var result = await dispatcher.ProcessPayloadAsync(meshId, incomingVersion, context.Request.Body, cancellationToken).ConfigureAwait(false);

        context.Response.StatusCode = result switch
        {
            HttpPayloadProcessResult.Success => StatusCodes.Status202Accepted,
            HttpPayloadProcessResult.BadRequest => StatusCodes.Status400BadRequest,
            HttpPayloadProcessResult.Forbidden => StatusCodes.Status403Forbidden,
            HttpPayloadProcessResult.UnsupportedVersion => StatusCodes.Status505HttpVersionNotsupported,
            HttpPayloadProcessResult.ServiceUnavailable => StatusCodes.Status503ServiceUnavailable,
            HttpPayloadProcessResult.InternalServerError => StatusCodes.Status500InternalServerError,
            _ => StatusCodes.Status500InternalServerError
        };
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