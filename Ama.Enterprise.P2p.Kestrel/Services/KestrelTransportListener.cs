namespace Ama.Enterprise.P2p.Kestrel.Services;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p;
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
    ICrdtSerializer serializer,
    ILogger<KestrelTransportListener> logger) : ITransportListener, IDisposable
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IOptionsMonitor<KestrelTransportOptions> optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly ICrdtSerializer serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
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

        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders(); // We manage our own logging context to avoid global noise

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

        app.MapPost(path, async (HttpContext context) => await ProcessRequestAsync(context, onMessageReceived, cancellationToken));

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

    private async Task ProcessRequestAsync(HttpContext context, Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        try
        {
            var incomingVersion = context.Request.Headers["X-P2P-Protocol-Version"].ToString();
            if (!string.IsNullOrWhiteSpace(incomingVersion) && !IsMajorVersionCompatible(incomingVersion, Constants.ProtocolVersion))
            {
                throw new NotSupportedException($"Header protocol version {incomingVersion} is not compatible with local version {Constants.ProtocolVersion}.");
            }

            using var memoryStream = new MemoryStream();
            await context.Request.Body.CopyToAsync(memoryStream, cancellationToken).ConfigureAwait(false);
            
            var payload = memoryStream.ToArray();
            var message = serializer.DeserializeFromBytes<IMeshMessage>(payload);

            if (message is not null) 
            {
                if (!IsMajorVersionCompatible(message.ProtocolVersion, Constants.ProtocolVersion))
                {
                    throw new NotSupportedException($"Internal message protocol version {message.ProtocolVersion} is not compatible with local version {Constants.ProtocolVersion}.");
                }

                if (!string.Equals(message.MeshId, meshId, StringComparison.Ordinal))
                {
                    logger.LogWarning("[{MeshId}] Rejected Kestrel message targeting foreign mesh ID {ForeignMeshId}.", meshId, message.MeshId);
                    context.Response.StatusCode = StatusCodes.Status403Forbidden;
                    return;
                }

                try
                {
                    await onMessageReceived(message).ConfigureAwait(false);
                    context.Response.StatusCode = StatusCodes.Status202Accepted;
                }
                catch (NotSupportedException ex)
                {
                    logger.LogWarning(ex, "[{MeshId}] Message rejected by inner payload handler natively: Protocol version not supported.", meshId);
                    context.Response.StatusCode = StatusCodes.Status505HttpVersionNotsupported;
                }
            }
            else
            {
                logger.LogWarning("[{MeshId}] Failed to deserialize incoming Kestrel message. Invalid format.", meshId);
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
            }
        }
        catch (NotSupportedException ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Kestrel message rejected: Protocol version not supported.", meshId);
            context.Response.StatusCode = StatusCodes.Status505HttpVersionNotsupported;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Error processing incoming Kestrel HTTP request natively.", meshId);
            context.Response.StatusCode = StatusCodes.Status500InternalServerError;
        }
    }

    private static bool IsMajorVersionCompatible(string? version1, string? version2)
    {
        if (string.IsNullOrWhiteSpace(version1) || string.IsNullOrWhiteSpace(version2)) 
        {
            return true;
        }

        var v1Major = version1.Split('.')[0];
        var v2Major = version2.Split('.')[0];

        return string.Equals(v1Major, v2Major, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (app is not null)
        {
            _ = app.DisposeAsync().AsTask();
            app = null;
        }
    }
}