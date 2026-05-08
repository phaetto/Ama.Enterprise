namespace Ama.Enterprise.P2p.Services.Transports;

using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements isolated inbound network listener, strictly binding configurations specific to a single mesh.
/// WARNING: For production environments, it is strongly recommended to use the ASP.NET Core Kestrel implementation instead.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="HttpTransportListener"/> class.
/// </remarks>
#if !DEBUG
[Experimental("AMA_P2P_HTTP_001", Message = "For production environments, it is strongly recommended to use the ASP.NET Core Kestrel implementation instead")]
#endif
public sealed class HttpTransportListener(
    string meshId,
    IOptionsMonitor<HttpTransportOptions> optionsMonitor,
    ICrdtSerializer serializer,
    ILogger<HttpTransportListener> logger) : ITransportListener, IDisposable
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IOptionsMonitor<HttpTransportOptions> optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly ICrdtSerializer serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly ILogger<HttpTransportListener> logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private HttpListener? httpListener;
    private CancellationTokenSource? listenerCts;
    private Task? listeningTask;

    /// <inheritdoc />
    public Task StartListeningAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (onMessageReceived is null)
        {
            throw new ArgumentNullException(nameof(onMessageReceived));
        }

        var options = optionsMonitor.Get(meshId);
        if (!options.IsEnabled)
        {
            logger.LogInformation("[{MeshId}] HTTP Transport Listener is disabled for this mesh.", meshId);
            return Task.CompletedTask;
        }

        listenerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        httpListener = new HttpListener();

        var host = string.IsNullOrWhiteSpace(options.ListenHost) ? "+" : options.ListenHost;
        var path = options.PathPrefix?.EndsWith("/") == true ? options.PathPrefix : (options.PathPrefix + "/");
        var prefix = $"http://{host}:{options.ListenPort}{path}";

        try
        {
            httpListener.Prefixes.Add(prefix);
            logger.LogInformation("[{MeshId}] Listening globally for HTTP P2P traffic on {Prefix}", meshId, prefix);

            httpListener.Start();
        }
        catch (HttpListenerException ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to start HTTP listener. Ensure proper port bindings and permissions.", meshId);
            throw;
        }

        listeningTask = Task.Run(() => ListenLoopAsync(onMessageReceived, listenerCts.Token), listenerCts.Token);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopListeningAsync(CancellationToken cancellationToken)
    {
        if (listenerCts is not null)
        {
            await listenerCts.CancelAsync().ConfigureAwait(false);
        }

        if (httpListener is not null)
        {
            try
            {
                httpListener.Stop();
            }
            catch (ObjectDisposedException) { }
        }

        if (listeningTask is not null)
        {
            try
            {
                await listeningTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }
        
        logger.LogInformation("[{MeshId}] HTTP transport listener stopped.", meshId);
    }

    private async Task ListenLoopAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (httpListener is null) return;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var context = await httpListener.GetContextAsync().ConfigureAwait(false);
                _ = Task.Run(() => ProcessRequestAsync(context, onMessageReceived, cancellationToken), cancellationToken);
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (HttpListenerException ex)
            {
                if (cancellationToken.IsCancellationRequested) break;

                bool isListening = false;
                try
                {
                    isListening = httpListener.IsListening;
                }
                catch (ObjectDisposedException) { break; }

                if (!isListening) break;

                logger.LogError(ex, "[{MeshId}] HTTP listener error while accepting incoming request.", meshId);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Error accepting incoming HTTP request.", meshId);
            }
        }
    }

    private async Task ProcessRequestAsync(HttpListenerContext context, Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        try
        {
            if (context.Request.HttpMethod != "POST")
            {
                context.Response.StatusCode = (int)HttpStatusCode.MethodNotAllowed;
                return;
            }

            var incomingVersion = context.Request.Headers["X-P2P-Protocol-Version"];
            if (!string.IsNullOrWhiteSpace(incomingVersion) && !IsMajorVersionCompatible(incomingVersion, Constants.ProtocolVersion))
            {
                throw new NotSupportedException($"Header protocol version {incomingVersion} is not compatible with local version {Constants.ProtocolVersion}.");
            }

            using var memoryStream = new MemoryStream();
            await context.Request.InputStream.CopyToAsync(memoryStream, cancellationToken).ConfigureAwait(false);
            
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
                    logger.LogWarning("[{MeshId}] Rejected message targeting foreign mesh ID {ForeignMeshId}.", meshId, message.MeshId);
                    context.Response.StatusCode = (int)HttpStatusCode.Forbidden;
                    return;
                }

                try
                {
                    await onMessageReceived(message).ConfigureAwait(false);
                    context.Response.StatusCode = (int)HttpStatusCode.Accepted;
                }
                catch (NotSupportedException ex)
                {
                    logger.LogWarning(ex, "[{MeshId}] Message rejected by inner payload handler: Protocol version not supported.", meshId);
                    context.Response.StatusCode = (int)HttpStatusCode.HttpVersionNotSupported;
                }
            }
            else
            {
                logger.LogWarning("[{MeshId}] Failed to deserialize incoming message. Invalid format.", meshId);
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            }
        }
        catch (NotSupportedException ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Message rejected: Protocol version not supported.", meshId);
            context.Response.StatusCode = (int)HttpStatusCode.HttpVersionNotSupported;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Error processing incoming HTTP request.", meshId);
            context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
        }
        finally
        {
            try
            {
                context.Response.Close();
            }
            catch (Exception)
            {
                // Ignored - ensure context closed cleanly.
            }
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
        if (listenerCts is not null)
        {
            listenerCts.Cancel();
            listenerCts.Dispose();
        }

        if (httpListener is not null)
        {
            try
            {
                if (httpListener.IsListening)
                {
                    httpListener.Stop();
                }
            }
            catch (ObjectDisposedException) { }
            catch (PlatformNotSupportedException) { }
            
            httpListener.Close();
        }
    }
}