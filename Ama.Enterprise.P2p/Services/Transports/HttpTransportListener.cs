namespace Ama.Enterprise.P2p.Services.Transports;

using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements generalized inbound listener using an <see cref="HttpListener"/> scoped to a given mesh identifier.
/// </summary>
/// <typeparam name="TMessage">The generic type of message payloads traversing the transport layer.</typeparam>
public sealed class HttpTransportListener<TMessage>(
    string meshId,
    IOptionsMonitor<HttpTransportOptions> optionsMonitor,
    ICrdtSerializer serializer,
    ILogger<HttpTransportListener<TMessage>> logger) : ITransportListener<TMessage>, IDisposable
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IOptionsMonitor<HttpTransportOptions> optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly ICrdtSerializer serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly ILogger<HttpTransportListener<TMessage>> logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private HttpListener? httpListener;
    private CancellationTokenSource? listenerCts;
    private Task? listeningTask;

    /// <inheritdoc />
    public Task StartListeningAsync(Func<TMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (onMessageReceived is null)
        {
            throw new ArgumentNullException(nameof(onMessageReceived));
        }

        listenerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        httpListener = new HttpListener();

        var options = optionsMonitor.Get(meshId);
        var host = string.IsNullOrWhiteSpace(options.ListenHost) ? "+" : options.ListenHost;
        var port = options.ListenPort;
        var path = options.PathPrefix.EndsWith("/") ? options.PathPrefix : options.PathPrefix + "/";
        var prefix = $"http://{host}:{port}{path}";
        
        httpListener.Prefixes.Add(prefix);

        try
        {
            httpListener.Start();
            logger.LogInformation("[{MeshId}] Listening for generic HTTP messages on {Prefix}", meshId, prefix);
        }
        catch (HttpListenerException ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to start HTTP listener on {Prefix}. Ensure proper permissions.", meshId, prefix);
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

        httpListener?.Stop();

        if (listeningTask is not null)
        {
            try
            {
                await listeningTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }
        
        logger.LogInformation("[{MeshId}] Transport listener stopped.", meshId);
    }

    private async Task ListenLoopAsync(Func<TMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (httpListener is null) return;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var context = await httpListener.GetContextAsync().ConfigureAwait(false);
                _ = Task.Run(() => ProcessRequestAsync(context, onMessageReceived, cancellationToken), cancellationToken);
            }
            catch (HttpListenerException) when (cancellationToken.IsCancellationRequested || !httpListener.IsListening)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Error accepting incoming HTTP request.", meshId);
            }
        }
    }

    private async Task ProcessRequestAsync(HttpListenerContext context, Func<TMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        try
        {
            if (context.Request.HttpMethod != "POST")
            {
                context.Response.StatusCode = (int)HttpStatusCode.MethodNotAllowed;
                return;
            }

            var versionHeader = context.Request.Headers["X-P2P-Protocol-Version"];
            var incomingVersion = new Version(0, 0, 0); 

            if (!string.IsNullOrWhiteSpace(versionHeader) && Version.TryParse(versionHeader, out var parsedVersion))
            {
                incomingVersion = parsedVersion;
            }

            var localVersion = Version.Parse(Constants.ProtocolVersion);
            
            if (incomingVersion.Major != localVersion.Major)
            {
                logger.LogWarning(
                    "[{MeshId}] Rejected incoming message due to protocol major version mismatch. Local: {LocalVersion}, Incoming: {IncomingVersion}", 
                    meshId, localVersion, incomingVersion);
                context.Response.StatusCode = (int)HttpStatusCode.HttpVersionNotSupported;
                return;
            }

            using var memoryStream = new MemoryStream();
            await context.Request.InputStream.CopyToAsync(memoryStream, cancellationToken).ConfigureAwait(false);
            
            var payload = memoryStream.ToArray();
            var message = serializer.DeserializeFromBytes<TMessage>(payload);

            if (message is not null) 
            {
                await onMessageReceived(message).ConfigureAwait(false);
                context.Response.StatusCode = (int)HttpStatusCode.Accepted;
            }
            else
            {
                logger.LogWarning("[{MeshId}] Failed to deserialize incoming generic message. Invalid format.", meshId);
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Error processing incoming HTTP request.", meshId);
            context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
        }
        finally
        {
            context.Response.Close();
        }
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
            if (httpListener.IsListening)
            {
                httpListener.Stop();
            }
            httpListener.Close();
        }
    }
}