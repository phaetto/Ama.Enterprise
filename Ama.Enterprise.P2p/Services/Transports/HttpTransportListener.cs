namespace Ama.Enterprise.P2p.Services.Transports;

using System;
using System.Collections.Generic;
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
/// Implements globally shared inbound multiplexing listeners explicitly wrapping HTTP natively capturing multi-tenant meshes accurately.
/// </summary>
/// <typeparam name="TMessage">The generic type of multiplexed messages natively traversing the transport effectively.</typeparam>
public sealed class HttpTransportListener<TMessage> : ITransportListener<TMessage>, IDisposable where TMessage : IMeshMessage
{
    private readonly IEnumerable<P2pMeshMetadata> meshes;
    private readonly IOptionsMonitor<HttpTransportOptions> optionsMonitor;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<HttpTransportListener<TMessage>> logger;
    private HttpListener? httpListener;
    private CancellationTokenSource? listenerCts;
    private Task? listeningTask;

    /// <summary>
    /// Initializes a new instance of the <see cref="HttpTransportListener{TMessage}"/> class.
    /// </summary>
    public HttpTransportListener(
        IEnumerable<P2pMeshMetadata> meshes,
        IOptionsMonitor<HttpTransportOptions> optionsMonitor,
        ICrdtSerializer serializer,
        ILogger<HttpTransportListener<TMessage>> logger)
    {
        this.meshes = meshes ?? throw new ArgumentNullException(nameof(meshes));
        this.optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public Task StartListeningAsync(Func<TMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (onMessageReceived is null)
        {
            throw new ArgumentNullException(nameof(onMessageReceived));
        }

        listenerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        httpListener = new HttpListener();

        var prefixes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Map and group unique listener prefixes by all configured meshes.
        foreach (var mesh in meshes)
        {
            var options = optionsMonitor.Get(mesh.MeshId);
            var host = string.IsNullOrWhiteSpace(options.ListenHost) ? "+" : options.ListenHost;
            var path = options.PathPrefix.EndsWith("/") ? options.PathPrefix : options.PathPrefix + "/";
            var prefix = $"http://{host}:{options.ListenPort}{path}";
            prefixes.Add(prefix);
        }

        try
        {
            foreach (var prefix in prefixes)
            {
                httpListener.Prefixes.Add(prefix);
                logger.LogInformation("Listening globally for multiplexed HTTP P2P traffic on {Prefix}", prefix);
            }

            httpListener.Start();
        }
        catch (HttpListenerException ex)
        {
            logger.LogError(ex, "Failed to start multiplexed HTTP listener. Ensure proper port bindings and permissions.");
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
        
        logger.LogInformation("Shared HTTP transport multiplexer listener stopped.");
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

                logger.LogError(ex, "HTTP listener error while accepting incoming multiplexed request.");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error accepting incoming globally multiplexed HTTP request.");
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
                logger.LogWarning("Rejected incoming generic message due to protocol major version mismatch. Local: {LocalVersion}, Incoming: {IncomingVersion}", localVersion, incomingVersion);
                context.Response.StatusCode = (int)HttpStatusCode.HttpVersionNotSupported;
                return;
            }

            using var memoryStream = new MemoryStream();
            await context.Request.InputStream.CopyToAsync(memoryStream, cancellationToken).ConfigureAwait(false);
            
            var payload = memoryStream.ToArray();
            var message = serializer.DeserializeFromBytes<TMessage>(payload);

            if (message is not null) 
            {
                var targetMeshOptions = optionsMonitor.Get(message.MeshId);

                // Enforce port isolation: ensure the target mesh actually exposes the port the message arrived on natively.
                if (targetMeshOptions != null && context.Request.LocalEndPoint.Port != targetMeshOptions.ListenPort)
                {
                    logger.LogWarning("Isolation rejected message for mesh {MeshId}: attempted to cross-connect via non-allowed port {Port}.", message.MeshId, context.Request.LocalEndPoint.Port);
                    context.Response.StatusCode = (int)HttpStatusCode.Forbidden;
                    return;
                }

                await onMessageReceived(message).ConfigureAwait(false);
                context.Response.StatusCode = (int)HttpStatusCode.Accepted;
            }
            else
            {
                logger.LogWarning("Failed to deserialize generic multiplexed incoming message. Invalid format.");
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing incoming unified HTTP request.");
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
            try
            {
                if (httpListener.IsListening)
                {
                    httpListener.Stop();
                }
            }
            catch (ObjectDisposedException) { }
            
            httpListener.Close();
        }
    }
}