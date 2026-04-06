namespace Ama.Enterprise.P2p.Services.Gossip;

using System.Net;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements inbound gossip listener using an <see cref="HttpListener"/>.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="HttpTransportListener"/> class.
/// </remarks>
public sealed class HttpTransportListener(
    IOptions<GossipOptions> options,
    ICrdtSerializer serializer,
    ILogger<HttpTransportListener> logger) : ITransportListener<GossipMessage>, IDisposable
{
    private readonly IOptions<GossipOptions> options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly ICrdtSerializer serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly ILogger<HttpTransportListener> logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private HttpListener? httpListener;
    private CancellationTokenSource? listenerCts;
    private Task? listeningTask;

    /// <inheritdoc />
    public Task StartListeningAsync(Func<GossipMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (onMessageReceived is null)
        {
            throw new ArgumentNullException(nameof(onMessageReceived));
        }

        listenerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        httpListener = new HttpListener();

        var host = string.IsNullOrWhiteSpace(options.Value.ListenHost) ? "+" : options.Value.ListenHost;
        var port = options.Value.ListenPort;
        var prefix = $"http://{host}:{port}/p2p/gossip/";
        
        httpListener.Prefixes.Add(prefix);

        try
        {
            httpListener.Start();
            logger.LogInformation("Listening for gossip messages on {Prefix}", prefix);
        }
        catch (HttpListenerException ex)
        {
            logger.LogError(ex, "Failed to start HTTP listener on {Prefix}. Ensure proper permissions or use a different port.", prefix);
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
            catch (OperationCanceledException)
            {
                // Expected during graceful shutdown
            }
        }
        
        logger.LogInformation("Gossip listener stopped.");
    }

    private async Task ListenLoopAsync(Func<GossipMessage, Task> onMessageReceived, CancellationToken cancellationToken)
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
                break; // Listener was stopped
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error accepting incoming HTTP request.");
            }
        }
    }

    private async Task ProcessRequestAsync(HttpListenerContext context, Func<GossipMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        try
        {
            if (context.Request.HttpMethod != "POST")
            {
                context.Response.StatusCode = (int)HttpStatusCode.MethodNotAllowed;
                return;
            }

            // Verify protocol version for backwards compatibility.
            var versionHeader = context.Request.Headers["X-P2P-Protocol-Version"];
            var incomingVersion = new Version(0, 0, 0); // Default to 0.0.0 for older clients without header

            if (!string.IsNullOrWhiteSpace(versionHeader) && Version.TryParse(versionHeader, out var parsedVersion))
            {
                incomingVersion = parsedVersion;
            }

            var localVersion = Version.Parse(Constants.ProtocolVersion);
            
            // Rejects mismatched major versions since they indicate breaking protocol changes
            if (incomingVersion.Major != localVersion.Major)
            {
                logger.LogWarning(
                    "Rejected incoming gossip message due to major protocol version mismatch. Local: {LocalVersion}, Incoming: {IncomingVersion}", 
                    localVersion, 
                    incomingVersion);
                context.Response.StatusCode = (int)HttpStatusCode.HttpVersionNotSupported;
                return;
            }

            using var memoryStream = new MemoryStream();
            await context.Request.InputStream.CopyToAsync(memoryStream, cancellationToken).ConfigureAwait(false);
            
            var payload = memoryStream.ToArray();
            var message = serializer.DeserializeFromBytes<GossipMessage>(payload);

            // In generic struct serialization, ensure we check default values. 
            // Since GossipMessage has a required valid Guid, checking it avoids processing bad payloads.
            if (message.MessageId != Guid.Empty) 
            {
                await onMessageReceived(message).ConfigureAwait(false);
                context.Response.StatusCode = (int)HttpStatusCode.Accepted;
            }
            else
            {
                logger.LogWarning("Failed to deserialize incoming gossip message. Invalid format.");
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing incoming HTTP request.");
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