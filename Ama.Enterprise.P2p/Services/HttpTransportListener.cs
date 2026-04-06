using System.Net;
using Ama.Enterprise.P2p.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// Implements inbound gossip listener using an <see cref="HttpListener"/>.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="HttpTransportListener"/> class.
/// </remarks>
public sealed class HttpTransportListener(
    IOptions<GossipOptions> options,
    IGossipSerializer serializer,
    ILogger<HttpTransportListener> logger) : ITransportListener, IDisposable
{
    private readonly IOptions<GossipOptions> options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly IGossipSerializer serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
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

        this.listenerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        this.httpListener = new HttpListener();

        var host = string.IsNullOrWhiteSpace(this.options.Value.ListenHost) ? "+" : this.options.Value.ListenHost;
        var port = this.options.Value.ListenPort;
        var prefix = $"http://{host}:{port}/p2p/gossip/";
        
        this.httpListener.Prefixes.Add(prefix);

        try
        {
            this.httpListener.Start();
            this.logger.LogInformation("Listening for gossip messages on {Prefix}", prefix);
        }
        catch (HttpListenerException ex)
        {
            this.logger.LogError(ex, "Failed to start HTTP listener on {Prefix}. Ensure proper permissions or use a different port.", prefix);
            throw;
        }

        this.listeningTask = Task.Run(() => this.ListenLoopAsync(onMessageReceived, this.listenerCts.Token), this.listenerCts.Token);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopListeningAsync(CancellationToken cancellationToken)
    {
        if (this.listenerCts is not null)
        {
            await this.listenerCts.CancelAsync().ConfigureAwait(false);
        }

        this.httpListener?.Stop();

        if (this.listeningTask is not null)
        {
            try
            {
                await this.listeningTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during graceful shutdown
            }
        }
        
        this.logger.LogInformation("Gossip listener stopped.");
    }

    private async Task ListenLoopAsync(Func<GossipMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (this.httpListener is null) return;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var context = await this.httpListener.GetContextAsync().ConfigureAwait(false);
                _ = Task.Run(() => this.ProcessRequestAsync(context, onMessageReceived, cancellationToken), cancellationToken);
            }
            catch (HttpListenerException) when (cancellationToken.IsCancellationRequested || !this.httpListener.IsListening)
            {
                break; // Listener was stopped
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Error accepting incoming HTTP request.");
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

            using var memoryStream = new MemoryStream();
            await context.Request.InputStream.CopyToAsync(memoryStream, cancellationToken).ConfigureAwait(false);
            
            var payload = memoryStream.ToArray();
            var message = this.serializer.Deserialize(payload);

            if (message.HasValue)
            {
                await onMessageReceived(message.Value).ConfigureAwait(false);
                context.Response.StatusCode = (int)HttpStatusCode.Accepted;
            }
            else
            {
                this.logger.LogWarning("Failed to deserialize incoming gossip message.");
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            }
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Error processing incoming HTTP request.");
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
        if (this.listenerCts is not null)
        {
            this.listenerCts.Cancel();
            this.listenerCts.Dispose();
        }

        if (this.httpListener is not null)
        {
            if (this.httpListener.IsListening)
            {
                this.httpListener.Stop();
            }
            this.httpListener.Close();
        }
    }
}