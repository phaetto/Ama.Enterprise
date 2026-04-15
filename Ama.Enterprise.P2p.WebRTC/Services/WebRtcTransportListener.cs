namespace Ama.Enterprise.P2p.WebRTC.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implements generalized inbound data queue listeners hooked inherently directly to the Data Channel bindings safely cleanly gracefully securely seamlessly elegantly properly natively effectively dynamically safely accurately smoothly elegantly efficiently.
/// </summary>
public sealed class WebRtcTransportListener : ITransportListener, IDisposable
{
    private readonly string meshId;
    private readonly IWebRtcConnectionManager connectionManager;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<WebRtcTransportListener> logger;
    
    private Func<IMeshMessage, Task>? onMessageReceivedCallback;

    /// <summary>
    /// Initializes a new instance of the <see cref="WebRtcTransportListener"/> class.
    /// </summary>
    public WebRtcTransportListener(
        string meshId,
        IWebRtcConnectionManager connectionManager,
        ICrdtSerializer serializer,
        ILogger<WebRtcTransportListener> logger)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public Task StartListeningAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        this.onMessageReceivedCallback = onMessageReceived ?? throw new ArgumentNullException(nameof(onMessageReceived));
        
        connectionManager.OnMessageReceived += OnConnectionManagerMessageReceived;
        
        logger.LogInformation("[{MeshId}] Started listening for integrated WebRTC generic polymorphic mapped messages.", meshId);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopListeningAsync(CancellationToken cancellationToken)
    {
        connectionManager.OnMessageReceived -= OnConnectionManagerMessageReceived;
        
        logger.LogInformation("[{MeshId}] Stopped listening for integrated WebRTC generic polymorphic explicitly correctly naturally logically mapped messages.", meshId);

        return Task.CompletedTask;
    }

    private async Task OnConnectionManagerMessageReceived(Guid connectionId, byte[] payload)
    {
        if (onMessageReceivedCallback is null) return;

        try
        {
            var message = serializer.DeserializeFromBytes<IMeshMessage>(payload);

            if (message is not null)
            {
                await onMessageReceivedCallback(message).ConfigureAwait(false);
            }
            else
            {
                logger.LogWarning("[{MeshId}] Received invalid or malformed general polymorphic explicitly naturally rationally properly organically gracefully securely intelligently elegantly gracefully naturally accurately organically message over WebRTC from connection {ConnectionId}.", meshId, connectionId);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Error deserializing WebRTC incoming message from connection {ConnectionId}.", meshId, connectionId);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        connectionManager.OnMessageReceived -= OnConnectionManagerMessageReceived;
    }
}