namespace Ama.Enterprise.P2p.WebRTC.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implements generalized inbound data queue listeners hooked inherently directly to the Data Channel bindings.
/// </summary>
public sealed class WebRtcTransportListener : ITransportListener, IDisposable
{
    private readonly string meshId;
    private readonly IWebRtcConnectionManager connectionManager;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<WebRtcTransportListener> logger;
    
    private readonly Meter meter;
    private readonly Counter<long> messagesReceivedCounter;
    private readonly Histogram<long> payloadBytesHistogram;
    
    private Func<IMeshMessage, Task>? onMessageReceivedCallback;

    public WebRtcTransportListener(
        string meshId,
        IWebRtcConnectionManager connectionManager,
        ICrdtSerializer serializer,
        ILogger<WebRtcTransportListener> logger,
        IMeterFactory? meterFactory = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(connectionManager);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.connectionManager = connectionManager;
        this.serializer = serializer;
        this.logger = logger;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.WebRtcTransportListener") ?? new Meter("Ama.Enterprise.P2p.WebRtcTransportListener");
        this.messagesReceivedCounter = this.meter.CreateCounter<long>(
            "p2p.transport.webrtc.messages_received", 
            "messages", 
            "Total messages received via WebRTC transport");
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>(
            "p2p.transport.webrtc.inbound_payload_bytes", 
            "bytes", 
            "Size of inbound WebRTC payload in bytes");
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
        
        logger.LogInformation("[{MeshId}] Stopped listening for integrated WebRTC mapped messages.", meshId);

        return Task.CompletedTask;
    }

    private async Task OnConnectionManagerMessageReceived(Guid connectionId, byte[] payload)
    {
        if (onMessageReceivedCallback is null) return;

        try
        {
            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
            messagesReceivedCounter.Add(1, tags);
            payloadBytesHistogram.Record(payload.Length, tags);

            var message = serializer.DeserializeFromBytes<IMeshMessage>(payload);

            if (message is not null)
            {
                await onMessageReceivedCallback(message).ConfigureAwait(false);
            }
            else
            {
                logger.LogWarning("[{MeshId}] Received invalid or malformed mapped message over WebRTC from connection {ConnectionId}.", meshId, connectionId);
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
        meter.Dispose();
    }
}