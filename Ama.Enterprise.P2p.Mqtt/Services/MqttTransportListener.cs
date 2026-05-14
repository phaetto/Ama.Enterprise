namespace Ama.Enterprise.P2p.Mqtt.Services;

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
/// Inbound listener that handles receiving data from the subscribed MQTT topic natively decoupling distinct mesh bounds.
/// </summary>
public sealed class MqttTransportListener : ITransportListener, IDisposable
{
    private readonly string meshId;
    private readonly IMqttClientManager clientManager;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<MqttTransportListener> logger;
    
    private readonly Meter meter;
    private readonly Counter<long> messagesReceivedCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    private Func<IMeshMessage, Task>? onMessageReceivedCallback;
    private bool isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MqttTransportListener"/> class.
    /// </summary>
    public MqttTransportListener(
        string meshId,
        IMqttClientManager clientManager,
        ICrdtSerializer serializer,
        ILogger<MqttTransportListener> logger,
        IMeterFactory? meterFactory = null)
    {
        ArgumentNullException.ThrowIfNull(meshId);
        ArgumentNullException.ThrowIfNull(clientManager);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.clientManager = clientManager;
        this.serializer = serializer;
        this.logger = logger;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.MqttTransportListener") ?? new Meter("Ama.Enterprise.P2p.MqttTransportListener");
        this.messagesReceivedCounter = this.meter.CreateCounter<long>(
            "p2p.transport.mqtt.messages_received", 
            "messages", 
            "Total messages received via MQTT transport");
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>(
            "p2p.transport.mqtt.inbound_payload_bytes", 
            "bytes", 
            "Size of inbound MQTT payload in bytes");
    }

    /// <inheritdoc />
    public async Task StartListeningAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        ArgumentNullException.ThrowIfNull(onMessageReceived);

        this.onMessageReceivedCallback = onMessageReceived;
        
        clientManager.OnMessageReceived += OnClientManagerMessageReceived;
        
        await clientManager.StartAsync(cancellationToken).ConfigureAwait(false);
        
        logger.LogInformation("[{MeshId}] Started listening for inbound generic mapped MQTT messages.", meshId);
    }

    /// <inheritdoc />
    public async Task StopListeningAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        clientManager.OnMessageReceived -= OnClientManagerMessageReceived;
        
        await clientManager.StopAsync(cancellationToken).ConfigureAwait(false);
        
        logger.LogInformation("[{MeshId}] Stopped listening for inbound MQTT messages.", meshId);
    }

    private async Task OnClientManagerMessageReceived(byte[] payload)
    {
        if (onMessageReceivedCallback is null) return;

        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
        messagesReceivedCounter.Add(1, tags);
        payloadBytesHistogram.Record(payload.Length, tags);

        try
        {
            var message = serializer.DeserializeFromBytes<IMeshMessage>(payload);

            if (message is not null)
            {
                await onMessageReceivedCallback(message).ConfigureAwait(false);
            }
            else
            {
                logger.LogWarning("[{MeshId}] Received invalid or malformed generic payload natively over MQTT.", meshId);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Error deserializing inbound MQTT generic constraints.", meshId);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (isDisposed) return;
        clientManager.OnMessageReceived -= OnClientManagerMessageReceived;
        meter.Dispose();
        isDisposed = true;
    }
}