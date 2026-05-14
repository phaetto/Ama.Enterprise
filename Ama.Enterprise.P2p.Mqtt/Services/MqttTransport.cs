namespace Ama.Enterprise.P2p.Mqtt.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Mqtt.Models;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Outbound transport mechanism routing P2P messages over isolated MQTT channels.
/// </summary>
public sealed class MqttTransport : ITransport, IDisposable
{
    private readonly string meshId;
    private readonly IMqttClientManager clientManager;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<MqttTransport> logger;

    private readonly Meter meter;
    private readonly Counter<long> messagesSentCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    private bool isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MqttTransport"/> class.
    /// </summary>
    public MqttTransport(
        string meshId,
        IMqttClientManager clientManager,
        ICrdtSerializer serializer,
        ILogger<MqttTransport> logger,
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

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.MqttTransport") ?? new Meter("Ama.Enterprise.P2p.MqttTransport");
        this.messagesSentCounter = this.meter.CreateCounter<long>(
            "p2p.transport.mqtt.messages_sent", 
            "messages", 
            "Total messages sent via MQTT transport");
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>(
            "p2p.transport.mqtt.outbound_payload_bytes", 
            "bytes", 
            "Size of outbound MQTT payload in bytes");
    }

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        ArgumentNullException.ThrowIfNull(endpoint);
        return endpoint is MqttPeerEndpoint;
    }

    /// <inheritdoc />
    public Task SendAsync(PeerEndpoint endpoint, IMeshMessage message, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(message);

        if (endpoint is not MqttPeerEndpoint mqttEndpoint)
        {
            logger.LogWarning("[{MeshId}] Cannot send MQTT message. Target endpoint is not an MqttPeerEndpoint.", meshId);
            return Task.CompletedTask;
        }

        var payload = serializer.SerializeToBytes(message);

        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
        messagesSentCounter.Add(1, tags);
        payloadBytesHistogram.Record(payload.Length, tags);

        return clientManager.PublishAsync(mqttEndpoint.ClientId, payload, cancellationToken);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (isDisposed) return;
        meter.Dispose();
        isDisposed = true;
    }
}