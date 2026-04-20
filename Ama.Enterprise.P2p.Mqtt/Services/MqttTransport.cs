namespace Ama.Enterprise.P2p.Mqtt.Services;

using System;
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
public sealed class MqttTransport : ITransport
{
    private readonly string meshId;
    private readonly IMqttClientManager clientManager;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<MqttTransport> logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="MqttTransport"/> class.
    /// </summary>
    public MqttTransport(
        string meshId,
        IMqttClientManager clientManager,
        ICrdtSerializer serializer,
        ILogger<MqttTransport> logger)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.clientManager = clientManager ?? throw new ArgumentNullException(nameof(clientManager));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint) => endpoint is MqttPeerEndpoint;

    /// <inheritdoc />
    public Task SendAsync(PeerEndpoint endpoint, IMeshMessage message, CancellationToken cancellationToken)
    {
        if (endpoint is not MqttPeerEndpoint mqttEndpoint)
        {
            logger.LogWarning("[{MeshId}] Cannot send MQTT message. Target endpoint is not an MqttPeerEndpoint.", meshId);
            return Task.CompletedTask;
        }

        var payload = serializer.SerializeToBytes(message);
        return clientManager.PublishAsync(mqttEndpoint.ClientId, payload, cancellationToken);
    }
}