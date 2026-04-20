namespace Ama.Enterprise.P2p.Mqtt.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Inbound listener that handles receiving data from the subscribed MQTT topic.
/// </summary>
public sealed class MqttTransportListener : ITransportListener, IDisposable
{
    private readonly string meshId;
    private readonly IMqttClientManager clientManager;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<MqttTransportListener> logger;
    
    private Func<IMeshMessage, Task>? onMessageReceivedCallback;

    /// <summary>
    /// Initializes a new instance of the <see cref="MqttTransportListener"/> class.
    /// </summary>
    public MqttTransportListener(
        string meshId,
        IMqttClientManager clientManager,
        ICrdtSerializer serializer,
        ILogger<MqttTransportListener> logger)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.clientManager = clientManager ?? throw new ArgumentNullException(nameof(clientManager));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task StartListeningAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        this.onMessageReceivedCallback = onMessageReceived ?? throw new ArgumentNullException(nameof(onMessageReceived));
        
        clientManager.OnMessageReceived += OnClientManagerMessageReceived;
        
        await clientManager.StartAsync(cancellationToken).ConfigureAwait(false);
        
        logger.LogInformation("[{MeshId}] Started listening for inbound MQTT messages.", meshId);
    }

    /// <inheritdoc />
    public async Task StopListeningAsync(CancellationToken cancellationToken)
    {
        clientManager.OnMessageReceived -= OnClientManagerMessageReceived;
        
        await clientManager.StopAsync(cancellationToken).ConfigureAwait(false);
        
        logger.LogInformation("[{MeshId}] Stopped listening for inbound MQTT messages.", meshId);
    }

    private async Task OnClientManagerMessageReceived(byte[] payload)
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
                logger.LogWarning("[{MeshId}] Received invalid or malformed message over MQTT.", meshId);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Error deserializing inbound MQTT message.", meshId);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        clientManager.OnMessageReceived -= OnClientManagerMessageReceived;
    }
}