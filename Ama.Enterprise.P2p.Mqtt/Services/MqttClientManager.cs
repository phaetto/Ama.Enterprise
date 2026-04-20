namespace Ama.Enterprise.P2p.Mqtt.Services;

using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Mqtt.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MQTTnet;

/// <summary>
/// Implementation managing the underlying MQTTnet client, handling continuous connections and specific mesh routing.
/// </summary>
public sealed class MqttClientManager : IMqttClientManager, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<MqttTransportOptions> optionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly ILogger<MqttClientManager> logger;
    private readonly IMqttClient mqttClient;

    /// <inheritdoc />
    public event Func<byte[], Task>? OnMessageReceived;

    /// <summary>
    /// Initializes a new instance of the <see cref="MqttClientManager"/> class.
    /// </summary>
    public MqttClientManager(
        string meshId,
        IOptionsMonitor<MqttTransportOptions> optionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        ILogger<MqttClientManager> logger)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var factory = new MqttClientFactory();
        this.mqttClient = factory.CreateMqttClient();
        
        this.mqttClient.ApplicationMessageReceivedAsync += HandleIncomingMessageAsync;
        this.mqttClient.DisconnectedAsync += HandleDisconnectedAsync;
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var options = optionsMonitor.Get(meshId);
        var nodeOptions = nodeOptionsMonitor.Get(meshId);

        var builder = new MqttClientOptionsBuilder()
            .WithTcpServer(options.Host, options.Port)
            .WithClientId(nodeOptions.LocalPeerId.ToString("N"));

        if (!string.IsNullOrWhiteSpace(options.Username))
        {
            builder.WithCredentials(options.Username, options.Password);
        }

        if (options.UseTls)
        {
            builder.WithTlsOptions(o => o.UseTls());
        }

        try
        {
            await mqttClient.ConnectAsync(builder.Build(), cancellationToken).ConfigureAwait(false);
            logger.LogInformation("[{MeshId}] Connected to MQTT broker at {Host}:{Port}.", meshId, options.Host, options.Port);

            var topic = GetTopicForClient(options.TopicPrefix, nodeOptions.LocalPeerId.ToString("N"));
            var subscribeOptions = new MqttClientFactory().CreateSubscribeOptionsBuilder()
                .WithTopicFilter(f => f.WithTopic(topic))
                .Build();

            await mqttClient.SubscribeAsync(subscribeOptions, cancellationToken).ConfigureAwait(false);
            logger.LogInformation("[{MeshId}] Subscribed to inbound MQTT topic: {Topic}", meshId, topic);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to connect to MQTT broker or subscribe to topics.", meshId);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (mqttClient.IsConnected)
        {
            await mqttClient.DisconnectAsync(new MqttClientDisconnectOptionsBuilder()
                .WithReason(MqttClientDisconnectOptionsReason.NormalDisconnection)
                .Build(), cancellationToken).ConfigureAwait(false);
                
            logger.LogInformation("[{MeshId}] Disconnected from MQTT broker.", meshId);
        }
    }

    /// <inheritdoc />
    public async Task PublishAsync(string targetClientId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(targetClientId))
        {
            throw new ArgumentException("Target client ID cannot be empty.", nameof(targetClientId));
        }

        if (!mqttClient.IsConnected)
        {
            logger.LogWarning("[{MeshId}] Cannot publish message, MQTT client is not connected.", meshId);
            return;
        }

        var options = optionsMonitor.Get(meshId);
        var topic = GetTopicForClient(options.TopicPrefix, targetClientId);

        var message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(payload.ToArray())
            .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();

        await mqttClient.PublishAsync(message, cancellationToken).ConfigureAwait(false);
    }

    private Task HandleIncomingMessageAsync(MqttApplicationMessageReceivedEventArgs args)
    {
        var payload = args.ApplicationMessage.Payload;
        
        if (payload.Length == 0)
        {
            return Task.CompletedTask;
        }

        if (OnMessageReceived != null)
        {
            return OnMessageReceived(payload.ToArray());
        }

        return Task.CompletedTask;
    }

    private async Task HandleDisconnectedAsync(MqttClientDisconnectedEventArgs args)
    {
        logger.LogWarning("[{MeshId}] MQTT connection lost. Reason: {Reason}", meshId, args.Reason);
        
        // Basic automatic reconnection back-off.
        await Task.Delay(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        
        try
        {
            await StartAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to automatically reconnect to MQTT broker.", meshId);
        }
    }

    private static string GetTopicForClient(string prefix, string clientId)
    {
        return $"{prefix.TrimEnd('/')}/{clientId}";
    }

    /// <inheritdoc />
    public void Dispose()
    {
        mqttClient.ApplicationMessageReceivedAsync -= HandleIncomingMessageAsync;
        mqttClient.DisconnectedAsync -= HandleDisconnectedAsync;
        mqttClient.Dispose();
    }
}