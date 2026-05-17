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
/// Implementation managing the underlying MQTTnet client, handling continuous connections explicitly isolating topic subscriptions across generic meshes natively.
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
        ArgumentNullException.ThrowIfNull(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(nodeOptionsMonitor);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.nodeOptionsMonitor = nodeOptionsMonitor;
        this.logger = logger;

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

        // Incorporate application prefix and meshId natively mapping distinct client instances safely avoiding broker kicks.
        var builder = new MqttClientOptionsBuilder()
            .WithTcpServer(options.Host, options.Port)
            .WithClientId($"ama-ent-{nodeOptions.LocalPeerId:N}-{meshId}");

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

            // Dynamically decouple internal generic routing injecting application prefix and meshId explicit bounds strictly
            var topic = GetTopicForClient(options.TopicPrefix, meshId, nodeOptions.LocalPeerId.ToString("N"));
            
            var subscribeOptions = new MqttClientFactory().CreateSubscribeOptionsBuilder()
                .WithTopicFilter(f => f.WithTopic(topic))
                .Build();

            await mqttClient.SubscribeAsync(subscribeOptions, cancellationToken).ConfigureAwait(false);
            logger.LogInformation("[{MeshId}] Subscribed to explicitly isolated inbound MQTT topic: {Topic}", meshId, topic);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to connect to MQTT broker mapping decoupled topics.", meshId);
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
                
            logger.LogInformation("[{MeshId}] Disconnected from explicit generic MQTT broker mapped connections.", meshId);
        }
    }

    /// <inheritdoc />
    public async Task PublishAsync(string targetClientId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetClientId);

        if (!mqttClient.IsConnected)
        {
            logger.LogWarning("[{MeshId}] Cannot publish generically decoupled payload natively, MQTT client is disconnected.", meshId);
            return;
        }

        var options = optionsMonitor.Get(meshId);
        var topic = GetTopicForClient(options.TopicPrefix, meshId, targetClientId);

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
        logger.LogWarning("[{MeshId}] Standard mapped MQTT connection lost. Reason: {Reason}", meshId, args.Reason);
        
        await Task.Delay(TimeSpan.FromSeconds(5)).ConfigureAwait(false); // TODO: Add/Use to options

        try
        {
            await StartAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to explicitly reconnect bound MQTT broker cleanly.", meshId);
        }
    }

    private static string GetTopicForClient(string prefix, string meshId, string clientId)
    {
        var p = string.IsNullOrWhiteSpace(prefix) ? "p2p" : prefix.Trim('/');
        return $"ama-enterprise/{p}/{meshId}/clients/{clientId}";
    }

    /// <inheritdoc />
    public void Dispose()
    {
        mqttClient.ApplicationMessageReceivedAsync -= HandleIncomingMessageAsync;
        mqttClient.DisconnectedAsync -= HandleDisconnectedAsync;
        mqttClient.Dispose();
    }
}