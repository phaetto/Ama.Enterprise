namespace Ama.Enterprise.P2p.WebRTC.AzureEventGrid.Services;

using System;
using System.Buffers;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.WebRTC.AzureEventGrid.Models;
using Ama.Enterprise.P2p.WebRTC.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Protocol;

/// <summary>
/// Background hosted service continuously managing an MQTT connection to broker WebRTC SDP invitations.
/// Utilizes a persistent push-based connection (e.g., Azure Event Grid MQTT) combining both offer generation and incoming answer processing.
/// </summary>
public sealed class MqttSignalingService : BackgroundService
{
    private readonly string meshId;
    private readonly IOptionsMonitor<MqttSignalingOptions> optionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly IServiceProvider serviceProvider;
    private readonly ILogger<MqttSignalingService> logger;
    
    private IMqttClient? mqttClient;
    private Guid? currentOfferConnectionId;
    private DateTimeOffset? currentOfferCreatedAt;

    /// <summary>
    /// Initializes a new instance of the <see cref="MqttSignalingService"/> class.
    /// </summary>
    public MqttSignalingService(
        string meshId,
        IOptionsMonitor<MqttSignalingOptions> optionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        IServiceProvider serviceProvider,
        ILogger<MqttSignalingService> logger)
    {
        ArgumentNullException.ThrowIfNull(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(nodeOptionsMonitor);
        ArgumentNullException.ThrowIfNull(serviceProvider);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.nodeOptionsMonitor = nodeOptionsMonitor;
        this.serviceProvider = serviceProvider;
        this.logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new MqttClientFactory();
        mqttClient = factory.CreateMqttClient();

        mqttClient.ApplicationMessageReceivedAsync += async e =>
        {
            await ProcessIncomingMessageAsync(e.ApplicationMessage, stoppingToken).ConfigureAwait(false);
        };

        mqttClient.DisconnectedAsync += async e =>
        {
            logger.LogWarning("[{MeshId}] MQTT client disconnected from broker.", meshId);
            await Task.CompletedTask.ConfigureAwait(false);
        };

        while (!stoppingToken.IsCancellationRequested)
        {
            var options = optionsMonitor.Get(meshId);

            try
            {
                if (string.IsNullOrWhiteSpace(options.Host))
                {
                    logger.LogWarning("[{MeshId}] MQTT Host is empty. Signaling service is dormant.", meshId);
                    await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken).ConfigureAwait(false);
                    continue;
                }

                await EnsureConnectionAsync(options, stoppingToken).ConfigureAwait(false);

                if (options.EnableOfferGeneration)
                {
                    await ProcessOfferGenerationAsync(options, stoppingToken).ConfigureAwait(false);
                }

                await Task.Delay(options.OfferInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Error observed during WebRTC MQTT signaling loop.", meshId);
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken).ConfigureAwait(false);
            }
        }

        if (mqttClient.IsConnected)
        {
            await mqttClient.DisconnectAsync(cancellationToken: CancellationToken.None).ConfigureAwait(false);
        }
        
        mqttClient.Dispose();
    }

    private async Task EnsureConnectionAsync(MqttSignalingOptions options, CancellationToken cancellationToken)
    {
        if (mqttClient != null && mqttClient.IsConnected)
        {
            return;
        }

        var clientOptionsBuilder = new MqttClientOptionsBuilder()
            .WithClientId(options.ClientId)
            .WithTcpServer(options.Host, options.Port);

        if (options.UseTls)
        {
            clientOptionsBuilder.WithTlsOptions(o => o.UseTls());
        }

        if (!string.IsNullOrWhiteSpace(options.Username))
        {
            clientOptionsBuilder.WithCredentials(options.Username, options.Password);
        }

        await mqttClient!.ConnectAsync(clientOptionsBuilder.Build(), cancellationToken).ConfigureAwait(false);

        // Subscribe to offers topic
        var offersTopic = $"p2p/{meshId}/webrtc/offers";
        await mqttClient.SubscribeAsync(offersTopic, cancellationToken: cancellationToken).ConfigureAwait(false);
        
        // Subscribe to answers topic
        var answersTopic = $"p2p/{meshId}/webrtc/answers/+";
        await mqttClient.SubscribeAsync(answersTopic, cancellationToken: cancellationToken).ConfigureAwait(false);

        logger.LogInformation("[{MeshId}] MQTT Connected and subscribed to signaling topics.", meshId);
    }

    private async Task ProcessOfferGenerationAsync(MqttSignalingOptions options, CancellationToken cancellationToken)
    {
        // Expire outdated offers
        if (currentOfferConnectionId.HasValue && currentOfferCreatedAt.HasValue)
        {
            if (currentOfferCreatedAt.Value < DateTimeOffset.UtcNow.Subtract(options.OfferExpiration))
            {
                currentOfferConnectionId = null;
                currentOfferCreatedAt = null;
            }
        }

        if (!currentOfferConnectionId.HasValue)
        {
            var invitationService = serviceProvider.GetRequiredKeyedService<IWebRtcInvitationService>(meshId);
            var nodeOptions = nodeOptionsMonitor.Get(meshId);
            
            var invitationResult = await invitationService.CreateInvitationAsync(cancellationToken).ConfigureAwait(false);
            
            currentOfferConnectionId = invitationResult.ConnectionId;
            currentOfferCreatedAt = DateTimeOffset.UtcNow;

            var payload = new MqttSignalingOfferPayload(
                ConnectionId: invitationResult.ConnectionId.ToString(),
                CreatorPeerId: nodeOptions.LocalPeerId.ToString(),
                OfferSdp: invitationResult.SdpOffer,
                CreatedAt: currentOfferCreatedAt.Value
            );

            var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(payload, MqttSignalingJsonContext.Default.MqttSignalingOfferPayload);

            var message = new MqttApplicationMessageBuilder()
                .WithTopic($"p2p/{meshId}/webrtc/offers")
                .WithPayload(jsonBytes)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await mqttClient!.PublishAsync(message, cancellationToken).ConfigureAwait(false);
            
            logger.LogDebug("[{MeshId}] Published WebRTC signaling offer {ConnectionId} over MQTT.", meshId, currentOfferConnectionId.Value);
        }
    }

    private async Task ProcessIncomingMessageAsync(MqttApplicationMessage message, CancellationToken cancellationToken)
    {
        var topic = message.Topic;
        var payloadSequence = message.Payload;

        if (payloadSequence.IsEmpty)
        {
            return;
        }

        var payloadBytes = payloadSequence.ToArray();

        if (topic.EndsWith("/offers", StringComparison.OrdinalIgnoreCase))
        {
            await HandleOfferAsync(payloadBytes, cancellationToken).ConfigureAwait(false);
        }
        else if (topic.Contains("/answers/", StringComparison.OrdinalIgnoreCase))
        {
            await HandleAnswerAsync(payloadBytes, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task HandleOfferAsync(byte[] payloadBytes, CancellationToken cancellationToken)
    {
        var options = optionsMonitor.Get(meshId);
        if (!options.EnableOfferAcceptance)
        {
            return;
        }

        try
        {
            var offer = JsonSerializer.Deserialize(payloadBytes, MqttSignalingJsonContext.Default.MqttSignalingOfferPayload);
            var nodeOptions = nodeOptionsMonitor.Get(meshId);

            if (offer.CreatorPeerId == nodeOptions.LocalPeerId.ToString())
            {
                return; // Discard localized loopback
            }

            if (offer.CreatedAt < DateTimeOffset.UtcNow.Subtract(options.OfferExpiration))
            {
                return; // Discard stale offers
            }

            var invitationService = serviceProvider.GetRequiredKeyedService<IWebRtcInvitationService>(meshId);
            var acceptResult = await invitationService.AcceptInvitationAsync(offer.OfferSdp, cancellationToken).ConfigureAwait(false);

            var answerPayload = new MqttSignalingAnswerPayload(
                ConnectionId: offer.ConnectionId,
                ResponderPeerId: nodeOptions.LocalPeerId.ToString(),
                AnswerSdp: acceptResult.SdpAnswer
            );

            var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(answerPayload, MqttSignalingJsonContext.Default.MqttSignalingAnswerPayload);

            var answerMessage = new MqttApplicationMessageBuilder()
                .WithTopic($"p2p/{meshId}/webrtc/answers/{offer.ConnectionId}")
                .WithPayload(jsonBytes)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await mqttClient!.PublishAsync(answerMessage, cancellationToken).ConfigureAwait(false);
            
            logger.LogInformation("[{MeshId}] Answered remote WebRTC offer {ConnectionId} via MQTT.", meshId, offer.ConnectionId);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to process inbound MQTT offer.", meshId);
        }
    }

    private async Task HandleAnswerAsync(byte[] payloadBytes, CancellationToken cancellationToken)
    {
        try
        {
            var answer = JsonSerializer.Deserialize(payloadBytes, MqttSignalingJsonContext.Default.MqttSignalingAnswerPayload);

            if (currentOfferConnectionId.HasValue && string.Equals(answer.ConnectionId, currentOfferConnectionId.Value.ToString(), StringComparison.OrdinalIgnoreCase))
            {
                var invitationService = serviceProvider.GetRequiredKeyedService<IWebRtcInvitationService>(meshId);
                await invitationService.FinalizeInvitationAsync(currentOfferConnectionId.Value, answer.AnswerSdp, cancellationToken).ConfigureAwait(false);

                logger.LogInformation("[{MeshId}] Finalized WebRTC connection {ConnectionId} from MQTT answer.", meshId, currentOfferConnectionId.Value);

                currentOfferConnectionId = null;
                currentOfferCreatedAt = null;
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to process inbound MQTT answer.", meshId);
        }
    }
}