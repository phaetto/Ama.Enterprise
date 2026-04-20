namespace Ama.Enterprise.P2p.Mqtt.Services.Discovery;

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Mqtt.Models;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MQTTnet;

/// <summary>
/// Implementation of IPeerDiscovery using a shared MQTT topic for centralized broker-based discovery.
/// </summary>
public sealed class MqttPeerDiscovery : IPeerDiscovery, IHostedService, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<MqttTransportOptions> transportOptionsMonitor;
    private readonly IOptionsMonitor<MqttDiscoveryOptions> discoveryOptionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly ILogger<MqttPeerDiscovery> logger;
    private readonly IPeerRegistry peerRegistry;
    private readonly IPeerAuthenticator authenticator;
    private readonly IFailureDetector failureDetector;

    private readonly IMqttClient mqttClient;
    private readonly ConcurrentDictionary<Guid, PeerNode> recentPeers = new();
    private CancellationTokenSource? backgroundTaskCancellationSource;
    private Task? discoveryTask;
    private bool isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MqttPeerDiscovery"/> class.
    /// </summary>
    public MqttPeerDiscovery(
        string meshId,
        IOptionsMonitor<MqttTransportOptions> transportOptionsMonitor,
        IOptionsMonitor<MqttDiscoveryOptions> discoveryOptionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        ILogger<MqttPeerDiscovery> logger,
        IPeerRegistry peerRegistry,
        IPeerAuthenticator authenticator,
        IFailureDetector failureDetector)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.transportOptionsMonitor = transportOptionsMonitor ?? throw new ArgumentNullException(nameof(transportOptionsMonitor));
        this.discoveryOptionsMonitor = discoveryOptionsMonitor ?? throw new ArgumentNullException(nameof(discoveryOptionsMonitor));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.peerRegistry = peerRegistry ?? throw new ArgumentNullException(nameof(peerRegistry));
        this.authenticator = authenticator ?? throw new ArgumentNullException(nameof(authenticator));
        this.failureDetector = failureDetector ?? throw new ArgumentNullException(nameof(failureDetector));

        var factory = new MqttClientFactory();
        this.mqttClient = factory.CreateMqttClient();
        
        this.mqttClient.ApplicationMessageReceivedAsync += HandleIncomingMessageAsync;
        this.mqttClient.DisconnectedAsync += HandleDisconnectedAsync;
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        backgroundTaskCancellationSource = new CancellationTokenSource();

        try
        {
            await ConnectAndSubscribeAsync(cancellationToken).ConfigureAwait(false);
            
            var transOptions = transportOptionsMonitor.Get(meshId);
            var nodeOptions = nodeOptionsMonitor.Get(meshId);
            
            logger.LogInformation("[{MeshId}] Connected to MQTT broker for discovery as {ClientId}.", meshId, $"{nodeOptions.LocalPeerId:N}-discovery");

            discoveryTask = DiscoveryLoopAsync(backgroundTaskCancellationSource.Token);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to start MQTT peer discovery.", meshId);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (backgroundTaskCancellationSource is not null)
        {
            await backgroundTaskCancellationSource.CancelAsync().ConfigureAwait(false);
        }

        if (discoveryTask is not null)
        {
            try
            {
                await discoveryTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }

        if (mqttClient.IsConnected)
        {
            await mqttClient.DisconnectAsync(new MqttClientDisconnectOptionsBuilder()
                .WithReason(MqttClientDisconnectOptionsReason.NormalDisconnection)
                .Build(), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<IEnumerable<PeerNode>> DiscoverPeersAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        recentPeers.Clear();

        if (!mqttClient.IsConnected)
        {
            return Enumerable.Empty<PeerNode>();
        }

        var transOptions = transportOptionsMonitor.Get(meshId);
        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        var discOptions = discoveryOptionsMonitor.Get(meshId);

        var topic = GetDiscoveryTopic(transOptions.TopicPrefix, discOptions.DiscoveryTopicSuffix);
        var localId = new PeerId(nodeOptions.LocalPeerId);

        var payload = localId.Value.ToByteArray();

        var message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(payload)
            .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce)
            .Build();

        try
        {
            await mqttClient.PublishAsync(message, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to publish MQTT discovery presence ping.", meshId);
        }

        try
        {
            await Task.Delay(discOptions.DiscoveryTimeout, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }

        return recentPeers.Values.ToList();
    }

    private async Task ConnectAndSubscribeAsync(CancellationToken token)
    {
        var transOptions = transportOptionsMonitor.Get(meshId);
        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        var discOptions = discoveryOptionsMonitor.Get(meshId);

        var clientId = $"{nodeOptions.LocalPeerId:N}-discovery";

        var builder = new MqttClientOptionsBuilder()
            .WithTcpServer(transOptions.Host, transOptions.Port)
            .WithClientId(clientId);

        if (!string.IsNullOrWhiteSpace(transOptions.Username))
        {
            builder.WithCredentials(transOptions.Username, transOptions.Password);
        }

        if (transOptions.UseTls)
        {
            builder.WithTlsOptions(o => o.UseTls());
        }

        await mqttClient.ConnectAsync(builder.Build(), token).ConfigureAwait(false);

        var topic = GetDiscoveryTopic(transOptions.TopicPrefix, discOptions.DiscoveryTopicSuffix);
        var subscribeOptions = new MqttClientFactory().CreateSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic(topic))
            .Build();

        await mqttClient.SubscribeAsync(subscribeOptions, token).ConfigureAwait(false);
    }

    private async Task HandleIncomingMessageAsync(MqttApplicationMessageReceivedEventArgs args)
    {
        var payload = args.ApplicationMessage.Payload;
        if (payload.Length == 0) return;

        var nodeOptions = nodeOptionsMonitor.Get(meshId);

        try
        {
            if (payload.Length != 16)
            {
                logger.LogTrace("[{MeshId}] Received an invalid MQTT discovery payload of length {Length}.", meshId, payload.Length);
                return;
            }

            var remoteIdValue = new Guid(payload.ToArray());

            if (remoteIdValue != nodeOptions.LocalPeerId && remoteIdValue != Guid.Empty)
            {
                var remoteNode = new PeerNode(new PeerId(remoteIdValue), new MqttPeerEndpoint(remoteIdValue.ToString("N")));

                var isAuthenticated = await authenticator.AuthenticateAsync(remoteNode, ReadOnlyMemory<byte>.Empty, CancellationToken.None).ConfigureAwait(false);

                if (isAuthenticated)
                {
                    await failureDetector.RecordHeartbeatAsync(remoteNode.Id, CancellationToken.None).ConfigureAwait(false);
                    await peerRegistry.AddOrUpdatePeerAsync(meshId, remoteNode, PeerStatus.Active, CancellationToken.None).ConfigureAwait(false);
                    recentPeers[remoteNode.Id.Value] = remoteNode;
                }
                else
                {
                    logger.LogDebug("[{MeshId}] Incoming MQTT discovery presence from {PeerId} failed authentication.", meshId, remoteIdValue);
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "[{MeshId}] An error occurred while processing an MQTT discovery broadcast.", meshId);
        }
    }

    private async Task HandleDisconnectedAsync(MqttClientDisconnectedEventArgs args)
    {
        if (backgroundTaskCancellationSource?.IsCancellationRequested == true) return;

        logger.LogWarning("[{MeshId}] MQTT discovery connection lost. Reason: {Reason}", meshId, args.Reason);

        try
        {
            await Task.Delay(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            
            if (backgroundTaskCancellationSource?.IsCancellationRequested == false)
            {
                await ConnectAndSubscribeAsync(backgroundTaskCancellationSource.Token).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to automatically reconnect MQTT discovery client.", meshId);
        }
    }

    private async Task DiscoveryLoopAsync(CancellationToken token)
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(2), token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { return; }

        while (!token.IsCancellationRequested)
        {
            var options = discoveryOptionsMonitor.Get(meshId);

            try
            {
                var discoveredPeers = await DiscoverPeersAsync(token).ConfigureAwait(false);

                foreach (var peer in discoveredPeers)
                {
                    await peerRegistry.AddOrUpdatePeerAsync(meshId, peer, PeerStatus.Active, token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Unexpected error in MQTT peer discovery loop.", meshId);
            }

            if (token.IsCancellationRequested) break;

            try
            {
                await Task.Delay(options.DiscoveryInterval, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }
    }

    private static string GetDiscoveryTopic(string prefix, string suffix)
    {
        return $"{prefix.TrimEnd('/')}/{suffix.TrimStart('/')}";
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (isDisposed) return;
        backgroundTaskCancellationSource?.Cancel();
        backgroundTaskCancellationSource?.Dispose();
        mqttClient.ApplicationMessageReceivedAsync -= HandleIncomingMessageAsync;
        mqttClient.DisconnectedAsync -= HandleDisconnectedAsync;
        mqttClient.Dispose();
        isDisposed = true;
    }
}