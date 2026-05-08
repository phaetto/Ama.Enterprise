namespace Ama.Enterprise.P2p.Mqtt.Services.Discovery;

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
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
    private readonly IOptionsMonitor<MqttDiscoveryOptions> discoveryOptionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly PeerEndpoint localEndpoint;
    private readonly IPeerHandshaker handshaker;
    private readonly ILogger<MqttPeerDiscovery> logger;
    private readonly IPeerRegistry peerRegistry;
    private readonly ICrdtSerializer serializer;
    private readonly IPeerAuthenticator authenticator;
    private readonly IFailureDetector failureDetector;

    private readonly IMqttClient mqttClient;
    private CancellationTokenSource? backgroundTaskCancellationSource;
    private Task? discoveryTask;
    private bool isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MqttPeerDiscovery"/> class.
    /// </summary>
    public MqttPeerDiscovery(
        string meshId,
        IOptionsMonitor<MqttDiscoveryOptions> discoveryOptionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        PeerEndpoint localEndpoint,
        IPeerHandshaker handshaker,
        ILogger<MqttPeerDiscovery> logger,
        IPeerRegistry peerRegistry,
        ICrdtSerializer serializer,
        IPeerAuthenticator authenticator,
        IFailureDetector failureDetector)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.discoveryOptionsMonitor = discoveryOptionsMonitor ?? throw new ArgumentNullException(nameof(discoveryOptionsMonitor));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.localEndpoint = localEndpoint ?? throw new ArgumentNullException(nameof(localEndpoint));
        this.handshaker = handshaker ?? throw new ArgumentNullException(nameof(handshaker));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.peerRegistry = peerRegistry ?? throw new ArgumentNullException(nameof(peerRegistry));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
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
            
            var nodeOptions = nodeOptionsMonitor.Get(meshId);
            
            logger.LogInformation("[{MeshId}] Connected to MQTT broker for Phase 1 discovery as {ClientId}.", meshId, $"ama-ent-{nodeOptions.LocalPeerId:N}-{meshId}-discovery");

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

        if (!mqttClient.IsConnected)
        {
            return Enumerable.Empty<PeerNode>();
        }

        var options = discoveryOptionsMonitor.Get(meshId);
        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        var discoveredPeers = new ConcurrentBag<PeerNode>();

        var factory = new MqttClientFactory();
        using var tempClient = factory.CreateMqttClient();

        var builder = new MqttClientOptionsBuilder()
            .WithTcpServer(options.Host, options.Port)
            .WithClientId($"ama-ent-{nodeOptions.LocalPeerId:N}-{meshId}-disc-{Guid.NewGuid():N}");

        if (!string.IsNullOrWhiteSpace(options.Username)) builder.WithCredentials(options.Username, options.Password);
        if (options.UseTls) builder.WithTlsOptions(o => o.UseTls());

        try
        {
            await tempClient.ConnectAsync(builder.Build(), cancellationToken).ConfigureAwait(false);

            var replyTopic = BuildDiscoveryReplyTopic(options.TopicPrefix, meshId, options.DiscoveryTopicSuffix, Guid.NewGuid().ToString("N"));
            var subscribeOptions = factory.CreateSubscribeOptionsBuilder().WithTopicFilter(f => f.WithTopic(replyTopic)).Build();
            
            await tempClient.SubscribeAsync(subscribeOptions, cancellationToken).ConfigureAwait(false);

            var discoveryPayload = new MqttDiscoveryMessage
            {
                MeshId = meshId,
                ClientId = nodeOptions.LocalPeerId.ToString("N"),
                IpAddress = GetLocalIpAddress(),
                HandshakePort = handshaker.LocalHandshakePort,
                ReplyToTopic = replyTopic
            };

            var requestBytes = serializer.SerializeToBytes(discoveryPayload);
            var broadcastTopic = BuildDiscoveryTopic(options.TopicPrefix, meshId, options.DiscoveryTopicSuffix);

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(broadcastTopic)
                .WithPayload(requestBytes)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce)
                .Build();

            await tempClient.PublishAsync(message, cancellationToken).ConfigureAwait(false);

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(options.DiscoveryTimeout);

            var localNode = new PeerNode(new PeerId(nodeOptions.LocalPeerId), localEndpoint);
            var handshakeTasks = new List<Task>();

            tempClient.ApplicationMessageReceivedAsync += e =>
            {
                try
                {
                    var pong = serializer.DeserializeFromBytes<MqttDiscoveryMessage>(e.ApplicationMessage.Payload.ToArray());
                    
                    if (pong.MeshId == meshId && pong.ClientId != nodeOptions.LocalPeerId.ToString("N"))
                    {
                        if (IPAddress.TryParse(pong.IpAddress, out var remoteIp))
                        {
                            var remotePort = pong.HandshakePort;
                            
                            handshakeTasks.Add(Task.Run(async () =>
                            {
                                var endpoint = new IPEndPoint(remoteIp, remotePort);
                                var remoteNode = await handshaker.HandshakeAsync(localNode, endpoint, timeoutCts.Token).ConfigureAwait(false);

                                if (!remoteNode.HasValue || remoteNode.Value.Id.Value == nodeOptions.LocalPeerId || remoteNode.Value.Id.Value == Guid.Empty)
                                {
                                    return;
                                }

                                var isAuthenticated = await authenticator.AuthenticateAsync(remoteNode.Value, ReadOnlyMemory<byte>.Empty, timeoutCts.Token).ConfigureAwait(false);

                                if (isAuthenticated)
                                {
                                    await failureDetector.RecordHeartbeatAsync(remoteNode.Value.Id, timeoutCts.Token).ConfigureAwait(false);
                                    discoveredPeers.Add(remoteNode.Value);
                                }
                            }, timeoutCts.Token));
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.LogTrace(ex, "[{MeshId}] Error processing MQTT discovery response.", meshId);
                }
                
                return Task.CompletedTask;
            };

            try
            {
                await Task.Delay(options.DiscoveryTimeout, timeoutCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }

            if (handshakeTasks.Count > 0)
            {
                try
                {
                    await Task.WhenAll(handshakeTasks).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { }
            }

            await tempClient.DisconnectAsync(new MqttClientDisconnectOptionsBuilder().WithReason(MqttClientDisconnectOptionsReason.NormalDisconnection).Build(), CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "[{MeshId}] Failed to execute active MQTT discovery.", meshId);
        }

        return discoveredPeers;
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

    private async Task ConnectAndSubscribeAsync(CancellationToken token)
    {
        var options = discoveryOptionsMonitor.Get(meshId);
        var nodeOptions = nodeOptionsMonitor.Get(meshId);

        var clientId = $"ama-ent-{nodeOptions.LocalPeerId:N}-{meshId}-discovery";

        var builder = new MqttClientOptionsBuilder()
            .WithTcpServer(options.Host, options.Port)
            .WithClientId(clientId);

        if (!string.IsNullOrWhiteSpace(options.Username))
        {
            builder.WithCredentials(options.Username, options.Password);
        }

        if (options.UseTls)
        {
            builder.WithTlsOptions(o => o.UseTls());
        }

        await mqttClient.ConnectAsync(builder.Build(), token).ConfigureAwait(false);

        var broadcastTopic = BuildDiscoveryTopic(options.TopicPrefix, meshId, options.DiscoveryTopicSuffix);

        var subscribeOptions = new MqttClientFactory().CreateSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic(broadcastTopic))
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
            var message = serializer.DeserializeFromBytes<MqttDiscoveryMessage>(payload.ToArray());

            if (message.MeshId == meshId && message.ClientId != nodeOptions.LocalPeerId.ToString("N") && !string.IsNullOrWhiteSpace(message.ReplyToTopic))
            {
                var pong = new MqttDiscoveryMessage
                {
                    MeshId = meshId,
                    ClientId = nodeOptions.LocalPeerId.ToString("N"),
                    IpAddress = GetLocalIpAddress(),
                    HandshakePort = handshaker.LocalHandshakePort,
                    ReplyToTopic = string.Empty
                };

                var responseBytes = serializer.SerializeToBytes(pong);

                var responseMessage = new MqttApplicationMessageBuilder()
                    .WithTopic(message.ReplyToTopic)
                    .WithPayload(responseBytes)
                    .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce)
                    .Build();

                await mqttClient.PublishAsync(responseMessage, CancellationToken.None).ConfigureAwait(false);

                // Actively reverse handshake to register the discovering peer avoiding one-sided topologies
                var token = backgroundTaskCancellationSource?.Token ?? CancellationToken.None;
                _ = Task.Run(async () =>
                {
                    try
                    {
                        if (IPAddress.TryParse(message.IpAddress, out var remoteIp))
                        {
                            var options = discoveryOptionsMonitor.Get(meshId);
                            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                            timeoutCts.CancelAfter(options.DiscoveryTimeout);

                            var endpoint = new IPEndPoint(remoteIp, message.HandshakePort);
                            var localNode = new PeerNode(new PeerId(nodeOptions.LocalPeerId), localEndpoint);

                            var remoteNode = await handshaker.HandshakeAsync(localNode, endpoint, timeoutCts.Token).ConfigureAwait(false);

                            if (remoteNode.HasValue && remoteNode.Value.Id.Value != nodeOptions.LocalPeerId && remoteNode.Value.Id.Value != Guid.Empty)
                            {
                                var isAuthenticated = await authenticator.AuthenticateAsync(remoteNode.Value, ReadOnlyMemory<byte>.Empty, timeoutCts.Token).ConfigureAwait(false);

                                if (isAuthenticated)
                                {
                                    await failureDetector.RecordHeartbeatAsync(remoteNode.Value.Id, timeoutCts.Token).ConfigureAwait(false);
                                    await peerRegistry.AddOrUpdatePeerAsync(meshId, remoteNode.Value, PeerStatus.Active, timeoutCts.Token).ConfigureAwait(false);
                                }
                            }
                        }
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex)
                    {
                        logger.LogTrace(ex, "[{MeshId}] Failed to process active reverse handshake via MQTT.", meshId);
                    }
                }, token);
            }
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "[{MeshId}] Ignored malformed MQTT discovery message.", meshId);
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
            await Task.Delay(TimeSpan.FromSeconds(1), token).ConfigureAwait(false);
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

    private static string BuildDiscoveryTopic(string prefix, string meshId, string suffix)
    {
        var p = string.IsNullOrWhiteSpace(prefix) ? "p2p" : prefix.Trim('/');
        var s = string.IsNullOrWhiteSpace(suffix) ? "discovery" : suffix.Trim('/');
        return $"ama-enterprise/{p}/{meshId}/{s}";
    }

    private static string BuildDiscoveryReplyTopic(string prefix, string meshId, string suffix, string replyId)
    {
        return $"{BuildDiscoveryTopic(prefix, meshId, suffix)}/replies/{replyId}";
    }

    private static string GetLocalIpAddress()
    {
        try
        {
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, 0);
            socket.Connect("8.8.8.8", 65530);
            var endPoint = socket.LocalEndPoint as IPEndPoint;
            return endPoint?.Address.ToString() ?? "127.0.0.1";
        }
        catch
        {
            try
            {
                var host = Dns.GetHostEntry(Dns.GetHostName());
                foreach (var ip in host.AddressList)
                {
                    if (ip.AddressFamily == AddressFamily.InterNetwork)
                    {
                        return ip.ToString();
                    }
                }
            }
            catch { }
            return "127.0.0.1";
        }
    }
}