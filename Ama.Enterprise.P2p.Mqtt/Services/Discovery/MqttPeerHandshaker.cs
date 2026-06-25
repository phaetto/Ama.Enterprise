namespace Ama.Enterprise.P2p.Mqtt.Services.Discovery;

using System;
using System.Buffers;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Mqtt.Models;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MQTTnet;

/// <summary>
/// Implementation of IPeerHandshaker orchestrating isolated Phase 2 MQTT unicast negotiations.
/// </summary>
public sealed class MqttPeerHandshaker : IPeerHandshaker, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<MqttHandshakeOptions> optionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly PeerEndpoint localEndpoint;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<MqttPeerHandshaker> logger;
    private readonly IPeerAuthenticator authenticator;
    private readonly IPeerRegistry peerRegistry;
    private readonly IFailureDetector failureDetector;

    private IMqttClient? listener;
    private CancellationTokenSource? backgroundTaskCancellationSource;
    private bool isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MqttPeerHandshaker"/> class.
    /// </summary>
    public MqttPeerHandshaker(
        string meshId,
        IOptionsMonitor<MqttHandshakeOptions> optionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        PeerEndpoint localEndpoint,
        ICrdtSerializer serializer,
        ILogger<MqttPeerHandshaker> logger,
        IPeerAuthenticator authenticator,
        IPeerRegistry peerRegistry,
        IFailureDetector failureDetector)
    {
        ArgumentNullException.ThrowIfNull(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(nodeOptionsMonitor);
        ArgumentNullException.ThrowIfNull(localEndpoint);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(authenticator);
        ArgumentNullException.ThrowIfNull(peerRegistry);
        ArgumentNullException.ThrowIfNull(failureDetector);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.nodeOptionsMonitor = nodeOptionsMonitor;
        this.localEndpoint = localEndpoint;
        this.serializer = serializer;
        this.logger = logger;
        this.authenticator = authenticator;
        this.peerRegistry = peerRegistry;
        this.failureDetector = failureDetector;
    }

    /// <inheritdoc />
    public int LocalHandshakePort => optionsMonitor.Get(meshId).HandshakePort;

    /// <inheritdoc />
    public async Task StartListeningAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        backgroundTaskCancellationSource = new CancellationTokenSource();

        try
        {
            var factory = new MqttClientFactory();
            listener = factory.CreateMqttClient();
            listener.ApplicationMessageReceivedAsync += HandleIncomingMessageAsync;
            listener.DisconnectedAsync += HandleDisconnectedAsync;

            await ConnectAndSubscribeAsync(cancellationToken).ConfigureAwait(false);

            var options = optionsMonitor.Get(meshId);
            var endpointStr = options.HandshakePort > 0 ? $"{GetLocalIpAddress()}:{options.HandshakePort}" : GetLocalIpAddress();
            var topic = BuildHandshakeTopic(options.TopicPrefix, meshId, options.HandshakeTopicSuffix, endpointStr);
            
            logger.LogInformation("[{MeshId}] MQTT Peer Handshaker started explicitly listening on targeted topic: {Topic}", meshId, topic);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to initialize explicit MQTT Phase 2 passive listener bounds.", meshId);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task StopListeningAsync(CancellationToken cancellationToken)
    {
        if (backgroundTaskCancellationSource is not null)
        {
            await backgroundTaskCancellationSource.CancelAsync().ConfigureAwait(false);
        }

        if (listener is not null && listener.IsConnected)
        {
            await listener.DisconnectAsync(new MqttClientDisconnectOptionsBuilder()
                .WithReason(MqttClientDisconnectOptionsReason.NormalDisconnection)
                .Build(), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<PeerHandshakePayload?> HandshakeAsync(PeerHandshakePayload localPayload, IPEndPoint endpoint, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        ArgumentNullException.ThrowIfNull(endpoint);

        var options = optionsMonitor.Get(meshId);
        var nodeOptions = nodeOptionsMonitor.Get(meshId);

        var factory = new MqttClientFactory();
        using var tempClient = factory.CreateMqttClient();

        var builder = new MqttClientOptionsBuilder()
            .WithTcpServer(options.Host, options.Port)
            .WithClientId($"ama-ent-{nodeOptions.LocalPeerId:N}-{meshId}-hs-{Guid.NewGuid():N}");

        if (!string.IsNullOrWhiteSpace(options.Username)) builder.WithCredentials(options.Username, options.Password);
        if (options.UseTls) builder.WithTlsOptions(o => o.UseTls());

        try
        {
            await tempClient.ConnectAsync(builder.Build(), cancellationToken).ConfigureAwait(false);

            var replyTopic = BuildHandshakeReplyTopic(options.TopicPrefix, meshId, options.HandshakeTopicSuffix, Guid.NewGuid().ToString("N"));
            var subscribeOptions = factory.CreateSubscribeOptionsBuilder().WithTopicFilter(f => f.WithTopic(replyTopic)).Build();
            
            await tempClient.SubscribeAsync(subscribeOptions, cancellationToken).ConfigureAwait(false);

            var requestPayloadBytes = serializer.SerializeToBytes(localPayload);
            var handshakeMessage = new MqttHandshakeMessage
            {
                ReplyToTopic = replyTopic,
                Payload = requestPayloadBytes
            };
            
            var requestBytes = serializer.SerializeToBytes(handshakeMessage);

            var endpointStr = endpoint.Port > 0 ? endpoint.ToString() : endpoint.Address.ToString();
            var targetTopic = BuildHandshakeTopic(options.TopicPrefix, meshId, options.HandshakeTopicSuffix, endpointStr);

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(targetTopic)
                .WithPayload(requestBytes)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce)
                .Build();

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(options.HandshakeTimeout);

            var tcs = new TaskCompletionSource<PeerHandshakePayload?>(TaskCreationOptions.RunContinuationsAsynchronously);
            timeoutCts.Token.Register(() => tcs.TrySetCanceled());

            tempClient.ApplicationMessageReceivedAsync += e =>
            {
                try
                {
                    var responseMessage = serializer.DeserializeFromBytes<MqttHandshakeMessage>(e.ApplicationMessage.Payload.ToArray());
                    var remotePayload = serializer.DeserializeFromBytes<PeerHandshakePayload>(responseMessage.Payload);
                    tcs.TrySetResult(remotePayload);
                }
                catch
                {
                    // Ignore malformed responses
                }
                return Task.CompletedTask;
            };

            await tempClient.PublishAsync(message, cancellationToken).ConfigureAwait(false);

            var result = await tcs.Task.ConfigureAwait(false);
            
            await tempClient.DisconnectAsync(new MqttClientDisconnectOptionsBuilder().WithReason(MqttClientDisconnectOptionsReason.NormalDisconnection).Build(), CancellationToken.None).ConfigureAwait(false);
            
            return result;
        }
        catch (OperationCanceledException)
        {
            logger.LogTrace("[{MeshId}] MQTT handshake timed out for {Target}.", meshId, endpoint);
            return null;
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "[{MeshId}] MQTT handshake failed for {Target}.", meshId, endpoint);
            return null;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (isDisposed) return;
        backgroundTaskCancellationSource?.Cancel();
        backgroundTaskCancellationSource?.Dispose();
        
        if (listener is not null)
        {
            listener.ApplicationMessageReceivedAsync -= HandleIncomingMessageAsync;
            listener.DisconnectedAsync -= HandleDisconnectedAsync;
            listener.Dispose();
        }
        
        isDisposed = true;
    }

    private async Task ConnectAndSubscribeAsync(CancellationToken token)
    {
        var options = optionsMonitor.Get(meshId);
        var nodeOptions = nodeOptionsMonitor.Get(meshId);

        var clientId = $"ama-ent-{nodeOptions.LocalPeerId:N}-{meshId}-handshaker";

        var builder = new MqttClientOptionsBuilder()
            .WithTcpServer(options.Host, options.Port)
            .WithClientId(clientId);

        if (!string.IsNullOrWhiteSpace(options.Username)) builder.WithCredentials(options.Username, options.Password);
        if (options.UseTls) builder.WithTlsOptions(o => o.UseTls());

        await listener!.ConnectAsync(builder.Build(), token).ConfigureAwait(false);

        var endpointStr = options.HandshakePort > 0 ? $"{GetLocalIpAddress()}:{options.HandshakePort}" : GetLocalIpAddress();
        var topic = BuildHandshakeTopic(options.TopicPrefix, meshId, options.HandshakeTopicSuffix, endpointStr);
        
        var subscribeOptions = new MqttClientFactory().CreateSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic(topic))
            .Build();

        await listener.SubscribeAsync(subscribeOptions, token).ConfigureAwait(false);
    }

    private async Task HandleIncomingMessageAsync(MqttApplicationMessageReceivedEventArgs args)
    {
        var payload = args.ApplicationMessage.Payload;
        if (payload.Length == 0) return;

        var nodeOptions = nodeOptionsMonitor.Get(meshId);

        try
        {
            var requestMessage = serializer.DeserializeFromBytes<MqttHandshakeMessage>(payload.ToArray());
            var remotePayload = serializer.DeserializeFromBytes<PeerHandshakePayload>(requestMessage.Payload);

            if (remotePayload.Node.Id.Value != nodeOptions.LocalPeerId && remotePayload.Node.Id.Value != Guid.Empty && !string.IsNullOrWhiteSpace(requestMessage.ReplyToTopic))
            {
                var localNode = new PeerNode(new PeerId(nodeOptions.LocalPeerId), localEndpoint);
                var localHandshakeData = await authenticator.GetLocalHandshakeDataAsync(CancellationToken.None).ConfigureAwait(false);
                var responsePayloadStruct = new PeerHandshakePayload
                {
                    Node = localNode,
                    HandshakeData = localHandshakeData.ToArray()
                };
                
                var responsePayloadBytes = serializer.SerializeToBytes(responsePayloadStruct);
                
                var responseMessage = new MqttHandshakeMessage
                {
                    ReplyToTopic = string.Empty,
                    Payload = responsePayloadBytes
                };
                
                var responseBytes = serializer.SerializeToBytes(responseMessage);

                var message = new MqttApplicationMessageBuilder()
                    .WithTopic(requestMessage.ReplyToTopic)
                    .WithPayload(responseBytes)
                    .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce)
                    .Build();

                await listener!.PublishAsync(message, CancellationToken.None).ConfigureAwait(false);

                try
                {
                    var isAuthenticated = await authenticator.AuthenticateAsync(remotePayload.Node, remotePayload.HandshakeData, CancellationToken.None).ConfigureAwait(false);

                    if (isAuthenticated)
                    {
                        await failureDetector.RecordHeartbeatAsync(remotePayload.Node.Id, CancellationToken.None).ConfigureAwait(false);
                        await peerRegistry.AddOrUpdatePeerAsync(meshId, remotePayload.Node, PeerStatus.Active, CancellationToken.None).ConfigureAwait(false);
                    }
                }
                catch (Exception authEx)
                {
                    logger.LogWarning(authEx, "[{MeshId}] Inbound MQTT handshake authentication failed for {PeerId}.", meshId, remotePayload.Node.Id.Value);
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "[{MeshId}] Ignored generic handshake request explicitly avoiding failures locally.", meshId);
        }
    }

    private async Task HandleDisconnectedAsync(MqttClientDisconnectedEventArgs args)
    {
        if (backgroundTaskCancellationSource?.IsCancellationRequested == true) return;

        logger.LogWarning("[{MeshId}] MQTT handshaker disconnected. Reason: {Reason}", meshId, args.Reason);

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
            logger.LogError(ex, "[{MeshId}] Failed to reconnect MQTT handshaker natively mapped actively effectively.", meshId);
        }
    }

    private static string BuildHandshakeTopic(string prefix, string meshId, string suffix, string endpointStr)
    {
        var p = string.IsNullOrWhiteSpace(prefix) ? "p2p" : prefix.Trim('/');
        var s = string.IsNullOrWhiteSpace(suffix) ? "handshake" : suffix.Trim('/');
        return $"ama-enterprise/{p}/{meshId}/{s}/{endpointStr}";
    }

    private static string BuildHandshakeReplyTopic(string prefix, string meshId, string suffix, string replyId)
    {
        var p = string.IsNullOrWhiteSpace(prefix) ? "p2p" : prefix.Trim('/');
        var s = string.IsNullOrWhiteSpace(suffix) ? "handshake" : suffix.Trim('/');
        return $"ama-enterprise/{p}/{meshId}/{s}/replies/{replyId}";
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