namespace Ama.Enterprise.P2p.Services.Discovery;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Discovery;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation of IPeerDiscovery acting as Phase 1, using UDP multicast natively explicitly decoupled from orchestration polling dynamically safely efficiently explicitly mapped natively globally.
/// </summary>
public sealed class UdpPeerDiscovery : IPeerDiscovery, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<UdpDiscoveryOptions> discoveryOptionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly PeerEndpoint localEndpoint;
    private readonly IPeerHandshaker handshaker;
    private readonly ILogger<UdpPeerDiscovery> logger;
    private readonly IPeerRegistry peerRegistry;
    private readonly ICrdtSerializer serializer;
    private readonly IPeerAuthenticator authenticator;
    private readonly IFailureDetector failureDetector;

    private UdpClient? listener;
    private CancellationTokenSource? backgroundTaskCancellationSource;
    private Task? listenTask;
    private bool isDisposed;

    private readonly Meter meter;
    private readonly Counter<long> multicastsSentCounter;
    private readonly Counter<long> multicastsReceivedCounter;
    private readonly Counter<long> peersFoundCounter;

    public UdpPeerDiscovery(
        string meshId,
        IOptionsMonitor<UdpDiscoveryOptions> discoveryOptionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        PeerEndpoint localEndpoint,
        IPeerHandshaker handshaker,
        ILogger<UdpPeerDiscovery> logger,
        IPeerRegistry peerRegistry,
        ICrdtSerializer serializer,
        IPeerAuthenticator authenticator,
        IFailureDetector failureDetector,
        IMeterFactory? meterFactory = null)
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

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.UdpPeerDiscovery") ?? new Meter("Ama.Enterprise.P2p.UdpPeerDiscovery");
        this.multicastsSentCounter = this.meter.CreateCounter<long>("p2p.discovery.udp.multicasts_sent", "multicasts", "Total UDP multicasts sent");
        this.multicastsReceivedCounter = this.meter.CreateCounter<long>("p2p.discovery.udp.multicasts_received", "multicasts", "Total UDP multicasts received");
        this.peersFoundCounter = this.meter.CreateCounter<long>("p2p.discovery.udp.peers_found", "peers", "Total peers successfully found via UDP");
    }

    /// <inheritdoc />
    public TimeSpan DiscoveryInterval => discoveryOptionsMonitor.Get(meshId).DiscoveryInterval;

    /// <inheritdoc />
    public Task StartListeningAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        var options = discoveryOptionsMonitor.Get(meshId);
        backgroundTaskCancellationSource = new CancellationTokenSource();

        var multicastAddress = IPAddress.Parse(options.MulticastAddress);
        var addressFamily = multicastAddress.AddressFamily;

        var localIpEndpoint = new IPEndPoint(addressFamily == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Any : IPAddress.Any, options.MulticastPort);
        listener = new UdpClient(addressFamily);

        listener.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        listener.Client.Bind(localIpEndpoint);
        listener.JoinMulticastGroup(multicastAddress);

        listenTask = ListenLoopAsync(backgroundTaskCancellationSource.Token);

        logger.LogInformation(
            "[{MeshId}] UDP Peer Discovery started passive listening natively on multicast group {Address}:{Port}",
            meshId,
            options.MulticastAddress,
            options.MulticastPort);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopListeningAsync(CancellationToken cancellationToken)
    {
        if (backgroundTaskCancellationSource is null)
        {
            return;
        }

        await backgroundTaskCancellationSource.CancelAsync().ConfigureAwait(false);

        if (listenTask is not null)
        {
            try
            {
                await listenTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }

        listener?.Close();
    }

    /// <inheritdoc />
    public async Task<IEnumerable<PeerNode>> DiscoverPeersAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
        var options = discoveryOptionsMonitor.Get(meshId);
        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        var discoveredPeers = new ConcurrentBag<PeerNode>();

        var targetAddress = IPAddress.Parse(options.MulticastAddress);
        var addressFamily = targetAddress.AddressFamily;

        using var client = new UdpClient(addressFamily);
        client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        client.Client.Bind(new IPEndPoint(addressFamily == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Any : IPAddress.Any, 0));

        var discoveryPayload = new UdpDiscoveryMessage
        {
            MeshId = meshId,
            HandshakePort = handshaker.LocalHandshakePort
        };

        var requestBytes = serializer.SerializeToBytes(discoveryPayload);
        var targetEndpoint = new IPEndPoint(targetAddress, options.MulticastPort);

        await client.SendAsync(requestBytes, requestBytes.Length, targetEndpoint).ConfigureAwait(false);
        multicastsSentCounter.Add(1, tags);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(options.DiscoveryTimeout);

        var localNode = new PeerNode(new PeerId(nodeOptions.LocalPeerId), localEndpoint);
        var handshakeTasks = new List<Task>();

        try
        {
            while (!timeoutCts.Token.IsCancellationRequested)
            {
                var result = await client.ReceiveAsync(timeoutCts.Token).ConfigureAwait(false);

                try
                {
                    var pong = serializer.DeserializeFromBytes<UdpDiscoveryMessage>(result.Buffer);

                    if (pong.MeshId == meshId)
                    {
                        var remoteIp = result.RemoteEndPoint.Address;
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
                catch (Exception ex)
                {
                    logger.LogTrace(ex, "[{MeshId}] Error processing UDP multicast response.", meshId);
                }
            }
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

        peersFoundCounter.Add(discoveredPeers.Count, tags);
        return discoveredPeers;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (isDisposed)
        {
            return;
        }

        backgroundTaskCancellationSource?.Cancel();
        backgroundTaskCancellationSource?.Dispose();
        listener?.Dispose();
        meter.Dispose();
        isDisposed = true;
    }

    private async Task ListenLoopAsync(CancellationToken token)
    {
        if (listener is null) return;
        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };

        try
        {
            while (!token.IsCancellationRequested)
            {
                var result = await listener.ReceiveAsync(token).ConfigureAwait(false);

                try
                {
                    var ping = serializer.DeserializeFromBytes<UdpDiscoveryMessage>(result.Buffer);

                    if (ping.MeshId == meshId)
                    {
                        multicastsReceivedCounter.Add(1, tags);

                        var pong = new UdpDiscoveryMessage
                        {
                            MeshId = meshId,
                            HandshakePort = handshaker.LocalHandshakePort
                        };

                        var responseBytes = serializer.SerializeToBytes(pong);
                        await listener.SendAsync(responseBytes, responseBytes.Length, result.RemoteEndPoint).ConfigureAwait(false);

                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                var options = discoveryOptionsMonitor.Get(meshId);
                                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                                timeoutCts.CancelAfter(options.DiscoveryTimeout);

                                var remoteIp = result.RemoteEndPoint.Address;
                                var endpoint = new IPEndPoint(remoteIp, ping.HandshakePort);
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
                            catch (OperationCanceledException) { }
                            catch (Exception ex)
                            {
                                logger.LogTrace(ex, "[{MeshId}] Failed to process active reverse handshake.", meshId);
                            }
                        }, token);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogTrace(ex, "[{MeshId}] Ignored malformed UDP multicast ping.", meshId);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (SocketException ex)
        {
            logger.LogDebug(ex, "[{MeshId}] UDP multicast listener socket exception.", meshId);
        }
    }
}