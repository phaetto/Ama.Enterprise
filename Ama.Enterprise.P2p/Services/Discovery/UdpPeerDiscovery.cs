namespace Ama.Enterprise.P2p.Services.Discovery;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Discovery;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation of IPeerDiscovery acting as Phase 1, using UDP multicast to resolve IPs and delegating negotiation to the handshaker.
/// </summary>
public sealed class UdpPeerDiscovery(
    string meshId,
    IOptionsMonitor<UdpDiscoveryOptions> discoveryOptionsMonitor,
    IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
    PeerEndpoint localEndpoint,
    IPeerHandshaker handshaker,
    ILogger<UdpPeerDiscovery> logger,
    IPeerRegistry peerRegistry,
    ICrdtSerializer serializer,
    IPeerAuthenticator authenticator,
    IFailureDetector failureDetector) : IPeerDiscovery, IHostedService, IDisposable
{
    private readonly string meshId = meshId;
    private readonly IOptionsMonitor<UdpDiscoveryOptions> discoveryOptionsMonitor = discoveryOptionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor = nodeOptionsMonitor;
    private readonly PeerEndpoint localEndpoint = localEndpoint;
    private readonly IPeerHandshaker handshaker = handshaker;
    private readonly ILogger<UdpPeerDiscovery> logger = logger;
    private readonly IPeerRegistry peerRegistry = peerRegistry;
    private readonly ICrdtSerializer serializer = serializer;
    private readonly IPeerAuthenticator authenticator = authenticator;
    private readonly IFailureDetector failureDetector = failureDetector;

    private UdpClient? listener;
    private CancellationTokenSource? backgroundTaskCancellationSource;
    private Task? listenTask;
    private Task? discoveryTask;
    private bool isDisposed;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        var options = discoveryOptionsMonitor.Get(meshId);
        backgroundTaskCancellationSource = new CancellationTokenSource();

        var localIpEndpoint = new IPEndPoint(IPAddress.Any, options.MulticastPort);
        listener = new UdpClient(AddressFamily.InterNetwork);

        listener.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        listener.Client.Bind(localIpEndpoint);

        var multicastAddress = IPAddress.Parse(options.MulticastAddress);
        listener.JoinMulticastGroup(multicastAddress);

        listenTask = ListenLoopAsync(backgroundTaskCancellationSource.Token);
        discoveryTask = DiscoveryLoopAsync(backgroundTaskCancellationSource.Token);

        logger.LogInformation(
            "[{MeshId}] UDP Peer Discovery started listening on multicast group {Address}:{Port}",
            meshId,
            options.MulticastAddress,
            options.MulticastPort);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (backgroundTaskCancellationSource is null)
        {
            return;
        }

        await backgroundTaskCancellationSource.CancelAsync().ConfigureAwait(false);

        var tasksToWait = new List<Task>();
        if (listenTask is not null) tasksToWait.Add(listenTask);
        if (discoveryTask is not null) tasksToWait.Add(discoveryTask);

        if (tasksToWait.Count > 0)
        {
            try
            {
                await Task.WhenAll(tasksToWait).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }

        listener?.Close();
    }

    /// <inheritdoc />
    public async Task<IEnumerable<PeerNode>> DiscoverPeersAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        var options = discoveryOptionsMonitor.Get(meshId);
        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        var discoveredPeers = new ConcurrentBag<PeerNode>();

        using var client = new UdpClient(AddressFamily.InterNetwork);
        client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        client.Client.Bind(new IPEndPoint(IPAddress.Any, 0));

        var discoveryPayload = new UdpDiscoveryMessage
        {
            MeshId = meshId,
            HandshakePort = handshaker.LocalHandshakePort
        };

        var requestBytes = serializer.SerializeToBytes(discoveryPayload);
        var targetEndpoint = new IPEndPoint(IPAddress.Parse(options.MulticastAddress), options.MulticastPort);

        await client.SendAsync(requestBytes, requestBytes.Length, targetEndpoint).ConfigureAwait(false);

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
        isDisposed = true;
    }

    private async Task ListenLoopAsync(CancellationToken token)
    {
        if (listener is null) return;

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
                        var pong = new UdpDiscoveryMessage
                        {
                            MeshId = meshId,
                            HandshakePort = handshaker.LocalHandshakePort
                        };

                        var responseBytes = serializer.SerializeToBytes(pong);
                        await listener.SendAsync(responseBytes, responseBytes.Length, result.RemoteEndPoint).ConfigureAwait(false);
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

    private async Task DiscoveryLoopAsync(CancellationToken token)
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(2), token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

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
                logger.LogError(ex, "[{MeshId}] Unexpected error in UDP peer discovery loop.", meshId);
            }

            if (token.IsCancellationRequested) break;

            try
            {
                await Task.Delay(options.DiscoveryInterval, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }
    }
}