namespace Ama.Enterprise.P2p.Services.Discovery;

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation of IPeerDiscovery using UDP multicast for local network discovery bound to a specific mesh.
/// </summary>
public sealed class UdpPeerDiscovery : IPeerDiscovery, IHostedService, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<UdpDiscoveryOptions> discoveryOptionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly PeerEndpoint localEndpoint;
    private readonly ILogger<UdpPeerDiscovery> logger;
    private readonly IPeerRegistry peerRegistry;
    private readonly ICrdtSerializer serializer;
    
    private UdpClient? listener;
    private CancellationTokenSource? backgroundTaskCancellationSource;
    private Task? listenTask;
    private Task? discoveryTask;
    private bool isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="UdpPeerDiscovery"/> class.
    /// </summary>
    /// <param name="meshId">The mesh context identifier.</param>
    /// <param name="discoveryOptionsMonitor">The UDP discovery configuration options monitor.</param>
    /// <param name="nodeOptionsMonitor">The global node configuration options monitor.</param>
    /// <param name="localEndpoint">The local network endpoint to advertise.</param>
    /// <param name="logger">The logger instance.</param>
    /// <param name="peerRegistry">The peer registry to populate with discovered nodes.</param>
    /// <param name="serializer">The centralized CRDT serializer.</param>
    /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
    public UdpPeerDiscovery(
        string meshId,
        IOptionsMonitor<UdpDiscoveryOptions> discoveryOptionsMonitor, 
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor, 
        PeerEndpoint localEndpoint,
        ILogger<UdpPeerDiscovery> logger,
        IPeerRegistry peerRegistry,
        ICrdtSerializer serializer)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.discoveryOptionsMonitor = discoveryOptionsMonitor ?? throw new ArgumentNullException(nameof(discoveryOptionsMonitor));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.localEndpoint = localEndpoint ?? throw new ArgumentNullException(nameof(localEndpoint));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.peerRegistry = peerRegistry ?? throw new ArgumentNullException(nameof(peerRegistry));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    }

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
        var discoveredPeers = new HashSet<PeerNode>();
        
        using var client = new UdpClient(AddressFamily.InterNetwork);
        client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        client.Client.Bind(new IPEndPoint(IPAddress.Any, 0)); 
        
        var localId = new PeerId(nodeOptions.LocalPeerId);
        var localNode = new PeerNode(localId, localEndpoint);
        
        var requestBytes = serializer.SerializeToBytes(localNode);
        var targetEndpoint = new IPEndPoint(IPAddress.Parse(options.MulticastAddress), options.MulticastPort);
        
        await client.SendAsync(requestBytes, requestBytes.Length, targetEndpoint).ConfigureAwait(false);
        
        using var timeoutCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCancellationSource.CancelAfter(options.DiscoveryTimeout);
        
        try
        {
            while (!timeoutCancellationSource.Token.IsCancellationRequested)
            {
                var result = await client.ReceiveAsync(timeoutCancellationSource.Token).ConfigureAwait(false);
                var payload = result.Buffer;

                try
                {
                    var node = serializer.DeserializeFromBytes<PeerNode>(payload);

                    if (node.Id.Value != nodeOptions.LocalPeerId && node.Id.Value != Guid.Empty)
                    {
                        discoveredPeers.Add(node);
                    }
                }
                catch
                {
                    // Ignore parsing errors from alien network packets
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] An error occurred while receiving UDP discovery responses.", meshId);
        }
        
        return discoveredPeers;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (isDisposed) return;
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
                var nodeOptions = nodeOptionsMonitor.Get(meshId);
                
                try
                {
                    var remoteNode = serializer.DeserializeFromBytes<PeerNode>(result.Buffer);

                    if (remoteNode.Id.Value != nodeOptions.LocalPeerId && remoteNode.Id.Value != Guid.Empty)
                    {
                        await peerRegistry.AddOrUpdatePeerAsync(remoteNode, PeerStatus.Active, token).ConfigureAwait(false);

                        var localNode = new PeerNode(new PeerId(nodeOptions.LocalPeerId), localEndpoint);
                        var responseBytes = serializer.SerializeToBytes(localNode);
                        
                        await listener.SendAsync(responseBytes, responseBytes.Length, result.RemoteEndPoint).ConfigureAwait(false);
                    }
                }
                catch { }
            }
        }
        catch (OperationCanceledException) { }
        catch (SocketException ex)
        {
            logger.LogDebug(ex, "[{MeshId}] UDP listener socket exception during shutdown or network configuration change.", meshId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Unexpected error in UDP peer discovery background listen loop.", meshId);
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
                    await peerRegistry.AddOrUpdatePeerAsync(peer, PeerStatus.Active, token).ConfigureAwait(false);
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