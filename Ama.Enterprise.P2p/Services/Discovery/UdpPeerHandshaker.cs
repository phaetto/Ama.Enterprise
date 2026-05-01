namespace Ama.Enterprise.P2p.Services.Discovery;

using System;
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
/// Implementation of IPeerHandshaker managing isolated unicast UDP probes.
/// </summary>
public sealed class UdpPeerHandshaker : IPeerHandshaker, IHostedService, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<UdpHandshakeOptions> optionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly PeerEndpoint localEndpoint;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<UdpPeerHandshaker> logger;

    private UdpClient? listener;
    private CancellationTokenSource? backgroundTaskCancellationSource;
    private Task? listenTask;
    private bool isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="UdpPeerHandshaker"/> class.
    /// </summary>
    public UdpPeerHandshaker(
        string meshId,
        IOptionsMonitor<UdpHandshakeOptions> optionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        PeerEndpoint localEndpoint,
        ICrdtSerializer serializer,
        ILogger<UdpPeerHandshaker> logger)
    {
        ArgumentNullException.ThrowIfNull(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(nodeOptionsMonitor);
        ArgumentNullException.ThrowIfNull(localEndpoint);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.nodeOptionsMonitor = nodeOptionsMonitor;
        this.localEndpoint = localEndpoint;
        this.serializer = serializer;
        this.logger = logger;
    }

    /// <inheritdoc />
    public int LocalHandshakePort => optionsMonitor.Get(meshId).ListenPort;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        var options = optionsMonitor.Get(meshId);

        listener = new UdpClient(AddressFamily.InterNetwork);
        listener.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        listener.Client.Bind(new IPEndPoint(IPAddress.Any, options.ListenPort));

        backgroundTaskCancellationSource = new CancellationTokenSource();
        listenTask = ListenLoopAsync(backgroundTaskCancellationSource.Token);

        logger.LogInformation("[{MeshId}] UDP Peer Handshaker started listening on port {Port}", meshId, options.ListenPort);
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
    public async Task<PeerNode?> HandshakeAsync(PeerNode localNode, IPEndPoint endpoint, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        ArgumentNullException.ThrowIfNull(endpoint);

        var options = optionsMonitor.Get(meshId);
        using var client = new UdpClient(AddressFamily.InterNetwork);
        client.Client.Bind(new IPEndPoint(IPAddress.Any, 0));

        var requestBytes = serializer.SerializeToBytes(localNode);
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(options.HandshakeTimeout);

        try
        {
            await client.SendAsync(requestBytes, requestBytes.Length, endpoint).ConfigureAwait(false);
            var result = await client.ReceiveAsync(timeoutCts.Token).ConfigureAwait(false);

            return serializer.DeserializeFromBytes<PeerNode>(result.Buffer);
        }
        catch (OperationCanceledException)
        {
            logger.LogTrace("[{MeshId}] UDP handshake timed out for {Target}.", meshId, endpoint);
            return null;
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "[{MeshId}] UDP handshake failed for {Target}.", meshId, endpoint);
            return null;
        }
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
        var nodeOptions = nodeOptionsMonitor.Get(meshId);

        try
        {
            while (!token.IsCancellationRequested)
            {
                var result = await listener.ReceiveAsync(token).ConfigureAwait(false);

                try
                {
                    var remoteNode = serializer.DeserializeFromBytes<PeerNode>(result.Buffer);

                    if (remoteNode.Id.Value != nodeOptions.LocalPeerId && remoteNode.Id.Value != Guid.Empty)
                    {
                        var localNode = new PeerNode(new PeerId(nodeOptions.LocalPeerId), localEndpoint);
                        var responseBytes = serializer.SerializeToBytes(localNode);

                        await listener.SendAsync(responseBytes, responseBytes.Length, result.RemoteEndPoint).ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogTrace(ex, "[{MeshId}] Failed to process inbound UDP handshake request.", meshId);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (SocketException ex)
        {
            logger.LogDebug(ex, "[{MeshId}] UDP handshaker listener socket exception.", meshId);
        }
    }
}