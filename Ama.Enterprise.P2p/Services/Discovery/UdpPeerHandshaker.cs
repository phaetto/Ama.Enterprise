namespace Ama.Enterprise.P2p.Services.Discovery;

using System;
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
/// Implementation of IPeerHandshaker managing isolated unicast UDP probes explicitly decoupled structurally.
/// </summary>
public sealed class UdpPeerHandshaker : IPeerHandshaker, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<UdpHandshakeOptions> optionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly PeerEndpoint localEndpoint;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<UdpPeerHandshaker> logger;
    private readonly IPeerAuthenticator authenticator;
    private readonly IPeerRegistry peerRegistry;
    private readonly IFailureDetector failureDetector;

    private UdpClient? listener;
    private CancellationTokenSource? backgroundTaskCancellationSource;
    private Task? listenTask;
    private bool isDisposed;

    private readonly Meter meter;
    private readonly Counter<long> requestsSentCounter;
    private readonly Counter<long> requestsReceivedCounter;

    /// <summary>
    /// Initializes a new instance of the <see cref="UdpPeerHandshaker"/> class.
    /// </summary>
    public UdpPeerHandshaker(
        string meshId,
        IOptionsMonitor<UdpHandshakeOptions> optionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        PeerEndpoint localEndpoint,
        ICrdtSerializer serializer,
        ILogger<UdpPeerHandshaker> logger,
        IPeerAuthenticator authenticator,
        IPeerRegistry peerRegistry,
        IFailureDetector failureDetector,
        IMeterFactory? meterFactory = null)
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

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.UdpPeerHandshaker") ?? new Meter("Ama.Enterprise.P2p.UdpPeerHandshaker");
        this.requestsSentCounter = this.meter.CreateCounter<long>("p2p.handshaker.udp.requests_sent", "requests", "Total UDP handshake requests sent");
        this.requestsReceivedCounter = this.meter.CreateCounter<long>("p2p.handshaker.udp.requests_received", "requests", "Total UDP handshake requests received");
    }

    /// <inheritdoc />
    public int LocalHandshakePort => optionsMonitor.Get(meshId).ListenPort;

    /// <inheritdoc />
    public Task StartListeningAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        var options = optionsMonitor.Get(meshId);

        listener = new UdpClient(AddressFamily.InterNetworkV6);
        listener.Client.DualMode = true;
        listener.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        listener.Client.Bind(new IPEndPoint(IPAddress.IPv6Any, options.ListenPort));

        backgroundTaskCancellationSource = new CancellationTokenSource();
        listenTask = ListenLoopAsync(backgroundTaskCancellationSource.Token);

        logger.LogInformation("[{MeshId}] UDP Peer Handshaker started natively listening on port {Port}", meshId, options.ListenPort);
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
    public async Task<PeerHandshakePayload?> HandshakeAsync(PeerHandshakePayload localPayload, IPEndPoint endpoint, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        ArgumentNullException.ThrowIfNull(endpoint);

        var options = optionsMonitor.Get(meshId);
        var addressFamily = endpoint.AddressFamily;
        
        using var client = new UdpClient(addressFamily);
        client.Client.Bind(new IPEndPoint(addressFamily == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Any : IPAddress.Any, 0));

        var requestBytes = serializer.SerializeToBytes(localPayload);
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(options.HandshakeTimeout);

        try
        {
            await client.SendAsync(requestBytes, requestBytes.Length, endpoint).ConfigureAwait(false);
            requestsSentCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", meshId));

            var result = await client.ReceiveAsync(timeoutCts.Token).ConfigureAwait(false);

            return serializer.DeserializeFromBytes<PeerHandshakePayload>(result.Buffer);
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
                    requestsReceivedCounter.Add(1, tags);
                    var remotePayload = serializer.DeserializeFromBytes<PeerHandshakePayload>(result.Buffer);

                    if (remotePayload.Node.Id.Value != nodeOptions.LocalPeerId && remotePayload.Node.Id.Value != Guid.Empty)
                    {
                        var localNode = new PeerNode(new PeerId(nodeOptions.LocalPeerId), localEndpoint);
                        var localHandshakeData = await authenticator.GetLocalHandshakeDataAsync(token).ConfigureAwait(false);
                        var responsePayload = new PeerHandshakePayload
                        {
                            Node = localNode,
                            HandshakeData = localHandshakeData.ToArray()
                        };
                        
                        var responseBytes = serializer.SerializeToBytes(responsePayload);
                        await listener.SendAsync(responseBytes, responseBytes.Length, result.RemoteEndPoint).ConfigureAwait(false);

                        try
                        {
                            var isAuthenticated = await authenticator.AuthenticateAsync(remotePayload.Node, remotePayload.HandshakeData, token).ConfigureAwait(false);

                            if (isAuthenticated)
                            {
                                await failureDetector.RecordHeartbeatAsync(remotePayload.Node.Id, token).ConfigureAwait(false);
                                await peerRegistry.AddOrUpdatePeerAsync(meshId, remotePayload.Node, PeerStatus.Active, token).ConfigureAwait(false);
                            }
                        }
                        catch (Exception authEx)
                        {
                            logger.LogWarning(authEx, "[{MeshId}] Inbound UDP handshake authentication failed for {PeerId}.", meshId, remotePayload.Node.Id.Value);
                        }
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