namespace Ama.Enterprise.P2p.Services.Transports;

using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements isolated outbound transport using robust natively multiplexed QUIC streams mapped against bounded architectures.
/// </summary>
public sealed class QuicTransport : ITransport, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<QuicTransportOptions> optionsMonitor;
    private readonly IMeshWireEncoder wireEncoder;
    private readonly IPeerRegistry peerRegistry;
    private readonly ILogger<QuicTransport> logger;

    private readonly Meter meter;
    private readonly Counter<long> messagesSentCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    private readonly ConcurrentDictionary<QuicPeerEndpoint, SemaphoreSlim> endpointLocks = new();
    private readonly ConcurrentDictionary<QuicPeerEndpoint, QuicConnection> activeConnections = new();

    public QuicTransport(
        string meshId,
        IOptionsMonitor<QuicTransportOptions> optionsMonitor,
        IMeshWireEncoder wireEncoder,
        IPeerRegistry peerRegistry,
        ILogger<QuicTransport> logger,
        IMeterFactory? meterFactory = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(wireEncoder);
        ArgumentNullException.ThrowIfNull(peerRegistry);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.wireEncoder = wireEncoder;
        this.peerRegistry = peerRegistry;
        this.logger = logger;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.QuicTransport") ?? new Meter("Ama.Enterprise.P2p.QuicTransport");
        this.messagesSentCounter = this.meter.CreateCounter<long>(
            "p2p.transport.quic.messages_sent", 
            "messages", 
            "Total messages sent via QUIC transport");
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>(
            "p2p.transport.quic.payload_bytes", 
            "bytes", 
            "Size of outbound QUIC payload in bytes");
    }

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        return endpoint is QuicPeerEndpoint;
    }

    /// <inheritdoc />
    public async Task SendAsync(PeerEndpoint endpoint, IMeshMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(message);

        if (!string.Equals(message.MeshId, meshId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Message MeshId '{message.MeshId}' does not match Transport MeshId '{meshId}'.");
        }

        if (endpoint is not QuicPeerEndpoint quicEndpoint)
        {
            logger.LogWarning("[{MeshId}] Cannot send QUIC message. Target endpoint is not a QuicPeerEndpoint: {Type}", meshId, endpoint.GetType().Name);
            return;
        }

        if (string.IsNullOrWhiteSpace(quicEndpoint.Host))
        {
            throw new ArgumentException("Endpoint host cannot be null or empty.", nameof(endpoint));
        }

        if (quicEndpoint.Port is <= 0 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(endpoint), "Endpoint port must be between 1 and 65535.");
        }

        if (!QuicConnection.IsSupported)
        {
            logger.LogError("[{MeshId}] QUIC is not supported on this platform natively. Operations aborted.", meshId);
            throw new PlatformNotSupportedException("System.Net.Quic is not supported on this operating system.");
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        var endpointLock = endpointLocks.GetOrAdd(quicEndpoint, _ => new SemaphoreSlim(1, 1));
        await endpointLock.WaitAsync(cts.Token).ConfigureAwait(false);

        try
        {
            if (!activeConnections.TryGetValue(quicEndpoint, out var connection))
            {
                var options = optionsMonitor.Get(meshId);
                
                var clientOptions = new QuicClientConnectionOptions
                {
                    RemoteEndPoint = new DnsEndPoint(quicEndpoint.Host, quicEndpoint.Port),
                    DefaultStreamErrorCode = 0x01,
                    DefaultCloseErrorCode = 0x02,
                    MaxInboundUnidirectionalStreams = 100,
                    MaxInboundBidirectionalStreams = 10,
                    ClientAuthenticationOptions = new SslClientAuthenticationOptions
                    {
                        ApplicationProtocols = new List<SslApplicationProtocol> { new SslApplicationProtocol(options.AlpnProtocol) },
                        RemoteCertificateValidationCallback = options.RemoteCertificateValidationCallback
                    }
                };

                connection = await QuicConnection.ConnectAsync(clientOptions, cts.Token).ConfigureAwait(false);
                activeConnections[quicEndpoint] = connection;
            }

            var payload = wireEncoder.Encode(message);
            var lengthBytes = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, payload.Length);

            // QUIC takes advantage of opening a distinct unidirectional stream per payload 
            // completely circumventing TCP head-of-line blocking inherently.
            await using var stream = await connection.OpenOutboundStreamAsync(QuicStreamType.Unidirectional, cts.Token).ConfigureAwait(false);

            await stream.WriteAsync(lengthBytes, cts.Token).ConfigureAwait(false);
            await stream.WriteAsync(payload, cts.Token).ConfigureAwait(false);
            stream.CompleteWrites(); // Signal EOF for this explicit payload stream securely

            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
            messagesSentCounter.Add(1, tags);
            payloadBytesHistogram.Record(payload.Length, tags);

            logger.LogTrace("[{MeshId}] Successfully dispatched {Bytes} bytes via multiplexed QUIC to {Host}:{Port}", meshId, payload.Length, quicEndpoint.Host, quicEndpoint.Port);
        }
        catch (Exception ex) when (ex is QuicException or SocketException or IOException or TimeoutException or OperationCanceledException or ObjectDisposedException)
        {
            if (activeConnections.TryRemove(quicEndpoint, out var brokenConnection))
            {
                await brokenConnection.DisposeAsync().ConfigureAwait(false);
            }

            logger.LogWarning(ex, "[{MeshId}] QUIC transport failure when communicating with {Host}:{Port}. Removing dead peer structurally.", meshId, quicEndpoint.Host, quicEndpoint.Port);
            await RemoveDeadPeerAsync(endpoint, cancellationToken).ConfigureAwait(false);
            throw;
        }
        finally
        {
            endpointLock.Release();
        }
    }

    private async Task RemoveDeadPeerAsync(PeerEndpoint endpoint, CancellationToken cancellationToken)
    {
        var allPeers = await peerRegistry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
        var deadPeer = allPeers.FirstOrDefault(p => p.Endpoint.Equals(endpoint));

        if (deadPeer.Id.Value != Guid.Empty)
        {
            logger.LogInformation("[{MeshId}] Automatically removing unreachable QUIC peer {PeerId}.", meshId, deadPeer.Id);
            await peerRegistry.RemovePeerAsync(meshId, deadPeer.Id, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var connection in activeConnections.Values)
        {
            try { connection.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        }
        activeConnections.Clear();

        foreach (var lockObj in endpointLocks.Values)
        {
            lockObj.Dispose();
        }
        endpointLocks.Clear();

        meter.Dispose();
    }
}