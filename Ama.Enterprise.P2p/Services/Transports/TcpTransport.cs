namespace Ama.Enterprise.P2p.Services.Transports;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implements isolated outbound transport using robust TCP streams explicitly mapped against bounded architectures.
/// </summary>
public sealed class TcpTransport : ITransport, IDisposable
{
    private readonly string meshId;
    private readonly ICrdtSerializer serializer;
    private readonly IPeerRegistry peerRegistry;
    private readonly ILogger<TcpTransport> logger;

    private readonly Meter meter;
    private readonly Counter<long> messagesSentCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    public TcpTransport(
        string meshId,
        ICrdtSerializer serializer,
        IPeerRegistry peerRegistry,
        ILogger<TcpTransport> logger,
        IMeterFactory? meterFactory = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(peerRegistry);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.serializer = serializer;
        this.peerRegistry = peerRegistry;
        this.logger = logger;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.TcpTransport") ?? new Meter("Ama.Enterprise.P2p.TcpTransport");
        this.messagesSentCounter = this.meter.CreateCounter<long>(
            "p2p.transport.tcp.messages_sent", 
            "messages", 
            "Total messages sent via TCP transport");
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>(
            "p2p.transport.tcp.payload_bytes", 
            "bytes", 
            "Size of outbound TCP payload in bytes");
    }

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        return endpoint is TcpPeerEndpoint;
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

        if (endpoint is not TcpPeerEndpoint tcpEndpoint)
        {
            logger.LogWarning("[{MeshId}] Cannot send TCP message. Target endpoint is not a TcpPeerEndpoint: {Type}", meshId, endpoint.GetType().Name);
            return;
        }

        if (string.IsNullOrWhiteSpace(tcpEndpoint.Host))
        {
            throw new ArgumentException("Endpoint host cannot be null or empty.", nameof(endpoint));
        }

        if (tcpEndpoint.Port is <= 0 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(endpoint), "Endpoint port must be between 1 and 65535.");
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(5));

        using var client = new TcpClient();

        try
        {
            await client.ConnectAsync(tcpEndpoint.Host, tcpEndpoint.Port, cts.Token).ConfigureAwait(false);
            await using var stream = client.GetStream();

            var payload = serializer.SerializeToBytes(message);
            var lengthBytes = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, payload.Length);

            await stream.WriteAsync(lengthBytes, cts.Token).ConfigureAwait(false);
            await stream.WriteAsync(payload, cts.Token).ConfigureAwait(false);

            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
            messagesSentCounter.Add(1, tags);
            payloadBytesHistogram.Record(payload.Length, tags);

            logger.LogTrace("[{MeshId}] Successfully dispatched {Bytes} bytes via TCP to {Host}:{Port}", meshId, payload.Length, tcpEndpoint.Host, tcpEndpoint.Port);
        }
        catch (Exception ex) when (ex is SocketException or IOException or TimeoutException or OperationCanceledException)
        {
            logger.LogWarning(ex, "[{MeshId}] TCP transport failure when communicating with {Host}:{Port}. Removing dead peer structurally.", meshId, tcpEndpoint.Host, tcpEndpoint.Port);
            await RemoveDeadPeerAsync(endpoint, cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    private async Task RemoveDeadPeerAsync(PeerEndpoint endpoint, CancellationToken cancellationToken)
    {
        var allPeers = await peerRegistry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
        var deadPeer = allPeers.FirstOrDefault(p => p.Endpoint.Equals(endpoint));

        if (deadPeer.Id.Value != Guid.Empty)
        {
            logger.LogInformation("[{MeshId}] Automatically removing unreachable TCP peer {PeerId}.", meshId, deadPeer.Id);
            await peerRegistry.RemovePeerAsync(meshId, deadPeer.Id, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        meter.Dispose();
    }
}