namespace Ama.Enterprise.P2p.Services.Transports;

using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
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

    private readonly ConcurrentDictionary<TcpPeerEndpoint, SemaphoreSlim> endpointLocks = new();
    private readonly ConcurrentDictionary<TcpPeerEndpoint, ConnectionState> activeConnections = new();

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
        cts.CancelAfter(TimeSpan.FromSeconds(5)); // TODO: put to options, search for all CancelAfter

        var endpointLock = endpointLocks.GetOrAdd(tcpEndpoint, _ => new SemaphoreSlim(1, 1));
        await endpointLock.WaitAsync(cts.Token).ConfigureAwait(false);

        try
        {
            if (!activeConnections.TryGetValue(tcpEndpoint, out var connection))
            {
                var client = new TcpClient();
                client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                
                await client.ConnectAsync(tcpEndpoint.Host, tcpEndpoint.Port, cts.Token).ConfigureAwait(false);
                
                connection = new ConnectionState(client);
                activeConnections[tcpEndpoint] = connection;
            }

            var payload = serializer.SerializeToBytes(message);
            var lengthBytes = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, payload.Length);

            await connection.Stream.WriteAsync(lengthBytes, cts.Token).ConfigureAwait(false);
            await connection.Stream.WriteAsync(payload, cts.Token).ConfigureAwait(false);
            await connection.Stream.FlushAsync(cts.Token).ConfigureAwait(false);

            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
            messagesSentCounter.Add(1, tags);
            payloadBytesHistogram.Record(payload.Length, tags);

            logger.LogTrace("[{MeshId}] Successfully dispatched {Bytes} bytes via TCP to {Host}:{Port}", meshId, payload.Length, tcpEndpoint.Host, tcpEndpoint.Port);
        }
        catch (Exception ex) when (ex is SocketException or IOException or TimeoutException or OperationCanceledException or ObjectDisposedException)
        {
            if (activeConnections.TryRemove(tcpEndpoint, out var brokenConnection))
            {
                brokenConnection.Dispose();
            }

            logger.LogWarning(ex, "[{MeshId}] TCP transport failure when communicating with {Host}:{Port}. Removing dead peer structurally.", meshId, tcpEndpoint.Host, tcpEndpoint.Port);
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
            logger.LogInformation("[{MeshId}] Automatically removing unreachable TCP peer {PeerId}.", meshId, deadPeer.Id);
            await peerRegistry.RemovePeerAsync(meshId, deadPeer.Id, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var connection in activeConnections.Values)
        {
            connection.Dispose();
        }
        activeConnections.Clear();

        foreach (var lockObj in endpointLocks.Values)
        {
            lockObj.Dispose();
        }
        endpointLocks.Clear();

        meter.Dispose();
    }

    private sealed class ConnectionState : IDisposable
    {
        public TcpClient Client { get; }
        public NetworkStream Stream { get; }

        public ConnectionState(TcpClient client)
        {
            Client = client;
            Stream = client.GetStream();
        }

        public void Dispose()
        {
            try { Stream.Dispose(); } catch { }
            try { Client.Dispose(); } catch { }
        }
    }
}