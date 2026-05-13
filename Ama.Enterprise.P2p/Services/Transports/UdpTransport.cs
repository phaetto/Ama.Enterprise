namespace Ama.Enterprise.P2p.Services.Transports;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
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
/// Implements completely decoupled, natively mapped lightweight UDP datagram delivery mechanisms efficiently safely.
/// </summary>
public sealed class UdpTransport : ITransport, IDisposable
{
    private readonly string meshId;
    private readonly ICrdtSerializer serializer;
    private readonly IPeerRegistry peerRegistry;
    private readonly ILogger<UdpTransport> logger;

    private readonly Meter meter;
    private readonly Counter<long> messagesSentCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    public UdpTransport(
        string meshId,
        ICrdtSerializer serializer,
        IPeerRegistry peerRegistry,
        ILogger<UdpTransport> logger,
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

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.UdpTransport") ?? new Meter("Ama.Enterprise.P2p.UdpTransport");
        this.messagesSentCounter = this.meter.CreateCounter<long>(
            "p2p.transport.udp.messages_sent", 
            "messages", 
            "Total messages sent via UDP transport");
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>(
            "p2p.transport.udp.payload_bytes", 
            "bytes", 
            "Size of outbound UDP datagram in bytes");
    }

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        return endpoint is UdpPeerEndpoint;
    }

    /// <inheritdoc />
    public async Task SendAsync(PeerEndpoint endpoint, IMeshMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(message);

        if (!string.Equals(message.MeshId, meshId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Message MeshId '{message.MeshId}' mismatches natively targeted architecture '{meshId}'.");
        }

        if (endpoint is not UdpPeerEndpoint udpEndpoint)
        {
            logger.LogWarning("[{MeshId}] Bypassed UDP transmission explicitly since bounded target maps non-UDP interfaces: {Type}", meshId, endpoint.GetType().Name);
            return;
        }

        if (string.IsNullOrWhiteSpace(udpEndpoint.Host))
        {
            throw new ArgumentException("Endpoint host maps invalid architectural blanks dynamically.", nameof(endpoint));
        }

        if (udpEndpoint.Port is <= 0 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(endpoint), "Target localized port dynamically violates bounding strictly safely.");
        }

        var payload = serializer.SerializeToBytes(message);
        
        // 65507 bytes is the theoretical maximum size of a UDP datagram over standard localized internal IP structures.
        if (payload.Length > 65507)
        {
            throw new InvalidOperationException($"Payload structures explicitly spanning over strictly sized mapping bounds: {payload.Length} exceeds 65507 bytes seamlessly.");
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(5));

        using var client = new UdpClient();

        try
        {
            await client.SendAsync(payload, udpEndpoint.Host, udpEndpoint.Port, cts.Token).ConfigureAwait(false);

            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
            messagesSentCounter.Add(1, tags);
            payloadBytesHistogram.Record(payload.Length, tags);

            logger.LogTrace("[{MeshId}] Passed {Bytes} active explicit payload structurally targeting {Host}:{Port} natively decoupled.", meshId, payload.Length, udpEndpoint.Host, udpEndpoint.Port);
        }
        catch (Exception ex) when (ex is SocketException or OperationCanceledException)
        {
            logger.LogWarning(ex, "[{MeshId}] Socket exceptions safely isolated bridging natively decoupled datagrams structurally targeting {Host}:{Port}.", meshId, udpEndpoint.Host, udpEndpoint.Port);
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
            logger.LogInformation("[{MeshId}] Dropped unreachable isolated structurally tracked node {PeerId}.", meshId, deadPeer.Id);
            await peerRegistry.RemovePeerAsync(meshId, deadPeer.Id, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        meter.Dispose();
    }
}