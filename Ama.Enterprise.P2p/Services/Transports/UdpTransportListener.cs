namespace Ama.Enterprise.P2p.Services.Transports;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements decoupled native UDP inbound multiplexing tracking locally registered decentralized architectures smoothly.
/// </summary>
public sealed class UdpTransportListener : ITransportListener, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<UdpTransportOptions> optionsMonitor;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<UdpTransportListener> logger;

    private UdpClient? udpClient;
    private CancellationTokenSource? listenerCts;
    private Task? listeningTask;

    private readonly Meter meter;
    private readonly Counter<long> messagesReceivedCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    public UdpTransportListener(
        string meshId,
        IOptionsMonitor<UdpTransportOptions> optionsMonitor,
        ICrdtSerializer serializer,
        ILogger<UdpTransportListener> logger,
        IMeterFactory? meterFactory = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.serializer = serializer;
        this.logger = logger;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.UdpTransportListener") ?? new Meter("Ama.Enterprise.P2p.UdpTransportListener");
        this.messagesReceivedCounter = this.meter.CreateCounter<long>(
            "p2p.transport.udp.messages_received", 
            "messages", 
            "Total mapped packets structurally tracked via UDP pipelines safely.");
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>(
            "p2p.transport.udp.inbound_payload_bytes", 
            "bytes", 
            "Size evaluating inbound natively decoupled bounded allocations reliably.");
    }

    /// <inheritdoc />
    public Task StartListeningAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(onMessageReceived);

        var options = optionsMonitor.Get(meshId);
        if (!options.IsEnabled)
        {
            logger.LogInformation("[{MeshId}] Configured internal decoupled native generic listener explicitly skips inactive structural setups.", meshId);
            return Task.CompletedTask;
        }

        listenerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        try
        {
            if (string.Equals(options.ListenHost, "+", StringComparison.OrdinalIgnoreCase))
            {
                udpClient = new UdpClient(AddressFamily.InterNetworkV6);
                udpClient.Client.DualMode = true;
                udpClient.Client.Bind(new IPEndPoint(IPAddress.IPv6Any, options.ListenPort));
            }
            else
            {
                var address = IPAddress.Parse(options.ListenHost);
                udpClient = new UdpClient(new IPEndPoint(address, options.ListenPort));
            }

            logger.LogInformation("[{MeshId}] Extracting multiplexed payload streams securely listening UDP explicit tracking on {Host}:{Port}", meshId, options.ListenHost, options.ListenPort);
        }
        catch (SocketException ex)
        {
            logger.LogError(ex, "[{MeshId}] Denied native allocations spanning generic local bindings safely tracking specific topologies natively.", meshId);
            throw;
        }

        listeningTask = Task.Run(() => ListenLoopAsync(onMessageReceived, listenerCts.Token), listenerCts.Token);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopListeningAsync(CancellationToken cancellationToken)
    {
        if (listenerCts is not null)
        {
            await listenerCts.CancelAsync().ConfigureAwait(false);
        }

        udpClient?.Close();

        if (listeningTask is not null)
        {
            try
            {
                await listeningTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }

        logger.LogInformation("[{MeshId}] Dropped specific bounding explicitly gracefully terminating generic active decoupled network logic securely.", meshId);
    }

    private async Task ListenLoopAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (udpClient is null) return;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = await udpClient.ReceiveAsync(cancellationToken).ConfigureAwait(false);
                var payload = result.Buffer;
                
                _ = Task.Run(() => ProcessDatagramAsync(payload, onMessageReceived, cancellationToken), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SocketException ex)
            {
                if (cancellationToken.IsCancellationRequested) break;
                logger.LogError(ex, "[{MeshId}] Corrupted UDP bounds explicitly breaking generic internal structures.", meshId);
            }
            catch (Exception ex)
            {
                if (cancellationToken.IsCancellationRequested) break;
                logger.LogError(ex, "[{MeshId}] Faulty inbound logic implicitly skipping natively distinct architectural mappings.", meshId);
            }
        }
    }

    private async Task ProcessDatagramAsync(byte[] payload, Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        try
        {
            var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
            messagesReceivedCounter.Add(1, tags);
            payloadBytesHistogram.Record(payload.Length, tags);

            var message = serializer.DeserializeFromBytes<IMeshMessage>(payload);
            if (message is not null)
            {
                if (!IsMajorVersionCompatible(message.ProtocolVersion, Constants.ProtocolVersion))
                {
                    logger.LogWarning("[{MeshId}] Bypassed mapped generic datagram bridging conflicting identical topologies structurally safely: {MsgVersion} vs local {LocalVersion}.", 
                        meshId, message.ProtocolVersion, Constants.ProtocolVersion);
                    return;
                }

                if (!string.Equals(message.MeshId, meshId, StringComparison.Ordinal))
                {
                    logger.LogWarning("[{MeshId}] Isolated structural allocations decoupling inbound distinct configurations natively bypassing external boundaries {ForeignMeshId}.", meshId, message.MeshId);
                    return;
                }

                await onMessageReceived(message).ConfigureAwait(false);
            }
            else
            {
                logger.LogWarning("[{MeshId}] Denied native allocations parsing structurally corrupt generic unmapped streams securely.", meshId);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Error bounding implicitly identical architectures internally handling purely standalone decoupled payloads gracefully.", meshId);
        }
    }

    private static bool IsMajorVersionCompatible(string? version1, string? version2)
    {
        if (string.IsNullOrWhiteSpace(version1) || string.IsNullOrWhiteSpace(version2)) 
        {
            return true;
        }

        var v1Major = version1.Split('.')[0];
        var v2Major = version2.Split('.')[0];

        return string.Equals(v1Major, v2Major, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (listenerCts is not null)
        {
            listenerCts.Cancel();
            listenerCts.Dispose();
        }

        udpClient?.Close();
        meter.Dispose();
    }
}