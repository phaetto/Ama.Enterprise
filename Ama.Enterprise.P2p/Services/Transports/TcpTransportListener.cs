namespace Ama.Enterprise.P2p.Services.Transports;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements inbound network listener extracting localized pure TCP streams mapping native robust pipelines.
/// </summary>
public sealed class TcpTransportListener : ITransportListener, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<TcpTransportOptions> optionsMonitor;
    private readonly IMeshWireEncoder wireEncoder;
    private readonly ILogger<TcpTransportListener> logger;

    private TcpListener? tcpListener;
    private CancellationTokenSource? listenerCts;
    private Task? listeningTask;

    private readonly Meter meter;
    private readonly Counter<long> messagesReceivedCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    public TcpTransportListener(
        string meshId,
        IOptionsMonitor<TcpTransportOptions> optionsMonitor,
        IMeshWireEncoder wireEncoder,
        ILogger<TcpTransportListener> logger,
        IMeterFactory? meterFactory = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(wireEncoder);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.wireEncoder = wireEncoder;
        this.logger = logger;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.TcpTransportListener") ?? new Meter("Ama.Enterprise.P2p.TcpTransportListener");
        this.messagesReceivedCounter = this.meter.CreateCounter<long>(
            "p2p.transport.tcp.messages_received", 
            "messages", 
            "Total messages received via TCP transport");
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>(
            "p2p.transport.tcp.inbound_payload_bytes", 
            "bytes", 
            "Size of inbound TCP payload in bytes");
    }

    /// <inheritdoc />
    public Task StartListeningAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(onMessageReceived);

        var options = optionsMonitor.Get(meshId);
        if (!options.IsEnabled)
        {
            logger.LogInformation("[{MeshId}] TCP Transport Listener is disabled explicitly.", meshId);
            return Task.CompletedTask;
        }

        listenerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        try
        {
            if (string.Equals(options.ListenHost, "+", StringComparison.OrdinalIgnoreCase))
            {
                tcpListener = TcpListener.Create(options.ListenPort);
            }
            else
            {
                var address = IPAddress.Parse(options.ListenHost);
                tcpListener = new TcpListener(address, options.ListenPort);
            }

            tcpListener.Start();
            logger.LogInformation("[{MeshId}] Listening globally for TCP P2P traffic on {Host}:{Port}", meshId, options.ListenHost, options.ListenPort);
        }
        catch (SocketException ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to bind internal TCP listener natively. Validate explicitly available ports properly.", meshId);
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

        tcpListener?.Stop();

        if (listeningTask is not null)
        {
            try
            {
                await listeningTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }

        logger.LogInformation("[{MeshId}] TCP transport listener stopped safely.", meshId);
    }

    private async Task ListenLoopAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (tcpListener is null) return;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var client = await tcpListener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
                _ = Task.Run(() => ProcessClientAsync(client, onMessageReceived, cancellationToken), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SocketException ex)
            {
                if (cancellationToken.IsCancellationRequested) break;
                logger.LogError(ex, "[{MeshId}] TCP listener error encountered mapping incoming pipeline safely.", meshId);
            }
            catch (Exception ex)
            {
                if (cancellationToken.IsCancellationRequested) break;
                logger.LogError(ex, "[{MeshId}] Fatal error bridging incoming TCP payloads explicitly.", meshId);
            }
        }
    }

    private async Task ProcessClientAsync(TcpClient client, Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        try
        {
            using (client)
            {
                client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                await using var stream = client.GetStream();
                
                var lengthBytes = new byte[4];

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        await stream.ReadExactlyAsync(lengthBytes, cancellationToken).ConfigureAwait(false);
                    }
                    catch (EndOfStreamException)
                    {
                        logger.LogDebug("[{MeshId}] Target node closed TCP socket cleanly.", meshId);
                        break;
                    }
                    catch (IOException ex) when (ex.InnerException is SocketException)
                    {
                        logger.LogDebug("[{MeshId}] Target node connection dropped.", meshId);
                        break;
                    }
                    
                    int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
                    var options = optionsMonitor.Get(meshId);
                    
                    if (payloadLength <= 0 || payloadLength > options.MaxMessageSize)
                    {
                        logger.LogWarning("[{MeshId}] Rejecting invalidly mapped TCP payload structured over bounding length limits: {Length} bytes. Dropping connection.", meshId, payloadLength);
                        break;
                    }

                    byte[] rentedBuffer = ArrayPool<byte>.Shared.Rent(payloadLength);
                    try
                    {
                        await stream.ReadExactlyAsync(rentedBuffer.AsMemory(0, payloadLength), cancellationToken).ConfigureAwait(false);

                        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
                        messagesReceivedCounter.Add(1, tags);
                        payloadBytesHistogram.Record(payloadLength, tags);

                        var message = wireEncoder.Decode(rentedBuffer.AsSpan(0, payloadLength).ToArray());
                        
                        if (message is not null)
                        {
                            if (!IsMajorVersionCompatible(message.ProtocolVersion, Constants.ProtocolVersion))
                            {
                                logger.LogWarning("[{MeshId}] Rejected message explicitly: Decoded version {MsgVersion} mismatches localized constraints {LocalVersion}.", 
                                    meshId, message.ProtocolVersion, Constants.ProtocolVersion);
                                continue;
                            }

                            if (!string.Equals(message.MeshId, meshId, StringComparison.Ordinal))
                            {
                                logger.LogWarning("[{MeshId}] Dropped TCP payload resolving foreign architectural target {ForeignMeshId}.", meshId, message.MeshId);
                                continue;
                            }

                            await onMessageReceived(message).ConfigureAwait(false);
                        }
                        else
                        {
                            logger.LogWarning("[{MeshId}] Rejected corrupt localized TCP envelope bridging natively unreadable bounds.", meshId);
                        }
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(rentedBuffer);
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Exception isolating explicitly mapped stream parsing safely natively.", meshId);
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

        tcpListener?.Stop();
        meter.Dispose();
    }
}