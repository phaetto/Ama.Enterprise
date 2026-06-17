namespace Ama.Enterprise.P2p.Services.Transports;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements inbound network listener extracting localized multiplexed QUIC streams safely mapping native robust pipelines.
/// </summary>
public sealed class QuicTransportListener : ITransportListener, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<QuicTransportOptions> optionsMonitor;
    private readonly IMeshWireEncoder wireEncoder;
    private readonly ILogger<QuicTransportListener> logger;

    private QuicListener? quicListener;
    private CancellationTokenSource? listenerCts;
    private Task? listeningTask;

    private readonly Meter meter;
    private readonly Counter<long> messagesReceivedCounter;
    private readonly Histogram<long> payloadBytesHistogram;

    public QuicTransportListener(
        string meshId,
        IOptionsMonitor<QuicTransportOptions> optionsMonitor,
        IMeshWireEncoder wireEncoder,
        ILogger<QuicTransportListener> logger,
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

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.QuicTransportListener") ?? new Meter("Ama.Enterprise.P2p.QuicTransportListener");
        this.messagesReceivedCounter = this.meter.CreateCounter<long>(
            "p2p.transport.quic.messages_received", 
            "messages", 
            "Total messages received via QUIC transport");
        this.payloadBytesHistogram = this.meter.CreateHistogram<long>(
            "p2p.transport.quic.inbound_payload_bytes", 
            "bytes", 
            "Size of inbound QUIC payload in bytes");
    }

    /// <inheritdoc />
    public async Task StartListeningAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(onMessageReceived);

        var options = optionsMonitor.Get(meshId);

        if (!QuicListener.IsSupported)
        {
            logger.LogError("[{MeshId}] QUIC is not supported on this platform natively. Listener aborted.", meshId);
            throw new PlatformNotSupportedException("System.Net.Quic is not supported on this operating system.");
        }

        if (options.ServerCertificate is null)
        {
            logger.LogError("[{MeshId}] QUIC listener requires a valid TLS 1.3 ServerCertificate explicitly configured natively.", meshId);
            throw new InvalidOperationException("A valid ServerCertificate must be supplied via QuicTransportOptions.");
        }

        listenerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        try
        {
            var endPoint = string.Equals(options.ListenHost, "+", StringComparison.OrdinalIgnoreCase) 
                ? new IPEndPoint(IPAddress.Any, options.ListenPort)
                : new IPEndPoint(IPAddress.Parse(options.ListenHost), options.ListenPort);

            var listenerOptions = new QuicListenerOptions
            {
                ListenEndPoint = endPoint,
                ApplicationProtocols = new List<SslApplicationProtocol> { new SslApplicationProtocol(options.AlpnProtocol) },
                ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(new QuicServerConnectionOptions
                {
                    DefaultStreamErrorCode = 0x01,
                    DefaultCloseErrorCode = 0x02,
                    MaxInboundUnidirectionalStreams = 100,
                    MaxInboundBidirectionalStreams = 10,
                    ServerAuthenticationOptions = new SslServerAuthenticationOptions
                    {
                        ApplicationProtocols = new List<SslApplicationProtocol> { new SslApplicationProtocol(options.AlpnProtocol) },
                        ServerCertificate = options.ServerCertificate
                    }
                })
            };

            quicListener = await QuicListener.ListenAsync(listenerOptions, listenerCts.Token).ConfigureAwait(false);
            logger.LogInformation("[{MeshId}] Listening globally for QUIC TLS 1.3 P2P traffic on {EndPoint}", meshId, quicListener.LocalEndPoint);
        }
        catch (QuicException ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to bind internal QUIC listener natively.", meshId);
            throw;
        }

        listeningTask = Task.Run(() => ListenLoopAsync(onMessageReceived, listenerCts.Token), listenerCts.Token);
    }

    /// <inheritdoc />
    public async Task StopListeningAsync(CancellationToken cancellationToken)
    {
        if (listenerCts is not null)
        {
            await listenerCts.CancelAsync().ConfigureAwait(false);
        }

        if (listeningTask is not null)
        {
            try
            {
                await listeningTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }

        if (quicListener is not null)
        {
            await quicListener.DisposeAsync().ConfigureAwait(false);
        }

        logger.LogInformation("[{MeshId}] QUIC transport listener stopped safely.", meshId);
    }

    private async Task ListenLoopAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (quicListener is null) return;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var connection = await quicListener.AcceptConnectionAsync(cancellationToken).ConfigureAwait(false);
                _ = Task.Run(() => ProcessConnectionAsync(connection, onMessageReceived, cancellationToken), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (QuicException ex)
            {
                if (cancellationToken.IsCancellationRequested) break;
                logger.LogError(ex, "[{MeshId}] QUIC listener error encountered mapping incoming pipeline safely.", meshId);
            }
            catch (Exception ex)
            {
                if (cancellationToken.IsCancellationRequested) break;
                logger.LogError(ex, "[{MeshId}] Fatal error bridging incoming QUIC payloads explicitly.", meshId);
            }
        }
    }

    private async Task ProcessConnectionAsync(QuicConnection connection, Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        try
        {
            await using (connection)
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var stream = await connection.AcceptInboundStreamAsync(cancellationToken).ConfigureAwait(false);
                    _ = Task.Run(() => ProcessStreamAsync(stream, onMessageReceived, cancellationToken), cancellationToken);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (QuicException) { /* Connection closed or reset remotely */ }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Exception isolating explicitly mapped QUIC connection parsing natively.", meshId);
        }
    }

    private async Task ProcessStreamAsync(QuicStream stream, Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        try
        {
            await using (stream)
            {
                var lengthBytes = new byte[4];
                
                try
                {
                    await stream.ReadExactlyAsync(lengthBytes, cancellationToken).ConfigureAwait(false);
                }
                catch (EndOfStreamException)
                {
                    return; // Graceful close natively
                }
                catch (IOException ex) when (ex.InnerException is QuicException)
                {
                    return; // Dropped connection
                }
                
                int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
                var options = optionsMonitor.Get(meshId);
                
                if (payloadLength <= 0 || payloadLength > options.MaxMessageSize)
                {
                    logger.LogWarning("[{MeshId}] Rejecting invalidly mapped QUIC payload structured over bounding length limits: {Length} bytes.", meshId, payloadLength);
                    return;
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
                            return;
                        }

                        if (!string.Equals(message.MeshId, meshId, StringComparison.Ordinal))
                        {
                            logger.LogWarning("[{MeshId}] Dropped QUIC payload resolving foreign architectural target {ForeignMeshId}.", meshId, message.MeshId);
                            return;
                        }

                        await onMessageReceived(message).ConfigureAwait(false);
                    }
                    else
                    {
                        logger.LogWarning("[{MeshId}] Rejected corrupt localized QUIC envelope bridging natively unreadable bounds.", meshId);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Exception parsing structural multiplexed QUIC stream payload.", meshId);
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

        meter.Dispose();
    }
}