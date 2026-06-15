namespace Ama.Enterprise.P2p.WebRTC.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.Models;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implements outbound generic transport dynamically mapping polymorphic messages across isolated WebRTC Data Channels.
/// </summary>
public sealed class WebRtcTransport : ITransport, IDisposable
{
    private readonly string meshId;
    private readonly IWebRtcConnectionManager connectionManager;
    private readonly IMeshWireEncoder wireEncoder;
    private readonly ILogger<WebRtcTransport> logger;
    
    private readonly Meter meter;
    private readonly Counter<long> messagesSentCounter;
    private readonly Histogram<long> outboundPayloadBytesHistogram;

    public WebRtcTransport(
        string meshId,
        IWebRtcConnectionManager connectionManager,
        IMeshWireEncoder wireEncoder,
        ILogger<WebRtcTransport> logger,
        IMeterFactory? meterFactory = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(connectionManager);
        ArgumentNullException.ThrowIfNull(wireEncoder);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.connectionManager = connectionManager;
        this.wireEncoder = wireEncoder;
        this.logger = logger;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.WebRtcTransport") ?? new Meter("Ama.Enterprise.P2p.WebRtcTransport");
        this.messagesSentCounter = this.meter.CreateCounter<long>(
            "p2p.transport.webrtc.messages_sent", 
            "messages", 
            "Total messages sent via WebRTC transport");
        this.outboundPayloadBytesHistogram = this.meter.CreateHistogram<long>(
            "p2p.transport.webrtc.outbound_payload_bytes", 
            "bytes", 
            "Size of outbound WebRTC payload in bytes");
    }

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint) => endpoint is WebRtcPeerEndpoint;

    /// <inheritdoc />
    public Task SendAsync(PeerEndpoint endpoint, IMeshMessage message, CancellationToken cancellationToken)
    {
        if (endpoint is not WebRtcPeerEndpoint webrtcEndpoint)
        {
            logger.LogWarning("[{MeshId}] Cannot send WebRTC message. Target endpoint is not WebRtcPeerEndpoint.", meshId);
            return Task.CompletedTask;
        }

        var payload = wireEncoder.Encode(message);
        
        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
        messagesSentCounter.Add(1, tags);
        outboundPayloadBytesHistogram.Record(payload.Length, tags);
        
        return connectionManager.SendMessageAsync(webrtcEndpoint.ConnectionId, payload, cancellationToken);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        meter.Dispose();
    }
}