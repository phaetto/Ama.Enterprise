namespace Ama.Enterprise.P2p.WebRTC.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.WebRTC.Models;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SIPSorcery.Net;

/// <summary>
/// Implements the management of WebRTC connections and out-of-band signaling using SIPSorcery.
/// </summary>
public sealed class WebRtcConnectionManager : IWebRtcConnectionManager, IWebRtcInvitationService, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<WebRtcOptions> optionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly IPeerRegistry peerRegistry;
    private readonly ILogger<WebRtcConnectionManager> logger;
    
    private readonly ConcurrentDictionary<Guid, PeerConnectionState> connections = new();

    /// <inheritdoc />
    public event Func<Guid, byte[], Task>? OnMessageReceived;

    /// <inheritdoc />
    public event Action<Guid, string>? OnConnectionStateChanged;

    /// <summary>
    /// Initializes a new instance of the <see cref="WebRtcConnectionManager"/> class.
    /// </summary>
    public WebRtcConnectionManager(
        string meshId,
        IOptionsMonitor<WebRtcOptions> optionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        IPeerRegistry peerRegistry,
        ILogger<WebRtcConnectionManager> logger)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.peerRegistry = peerRegistry ?? throw new ArgumentNullException(nameof(peerRegistry));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task<WebRtcInvitationOffer> CreateInvitationAsync(CancellationToken cancellationToken)
    {
        var connectionId = Guid.NewGuid();
        var pc = CreatePeerConnection(connectionId);
        var dc = await pc.createDataChannel("p2p-data").ConfigureAwait(false);
        
        var state = new PeerConnectionState(pc, dc);
        connections.TryAdd(connectionId, state);

        BindDataChannelEvents(connectionId, dc);

        var offer = pc.createOffer(null);
        await pc.setLocalDescription(offer).ConfigureAwait(false);

        var sdpOffer = await WaitForIceGatheringAsync(pc, cancellationToken).ConfigureAwait(false);
        
        logger.LogInformation("[{MeshId}] Created WebRTC invitation {ConnectionId}.", meshId, connectionId);
        
        return new WebRtcInvitationOffer(connectionId, sdpOffer);
    }

    /// <inheritdoc />
    public async Task<WebRtcInvitationAnswer> AcceptInvitationAsync(string sdpOffer, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(sdpOffer))
        {
            throw new ArgumentException("SDP Offer cannot be null or empty.", nameof(sdpOffer));
        }

        var connectionId = Guid.NewGuid();
        var pc = CreatePeerConnection(connectionId);
        var state = new PeerConnectionState(pc, null);
        connections.TryAdd(connectionId, state);

        pc.ondatachannel += (dc) =>
        {
            if (dc.label == "p2p-data")
            {
                state.DataChannel = dc;
                BindDataChannelEvents(connectionId, dc);
            }
        };

        var init = new RTCSessionDescriptionInit { type = RTCSdpType.offer, sdp = sdpOffer };
        var result = pc.setRemoteDescription(init);
        if (result != SetDescriptionResultEnum.OK)
        {
            throw new InvalidOperationException($"Failed to set remote description: {result}");
        }

        var answer = pc.createAnswer(null);
        await pc.setLocalDescription(answer).ConfigureAwait(false);

        var sdpAnswer = await WaitForIceGatheringAsync(pc, cancellationToken).ConfigureAwait(false);

        logger.LogInformation("[{MeshId}] Accepted WebRTC invitation {ConnectionId}.", meshId, connectionId);

        return new WebRtcInvitationAnswer(connectionId, sdpAnswer);
    }

    /// <inheritdoc />
    public Task FinalizeInvitationAsync(Guid connectionId, string sdpAnswer, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(sdpAnswer))
        {
            throw new ArgumentException("SDP Answer cannot be null or empty.", nameof(sdpAnswer));
        }

        if (!connections.TryGetValue(connectionId, out var state))
        {
            throw new InvalidOperationException($"Connection {connectionId} not found.");
        }

        var init = new RTCSessionDescriptionInit { type = RTCSdpType.answer, sdp = sdpAnswer };
        var result = state.PeerConnection.setRemoteDescription(init);
        if (result != SetDescriptionResultEnum.OK)
        {
            throw new InvalidOperationException($"Failed to set remote description: {result}");
        }

        logger.LogInformation("[{MeshId}] Finalized WebRTC connection {ConnectionId}.", meshId, connectionId);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task SendMessageAsync(Guid connectionId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (payload.IsEmpty)
        {
            throw new ArgumentException("Payload cannot be empty.", nameof(payload));
        }

        if (connections.TryGetValue(connectionId, out var state) && state.DataChannel is { readyState: RTCDataChannelState.open })
        {
            var buffer = new byte[payload.Length + 1];
            buffer[0] = 0x00; // Normal message indicator
            payload.CopyTo(buffer.AsMemory(1));
            state.DataChannel.send(buffer);
        }
        else
        {
            logger.LogWarning("[{MeshId}] Cannot send message on WebRTC connection {ConnectionId}. Channel is closed or non-existent.", meshId, connectionId);
        }

        return Task.CompletedTask;
    }

    private RTCPeerConnection CreatePeerConnection(Guid connectionId)
    {
        var options = optionsMonitor.Get(meshId);
        var iceServers = new List<RTCIceServer>();
        
        foreach (var server in options.IceServers)
        {
            iceServers.Add(new RTCIceServer { urls = server });
        }

        var config = new RTCConfiguration
        {
            iceServers = iceServers
        };

        var pc = new RTCPeerConnection(config);
        
        pc.onconnectionstatechange += (state) => 
        {
            logger.LogDebug("[{MeshId}] WebRTC connection {ConnectionId} state changed: {State}", meshId, connectionId, state);
            
            // Broadcast connection state strictly to attached UI interfaces natively
            OnConnectionStateChanged?.Invoke(connectionId, state.ToString());

            if (state == RTCPeerConnectionState.closed || state == RTCPeerConnectionState.failed)
            {
                if (connections.TryRemove(connectionId, out var removedState))
                {
                    removedState.DataChannel?.close();
                    removedState.PeerConnection?.Close("Disconnected");
                }
            }
        };

        return pc;
    }

    private void BindDataChannelEvents(Guid connectionId, RTCDataChannel dc)
    {
        dc.onmessage += (channel, protocol, data) =>
        {
            if (data == null || data.Length == 0) return;

            try
            {
                if (data[0] == 0xFF) // Handshake initialization
                {
                    var jsonBytes = data.AsSpan(1);
                    var handshake = System.Text.Json.JsonSerializer.Deserialize(jsonBytes, WebRtcJsonContext.Default.WebRtcHandshakeMessage);
                    
                    if (handshake != null && handshake.PeerId != Guid.Empty)
                    {
                        var peerId = new PeerId(handshake.PeerId);
                        var endpoint = new WebRtcPeerEndpoint(connectionId);
                        var node = new PeerNode(peerId, endpoint);
                        
                        logger.LogInformation("[{MeshId}] Discovered PeerId {PeerId} via WebRTC handshake on connection {ConnectionId}.", meshId, peerId.Value, connectionId);
                        
                        // Register dynamically to trigger discovery topology events implicitly
                        _ = peerRegistry.AddOrUpdatePeerAsync(meshId, node, PeerStatus.Active, CancellationToken.None);
                    }
                }
                else if (data[0] == 0x00) // Standard payload mapping
                {
                    if (OnMessageReceived != null)
                    {
                        var payload = data.Skip(1).ToArray();
                        _ = OnMessageReceived(connectionId, payload);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Error processing incoming WebRTC message on connection {ConnectionId}.", meshId, connectionId);
            }
        };

        bool handshakeSent = false;
        object handshakeLock = new object();

        void SendHandshake()
        {
            lock (handshakeLock)
            {
                if (handshakeSent) return;
                handshakeSent = true;
            }

            logger.LogInformation("[{MeshId}] WebRTC data channel ready for connection {ConnectionId}. Sending handshake.", meshId, connectionId);
            
            try
            {
                var nodeOptions = nodeOptionsMonitor.Get(meshId);
                var handshake = new WebRtcHandshakeMessage { PeerId = nodeOptions.LocalPeerId };
                
                var jsonBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(handshake, WebRtcJsonContext.Default.WebRtcHandshakeMessage);
                var buffer = new byte[jsonBytes.Length + 1];
                buffer[0] = 0xFF; // Handshake prefix
                jsonBytes.CopyTo(buffer, 1);
                
                dc.send(buffer);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Failed to send handshake on WebRTC connection {ConnectionId}.", meshId, connectionId);
            }
        }

        dc.onopen += SendHandshake;

        if (dc.readyState == RTCDataChannelState.open)
        {
            SendHandshake();
        }
    }

    private async Task<string> WaitForIceGatheringAsync(RTCPeerConnection pc, CancellationToken cancellationToken)
    {
        var options = optionsMonitor.Get(meshId);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(options.IceGatheringTimeout);

        var tcs = new TaskCompletionSource<string>();
        
        void IceGatheringHandler(RTCIceGatheringState state)
        {
            if (state == RTCIceGatheringState.complete)
            {
                tcs.TrySetResult(pc.localDescription.sdp.ToString());
            }
        }

        pc.onicegatheringstatechange += IceGatheringHandler;

        try
        {
            if (pc.iceGatheringState == RTCIceGatheringState.complete)
            {
                return pc.localDescription.sdp.ToString();
            }

            await using (cts.Token.Register(() => tcs.TrySetCanceled()))
            {
                return await tcs.Task.ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("[{MeshId}] ICE gathering timed out. Proceeding with currently gathered trickle candidates.", meshId);
            return pc.localDescription.sdp.ToString();
        }
        finally
        {
            pc.onicegatheringstatechange -= IceGatheringHandler;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var state in connections.Values)
        {
            state.DataChannel?.close();
            state.PeerConnection?.Close("Disposing");
        }
        connections.Clear();
    }

    private sealed class PeerConnectionState
    {
        public RTCPeerConnection PeerConnection { get; }
        public RTCDataChannel? DataChannel { get; set; }

        public PeerConnectionState(RTCPeerConnection peerConnection, RTCDataChannel? dataChannel)
        {
            PeerConnection = peerConnection;
            DataChannel = dataChannel;
        }
    }
}