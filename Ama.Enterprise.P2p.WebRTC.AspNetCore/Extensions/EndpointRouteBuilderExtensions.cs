namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Extensions;

using System;
using System.Net.WebSockets;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Models;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;
using Ama.Enterprise.P2p.WebRTC.Models;
using Ama.Enterprise.P2p.WebRTC.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Provides extension methods for mapping ASP.NET Core API endpoints resolving out-of-band WebSocket WebRTC signaling streams.
/// </summary>
public static class EndpointRouteBuilderExtensions
{
    /// <summary>
    /// Maps standard inbound WebRTC signaling endpoints managing WebSocket isolated handshakes negotiating SDP descriptors dynamically.
    /// </summary>
    /// <param name="endpoints">The route builder instance tracking internal network structural boundaries.</param>
    /// <returns>A convention builder permitting explicit mapped endpoint customization.</returns>
    public static IEndpointConventionBuilder MapP2pWebRtcSignalingEndpoints(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var options = endpoints.ServiceProvider.GetService<IOptions<WebRtcSignalingOptions>>()?.Value;
        var pathPrefix = options?.PathPrefix ?? "/ama-enterprise/webrtc-signaling";

        var basePath = string.IsNullOrWhiteSpace(pathPrefix) ? "/ama-enterprise/webrtc-signaling" : pathPrefix.TrimEnd('/');
        if (!basePath.StartsWith("/", StringComparison.Ordinal))
        {
            basePath = "/" + basePath;
        }

        var group = endpoints.MapGroup(basePath);

        // Handle fully decoupled native full-duplex WebSockets interactions exclusively.
        group.MapGet("/{meshId}/ws", new RequestDelegate(async context =>
        {
            var meshId = context.GetRouteValue("meshId")?.ToString();
            if (string.IsNullOrEmpty(meshId) || !context.WebSockets.IsWebSocketRequest)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var invitationService = context.RequestServices.GetKeyedService<IWebRtcInvitationService>(meshId);
            if (invitationService is null)
            {
                context.Response.StatusCode = StatusCodes.Status404NotFound;
                return;
            }

            // Resolve dependencies aggressively safely supporting both generic Keyed scopes or overarching Singletons mappings
            var serializer = context.RequestServices.GetService<ICrdtSerializer>();
            var logger = context.RequestServices.GetService<ILoggerFactory>()?.CreateLogger(typeof(EndpointRouteBuilderExtensions));
            
            var authenticator = context.RequestServices.GetKeyedService<IPeerAuthenticator>(meshId) ?? context.RequestServices.GetService<IPeerAuthenticator>();
            var peerRegistry = context.RequestServices.GetKeyedService<IPeerRegistry>(meshId) ?? context.RequestServices.GetService<IPeerRegistry>();
            var failureDetector = context.RequestServices.GetKeyedService<IFailureDetector>(meshId) ?? context.RequestServices.GetService<IFailureDetector>();
            var nodeOptionsMonitor = context.RequestServices.GetService<IOptionsMonitor<P2pNodeOptions>>();

            if (serializer is null || logger is null || authenticator is null || peerRegistry is null || failureDetector is null || nodeOptionsMonitor is null)
            {
                context.Response.StatusCode = StatusCodes.Status500InternalServerError;
                return;
            }

            using var webSocket = await context.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);
            
            try
            {
                var (action, reqPayload) = await WebRtcSignalingWsHelper.ReceiveMessageAsync(webSocket, context.RequestAborted).ConfigureAwait(false);
                
                // 1. Enforce Authentication Step
                if (action != WebRtcSignalingAction.AuthRequest)
                {
                    logger.LogWarning("[{MeshId}] Dropping WebSocket connection. Required authentication handshake missing.", meshId);
                    await WebRtcSignalingWsHelper.SendMessageAsync(webSocket, WebRtcSignalingAction.Error, Array.Empty<byte>(), context.RequestAborted).ConfigureAwait(false);
                    await webSocket.CloseAsync(WebSocketCloseStatus.PolicyViolation, "Authentication Required", context.RequestAborted).ConfigureAwait(false);
                    return;
                }

                var clientPayload = serializer.DeserializeFromBytes<PeerHandshakePayload>(reqPayload);
                var isAuthenticated = await authenticator.AuthenticateAsync(clientPayload.Node, clientPayload.HandshakeData, context.RequestAborted).ConfigureAwait(false);

                if (!isAuthenticated)
                {
                    logger.LogWarning("[{MeshId}] Unauthorized WebRTC WebSocket signaling connection explicitly rejected.", meshId);
                    await WebRtcSignalingWsHelper.SendMessageAsync(webSocket, WebRtcSignalingAction.Error, Array.Empty<byte>(), context.RequestAborted).ConfigureAwait(false);
                    await webSocket.CloseAsync(WebSocketCloseStatus.PolicyViolation, "Unauthorized", context.RequestAborted).ConfigureAwait(false);
                    return;
                }

                // 2. Respond with Local Authentication
                var nodeOptions = nodeOptionsMonitor.Get(meshId);
                var localHandshakeData = await authenticator.GetLocalHandshakeDataAsync(context.RequestAborted).ConfigureAwait(false);
                var localNode = new PeerNode(new PeerId(nodeOptions.LocalPeerId), new WebRtcPeerEndpoint(Guid.Empty));
                
                var serverPayload = new PeerHandshakePayload
                {
                    Node = localNode,
                    HandshakeData = localHandshakeData.ToArray()
                };
                
                var serverPayloadBytes = serializer.SerializeToBytes(serverPayload);
                await WebRtcSignalingWsHelper.SendMessageAsync(webSocket, WebRtcSignalingAction.AuthResponse, serverPayloadBytes, context.RequestAborted).ConfigureAwait(false);

                // 3. Receive Next Action (RequestOffer or Offer)
                var nextMsg = await WebRtcSignalingWsHelper.ReceiveMessageAsync(webSocket, context.RequestAborted).ConfigureAwait(false);
                action = nextMsg.Action;
                reqPayload = nextMsg.Payload;

                if (action == WebRtcSignalingAction.RequestOffer)
                {
                    var offer = await invitationService.CreateInvitationAsync(context.RequestAborted).ConfigureAwait(false);
                    
                    // Track explicitly authorized remote peer using dynamic SDP connection binding
                    var remoteNode = new PeerNode(clientPayload.Node.Id, new WebRtcPeerEndpoint(offer.ConnectionId));
                    await failureDetector.RecordHeartbeatAsync(remoteNode.Id, context.RequestAborted).ConfigureAwait(false);
                    await peerRegistry.AddOrUpdatePeerAsync(meshId, remoteNode, PeerStatus.Active, context.RequestAborted).ConfigureAwait(false);

                    var offerPayload = serializer.SerializeToBytes(offer);
                    await WebRtcSignalingWsHelper.SendMessageAsync(webSocket, WebRtcSignalingAction.Offer, offerPayload, context.RequestAborted).ConfigureAwait(false);
                    
                    var (ansAction, ansPayload) = await WebRtcSignalingWsHelper.ReceiveMessageAsync(webSocket, context.RequestAborted).ConfigureAwait(false);
                    if (ansAction == WebRtcSignalingAction.Answer)
                    {
                        var answer = serializer.DeserializeFromBytes<WebRtcInvitationAnswer>(ansPayload);
                        await invitationService.FinalizeInvitationAsync(answer.ConnectionId, answer.SdpAnswer, context.RequestAborted).ConfigureAwait(false);
                        
                        await WebRtcSignalingWsHelper.SendMessageAsync(webSocket, WebRtcSignalingAction.FinalizeAck, Array.Empty<byte>(), context.RequestAborted).ConfigureAwait(false);
                        
                        if (webSocket.State == WebSocketState.Open || webSocket.State == WebSocketState.CloseReceived)
                        {
                            await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Negotiation Complete", context.RequestAborted).ConfigureAwait(false);
                        }
                    }
                }
                else if (action == WebRtcSignalingAction.Offer)
                {
                    var offer = serializer.DeserializeFromBytes<WebRtcInvitationOffer>(reqPayload);
                    
                    // Track explicitly authorized remote peer using dynamic SDP connection binding
                    var remoteNode = new PeerNode(clientPayload.Node.Id, new WebRtcPeerEndpoint(offer.ConnectionId));
                    await failureDetector.RecordHeartbeatAsync(remoteNode.Id, context.RequestAborted).ConfigureAwait(false);
                    await peerRegistry.AddOrUpdatePeerAsync(meshId, remoteNode, PeerStatus.Active, context.RequestAborted).ConfigureAwait(false);

                    var localAnswer = await invitationService.AcceptInvitationAsync(offer.SdpOffer, context.RequestAborted).ConfigureAwait(false);
                    var answerDto = new WebRtcInvitationAnswer(offer.ConnectionId, localAnswer.SdpAnswer);
                    var ansPayload = serializer.SerializeToBytes(answerDto);
                    
                    await WebRtcSignalingWsHelper.SendMessageAsync(webSocket, WebRtcSignalingAction.Answer, ansPayload, context.RequestAborted).ConfigureAwait(false);
                    
                    var (ackAction, _) = await WebRtcSignalingWsHelper.ReceiveMessageAsync(webSocket, context.RequestAborted).ConfigureAwait(false);
                    if (ackAction == WebRtcSignalingAction.FinalizeAck)
                    {
                        if (webSocket.State == WebSocketState.Open || webSocket.State == WebSocketState.CloseReceived)
                        {
                            await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Negotiation Complete", context.RequestAborted).ConfigureAwait(false);
                        }
                    }
                }
                else
                {
                    if (webSocket.State == WebSocketState.Open)
                    {
                        await webSocket.CloseAsync(WebSocketCloseStatus.InvalidMessageType, "Invalid Action", context.RequestAborted).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "[{MeshId}] Exception during WebRTC explicit WebSocket signaling negotiation natively.", meshId);
                if (webSocket.State == WebSocketState.Open)
                {
                    await webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "Error", context.RequestAborted).ConfigureAwait(false);
                }
            }
        }));

        return group;
    }
}