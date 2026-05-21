namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;

using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Models;

/// <summary>
/// Internal helper safely wrapping explicit native WebSockets bounds isolating generic out-of-band envelope negotiations natively.
/// </summary>
internal static class WebRtcSignalingWsHelper
{
    public static async Task<(WebRtcSignalingAction Action, byte[] Payload)> ReceiveMessageAsync(WebSocket webSocket, CancellationToken cancellationToken)
    {
        var buffer = new byte[8192];
        using var ms = new MemoryStream();
        WebSocketReceiveResult result;
        do
        {
            result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), cancellationToken).ConfigureAwait(false);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                return (WebRtcSignalingAction.Error, Array.Empty<byte>());
            }
                
            ms.Write(buffer, 0, result.Count);

            if (ms.Length > 1024 * 1024)
            {
                throw new InvalidDataException("WebSocket inbound signaling payload exceeds 1MB limit dynamically effectively.");
            }
        } while (!result.EndOfMessage);

        var data = ms.ToArray();
        if (data.Length == 0) return (WebRtcSignalingAction.Error, Array.Empty<byte>());
        
        return ((WebRtcSignalingAction)data[0], data.AsSpan(1).ToArray());
    }

    public static async Task SendMessageAsync(WebSocket webSocket, WebRtcSignalingAction action, byte[] payload, CancellationToken cancellationToken)
    {
        var buffer = new byte[payload.Length + 1];
        buffer[0] = (byte)action;
        payload.CopyTo(buffer.AsSpan(1));
        await webSocket.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Binary, true, cancellationToken).ConfigureAwait(false);
    }
}