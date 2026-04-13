namespace Ama.Enterprise.P2p.WebRTC.Models;

using System.Text.Json.Serialization;

/// <summary>
/// Source-generated JSON serialization context for AOT-friendly serialization of WebRTC specific payloads.
/// </summary>
[JsonSerializable(typeof(WebRtcHandshakeMessage))]
[JsonSerializable(typeof(WebRtcPeerEndpoint))]
[JsonSerializable(typeof(WebRtcInvitationOffer))]
[JsonSerializable(typeof(WebRtcInvitationAnswer))]
internal sealed partial class WebRtcJsonContext : JsonSerializerContext
{
}