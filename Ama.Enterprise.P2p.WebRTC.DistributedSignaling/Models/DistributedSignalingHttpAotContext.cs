namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Models;

using System.Collections.Generic;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.WebRTC.Models;

/// <summary>
/// AOT JSON context for the WebRTC CRDT distributed signaling HTTP endpoints ensuring bounds check on models.
/// </summary>
[JsonSourceGenerationOptions(
    WriteIndented = false,
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    GenerationMode = JsonSourceGenerationMode.Default)]
[JsonSerializable(typeof(IReadOnlyDictionary<string, long>))]
[JsonSerializable(typeof(Dictionary<string, long>))]
[JsonSerializable(typeof(IReadOnlyDictionary<string, WebRtcInvitationOffer>))]
[JsonSerializable(typeof(IReadOnlyDictionary<string, WebRtcInvitationAnswer>))]
[JsonSerializable(typeof(Dictionary<string, WebRtcInvitationOffer>))]
[JsonSerializable(typeof(Dictionary<string, WebRtcInvitationAnswer>))]
[JsonSerializable(typeof(WebRtcInvitationOffer))]
[JsonSerializable(typeof(WebRtcInvitationAnswer))]
internal partial class DistributedSignalingHttpAotContext : JsonSerializerContext
{
}