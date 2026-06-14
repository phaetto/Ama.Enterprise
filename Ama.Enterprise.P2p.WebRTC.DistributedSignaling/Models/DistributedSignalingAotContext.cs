namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Models;

using Ama.CRDT.Models;
using Ama.Enterprise.P2p.WebRTC.Models;
using System.Collections.Generic;
using System.Text.Json.Serialization;

/// <summary>
/// AOT JSON context for the WebRTC CRDT distributed signaling state models.
/// </summary>
[JsonSourceGenerationOptions(
    WriteIndented = false,
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    GenerationMode = JsonSourceGenerationMode.Default)]
[JsonSerializable(typeof(CrdtSignalingState))]
[JsonSerializable(typeof(Dictionary<string, long>))]
[JsonSerializable(typeof(IReadOnlyDictionary<string, long>))]
[JsonSerializable(typeof(IReadOnlyDictionary<string, WebRtcInvitationOffer>))]
[JsonSerializable(typeof(IReadOnlyDictionary<string, WebRtcInvitationAnswer>))]
[JsonSerializable(typeof(Dictionary<string, WebRtcInvitationOffer>))]
[JsonSerializable(typeof(Dictionary<string, WebRtcInvitationAnswer>))]
[JsonSerializable(typeof(WebRtcInvitationOffer))]
[JsonSerializable(typeof(WebRtcInvitationAnswer))]
[JsonSerializable(typeof(CrdtDocument<CrdtSignalingState>))]
[JsonSerializable(typeof(List<JournaledOperation>))]
[JsonSerializable(typeof(DottedVersionVector))]
public sealed partial class DistributedSignalingAotContext : JsonSerializerContext
{
}