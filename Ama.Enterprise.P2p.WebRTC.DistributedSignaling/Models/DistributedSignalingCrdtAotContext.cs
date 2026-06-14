namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Models;

using Ama.CRDT.Attributes;
using Ama.CRDT.Models.Aot;
using Ama.Enterprise.P2p.WebRTC.Models;
using System.Collections.Generic;

/// <summary>
/// AOT reflection context for the WebRTC Distributed Signaling CRDT models.
/// </summary>
[CrdtAotType(typeof(CrdtSignalingState))]
[CrdtAotType(typeof(WebRtcInvitationOffer))]
[CrdtAotType(typeof(WebRtcInvitationAnswer))]
[CrdtAotType(typeof(IDictionary<string, WebRtcInvitationOffer>))]
[CrdtAotType(typeof(Dictionary<string, WebRtcInvitationOffer>))]
[CrdtAotType(typeof(IDictionary<string, WebRtcInvitationAnswer>))]
[CrdtAotType(typeof(Dictionary<string, WebRtcInvitationAnswer>))]
[CrdtAotType(typeof(IDictionary<string, long>))]
[CrdtAotType(typeof(Dictionary<string, long>))]
public sealed partial class DistributedSignalingCrdtAotContext : CrdtAotContext
{
}