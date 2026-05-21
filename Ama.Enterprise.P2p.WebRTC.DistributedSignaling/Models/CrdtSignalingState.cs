namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Models;

using System;
using System.Collections.Generic;
using Ama.Enterprise.P2p.WebRTC.Models;

/// <summary>
/// Root CRDT document model representing the WebRTC out-of-band signaling state.
/// Acts as an in-memory drop-box for SDP Offers and Answers.
/// </summary>
public sealed class CrdtSignalingState
{
    /// <summary>
    /// Gets or sets the explicit string identifier defining this distinct signaling hub state.
    /// </summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the mapped sequence tracking active SDP offers awaiting answers.
    /// Keyed by ConnectionId as string.
    /// </summary>
    public Dictionary<string, WebRtcInvitationOffer> Offers { get; set; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Gets or sets the mapped sequence tracking active SDP answers awaiting collection by the offerer.
    /// Keyed by ConnectionId as string.
    /// </summary>
    public Dictionary<string, WebRtcInvitationAnswer> Answers { get; set; } = new(StringComparer.Ordinal);
}