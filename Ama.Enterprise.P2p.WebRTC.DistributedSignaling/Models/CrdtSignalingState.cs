namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Models;

using System;
using System.Collections.Generic;
using Ama.Enterprise.P2p.WebRTC.Models;

/// <summary>
/// Root CRDT document model representing the WebRTC out-of-band signaling state.
/// Acts as an in-memory drop-box for SDP Offers, Answers, and active Join Intents.
/// </summary>
public sealed class CrdtSignalingState
{
    /// <summary>
    /// Gets or sets the explicit string identifier defining this distinct signaling hub state.
    /// </summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the mapped sequence tracking active participant presence (Join Intents).
    /// Keyed by PeerId as string, with the value tracking the UTC timestamp of intent creation.
    /// </summary>
    public Dictionary<string, long> JoinIntents { get; set; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Gets or sets the mapped sequence tracking active SDP offers awaiting answers.
    /// Keyed dynamically by `{TargetPeerId}:{OffererPeerId}`.
    /// </summary>
    public Dictionary<string, WebRtcInvitationOffer> Offers { get; set; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Gets or sets the mapped sequence tracking active SDP answers awaiting collection by the offerer.
    /// Keyed dynamically by `{OffererPeerId}:{AnswererPeerId}`.
    /// </summary>
    public Dictionary<string, WebRtcInvitationAnswer> Answers { get; set; } = new(StringComparer.Ordinal);
}