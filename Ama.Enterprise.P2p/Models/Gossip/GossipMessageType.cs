namespace Ama.Enterprise.P2p.Models.Gossip;

/// <summary>
/// Defines the behavior and explicit target payload mapping of a generic gossip message, enabling advanced push-pull capabilities.
/// </summary>
public enum GossipMessageType : byte
{
    /// <summary>
    /// Standard rumor-mongering application payload broadcast over the active mesh network.
    /// </summary>
    Broadcast = 0,

    /// <summary>
    /// Outbound push digest containing localized active message identifiers to initiate anti-entropy state synchronization.
    /// </summary>
    Digest = 1,

    /// <summary>
    /// Direct targeted pull request requesting missing payloads previously identified via an explicit push digest.
    /// </summary>
    PullRequest = 2
}