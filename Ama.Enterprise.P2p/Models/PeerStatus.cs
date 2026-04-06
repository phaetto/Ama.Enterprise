namespace Ama.Enterprise.P2p.Models;

/// <summary>
/// Defines the lifecycle state of a peer node in the gossip network.
/// </summary>
public enum PeerStatus
{
    /// <summary>
    /// The peer is considered active, reachable, and healthy.
    /// </summary>
    Active,

    /// <summary>
    /// The peer has missed heartbeats but is not yet considered dead.
    /// </summary>
    Suspect,

    /// <summary>
    /// The peer has been unresponsive for too long and is considered dead or disconnected.
    /// </summary>
    Dead
}