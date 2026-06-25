namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Models;

/// <summary>
/// Defines the explicit binary protocol actions orchestrating the stateful WebSockets WebRTC signaling natively.
/// </summary>
public enum WebRtcSignalingAction : byte
{
    RequestOffer = 1,
    Offer = 2,
    Answer = 3,
    FinalizeAck = 4,
    Error = 5,
    AuthRequest = 6,
    AuthResponse = 7
}