namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling;

/// <summary>
/// Global constants for the distributed WebRTC signaling components.
/// </summary>
public static class Constants
{
    /// <summary>
    /// The default global document identifier for the WebRTC signaling drop-box registry.
    /// </summary>
    public const string DefaultSignalingDocumentId = "webrtc-signaling-hub";

    /// <summary>
    /// The underlying CRDT document type alias mapped across the mesh network.
    /// </summary>
    public const string SignalingDocumentTypeAlias = "WebRtcSignalingState";
}