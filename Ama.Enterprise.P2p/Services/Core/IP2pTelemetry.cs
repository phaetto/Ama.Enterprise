namespace Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// Provides an abstraction for recording metrics and distributed traces related to the P2P network.
/// </summary>
public interface IP2pTelemetry
{
    /// <summary>
    /// Records the transmission of a gossip message.
    /// </summary>
    /// <param name="payloadSizeBytes">The size of the payload in bytes.</param>
    void RecordMessageSent(int payloadSizeBytes);

    /// <summary>
    /// Records the receipt of a gossip message.
    /// </summary>
    /// <param name="payloadSizeBytes">The size of the payload in bytes.</param>
    void RecordMessageReceived(int payloadSizeBytes);

    /// <summary>
    /// Records an event where a new peer joins the network and is tracked locally.
    /// </summary>
    void RecordPeerJoined();

    /// <summary>
    /// Records an event where a peer is considered dead and is removed from the active registry.
    /// </summary>
    void RecordPeerDeparted();

    /// <summary>
    /// Records when a message is dropped (e.g., due to TTL expiration or failing validation).
    /// </summary>
    void RecordMessageDropped();
}