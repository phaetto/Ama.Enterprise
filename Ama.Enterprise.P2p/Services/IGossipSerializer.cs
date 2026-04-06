using Ama.Enterprise.P2p.Models;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// Provides AOT-friendly serialization capabilities for gossip network messages.
/// Designed to strictly use System.Text.Json in AOT mode.
/// </summary>
public interface IGossipSerializer
{
    /// <summary>
    /// Serializes a gossip message into a byte representation suitable for network transport.
    /// </summary>
    /// <param name="message">The message to serialize.</param>
    /// <returns>A read-only memory sequence containing the serialized bytes.</returns>
    ReadOnlyMemory<byte> Serialize(GossipMessage message);

    /// <summary>
    /// Deserializes network bytes back into a gossip message struct.
    /// </summary>
    /// <param name="data">The byte sequence received from the network.</param>
    /// <returns>The deserialized gossip message, or null if deserialization fails.</returns>
    GossipMessage? Deserialize(ReadOnlyMemory<byte> data);
}