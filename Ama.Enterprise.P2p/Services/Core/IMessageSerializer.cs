namespace Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// Provides AOT-friendly serialization capabilities for structured protocol messages.
/// </summary>
/// <typeparam name="TMessage">The type of the message being serialized.</typeparam>
public interface IMessageSerializer<TMessage>
{
    /// <summary>
    /// Serializes a protocol message into a byte representation suitable for network transport.
    /// </summary>
    /// <param name="message">The message to serialize.</param>
    /// <returns>A read-only memory sequence containing the serialized bytes.</returns>
    ReadOnlyMemory<byte> Serialize(TMessage message);

    /// <summary>
    /// Deserializes network bytes back into a generic protocol message structure.
    /// </summary>
    /// <param name="data">The byte sequence received from the network.</param>
    /// <returns>The deserialized message, or null/default if deserialization fails.</returns>
    TMessage? Deserialize(ReadOnlyMemory<byte> data);
}