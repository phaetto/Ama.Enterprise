namespace Ama.Enterprise.P2p.Services.Core;

using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Interface for encoding and decoding mesh messages, evaluating optional cryptographic data-in-transit boundaries safely.
/// </summary>
public interface IMeshWireEncoder
{
    /// <summary>
    /// Encodes the P2P mesh message securely handling optional mapped encryption routines.
    /// </summary>
    /// <param name="message">The target message to encode.</param>
    /// <returns>The encoded wire-ready byte payload.</returns>
    byte[] Encode(IMeshMessage message);

    /// <summary>
    /// Decodes the byte payload into a generic mesh message mapping decryption natively.
    /// </summary>
    /// <param name="payload">The byte array payload to decode.</param>
    /// <returns>The parsed mesh message, or null if gracefully rejected.</returns>
    IMeshMessage? Decode(byte[] payload);
}