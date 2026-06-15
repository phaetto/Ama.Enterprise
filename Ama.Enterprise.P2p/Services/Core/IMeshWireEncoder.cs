namespace Ama.Enterprise.P2p.Services.Core;

using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Interface for encoding and decoding mesh messages, evaluating optional cryptographic data-in-transit boundaries safely.
/// </summary>
/// <remarks>
/// SECURITY EXPECTATIONS AND LIMITATIONS:
/// This encoder is designed strictly as a "Defense in Depth" application-level encryption layer.
/// It is NOT a replacement for a comprehensive secure transport protocol.
/// 
/// - Static Keys: It utilizes a single, static shared symmetric key (AES-GCM) across the entire mesh.
/// - No Perfect Forward Secrecy (PFS): Because the key is static, compromise of the key allows historical payload decryption.
/// - No Replay Protection: It lacks cryptographic nonces or monotonic sequence numbers required to drop duplicated packets.
/// 
/// Enterprise Usage:
/// For enterprise-grade security, this encoder MUST be tunneled over a secure transport layer such as HTTPS/mTLS or QUIC.
/// When combined with mTLS, the transport layer handles Replay Protection, Perfect Forward Secrecy, and Identity Binding automatically.
/// This custom wire encoder then serves as an additional zero-trust boundary, ensuring that payloads remain encrypted 
/// against infrastructure-level packet inspection or intermediate multi-hop routing intercepts.
/// </remarks>
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