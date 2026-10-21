namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Buffers;
using System.Security.Cryptography;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements the wire encoding pipeline bridging raw serialization to optional encryption bounds dynamically.
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
public sealed class MeshWireEncoder : IMeshWireEncoder
{
    private readonly string meshId;
    private readonly IOptionsMonitor<WireEncoderOptions> optionsMonitor;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<MeshWireEncoder> logger;

    public MeshWireEncoder(
        string meshId,
        IOptionsMonitor<WireEncoderOptions> optionsMonitor,
        ICrdtSerializer serializer,
        ILogger<MeshWireEncoder> logger)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.serializer = serializer;
        this.logger = logger;
    }

    /// <inheritdoc />
    public byte[] Encode(IMeshMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        var plaintext = serializer.SerializeToBytes(message);
        var options = optionsMonitor.Get(meshId);

        if (!options.IsEncryptionEnabled)
        {
            var result = new byte[plaintext.Length + 1];
            result[0] = 0x00;
            plaintext.CopyTo(result.AsSpan(1));
            return result;
        }

        if (string.IsNullOrWhiteSpace(options.EncryptionKeyBase64))
        {
            throw new InvalidOperationException($"Encryption is enabled for mesh {meshId} but no EncryptionKeyBase64 is configured.");
        }

        Span<byte> key = stackalloc byte[64];
        if (!Convert.TryFromBase64String(options.EncryptionKeyBase64, key, out int keyBytesWritten) || keyBytesWritten != 32)
        {
            throw new InvalidOperationException($"Encryption key for mesh {meshId} must bridge a valid 256-bit (32 bytes) Base64 string bounds.");
        }

        var resultBytes = new byte[1 + 12 + 16 + plaintext.Length];
        resultBytes[0] = 0x01;

        var nonce = resultBytes.AsSpan(1, 12);
        RandomNumberGenerator.Fill(nonce);

        using var aes = new AesGcm(key.Slice(0, 32), 16);
        aes.Encrypt(
            nonce: nonce,
            plaintext: plaintext,
            ciphertext: resultBytes.AsSpan(29),
            tag: resultBytes.AsSpan(13, 16)
        );

        return resultBytes;
    }

    /// <inheritdoc />
    public IMeshMessage? Decode(ReadOnlySpan<byte> payload)
    {
        if (payload.IsEmpty)
        {
            return null;
        }

        var options = optionsMonitor.Get(meshId);

        if (payload[0] == 0x00)
        {
            if (options.IsEncryptionEnabled)
            {
                logger.LogWarning("[{MeshId}] Received unencrypted payload but encryption is mandated by configuration. Payload rejected.", meshId);
                return null;
            }
            
            return serializer.DeserializeFromBytes<IMeshMessage>(payload.Slice(1));
        }

        if (payload[0] == 0x01)
        {
            if (!options.IsEncryptionEnabled)
            {
                logger.LogWarning("[{MeshId}] Received structurally encrypted payload explicitly but encryption is disabled locally.", meshId);
                return null;
            }

            if (payload.Length < 29)
            {
                logger.LogWarning("[{MeshId}] Received encrypted payload shorter than the minimum header length.", meshId);
                return null;
            }

            if (string.IsNullOrWhiteSpace(options.EncryptionKeyBase64))
            {
                logger.LogError("[{MeshId}] Received encrypted payload inherently bypassing generic configuration since no key is distinctly mapped.", meshId);
                return null;
            }

            Span<byte> key = stackalloc byte[64];
            if (!Convert.TryFromBase64String(options.EncryptionKeyBase64, key, out int keyBytesWritten) || keyBytesWritten != 32)
            {
                logger.LogError("[{MeshId}] Configured encryption key is not a valid 256-bit Base64 string.", meshId);
                return null;
            }

            using var aes = new AesGcm(key.Slice(0, 32), 16);

            var nonce = payload.Slice(1, 12);
            var tag = payload.Slice(13, 16);
            var ciphertext = payload.Slice(29);

            byte[] rentedPlaintext = ArrayPool<byte>.Shared.Rent(ciphertext.Length);
            try
            {
                var plaintextSpan = rentedPlaintext.AsSpan(0, ciphertext.Length);
                try
                {
                    aes.Decrypt(nonce, ciphertext, tag, plaintextSpan);
                }
                catch (CryptographicException ex)
                {
                    logger.LogWarning(ex, "[{MeshId}] Failed to decrypt incoming structurally mapped payload natively. Invalid bounds or authentication mismatch.", meshId);
                    return null;
                }

                return serializer.DeserializeFromBytes<IMeshMessage>(plaintextSpan);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rentedPlaintext);
            }
        }

        logger.LogWarning("[{MeshId}] Received payload with an unknown prefix byte. Payload rejected to prevent downgrade attacks.", meshId);
        return null;
    }
}