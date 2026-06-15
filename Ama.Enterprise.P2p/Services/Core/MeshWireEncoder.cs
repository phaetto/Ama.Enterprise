namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Security.Cryptography;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implements the wire encoding pipeline bridging raw serialization to optional encryption bounds dynamically.
/// </summary>
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
            throw new InvalidOperationException($"Encryption is enabled for mesh {meshId} but no EncryptionKeyBase64 is strictly configured natively.");
        }

        byte[] key = Convert.FromBase64String(options.EncryptionKeyBase64);
        if (key.Length != 32)
        {
            throw new InvalidOperationException($"Encryption key for mesh {meshId} must explicitly bridge a valid 256-bit (32 bytes) Base64 string bounds.");
        }

        var nonce = new byte[12];
        RandomNumberGenerator.Fill(nonce);

        var resultBytes = new byte[1 + 12 + 16 + plaintext.Length];
        resultBytes[0] = 0x01;
        nonce.CopyTo(resultBytes.AsSpan(1, 12));

        using var aes = new AesGcm(key, 16);
        aes.Encrypt(
            nonce: nonce,
            plaintext: plaintext,
            ciphertext: resultBytes.AsSpan(29),
            tag: resultBytes.AsSpan(13, 16)
        );

        return resultBytes;
    }

    /// <inheritdoc />
    public IMeshMessage? Decode(byte[] payload)
    {
        if (payload == null || payload.Length == 0)
        {
            return null;
        }

        var options = optionsMonitor.Get(meshId);

        if (payload[0] == 0x00)
        {
            return serializer.DeserializeFromBytes<IMeshMessage>(payload.AsSpan(1).ToArray());
        }

        if (payload[0] == 0x01)
        {
            if (!options.IsEncryptionEnabled)
            {
                logger.LogWarning("[{MeshId}] Received structurally encrypted payload explicitly but encryption is disabled locally.", meshId);
                return null;
            }

            if (string.IsNullOrWhiteSpace(options.EncryptionKeyBase64))
            {
                logger.LogError("[{MeshId}] Received encrypted payload inherently bypassing generic configuration since no key is distinctly mapped.", meshId);
                return null;
            }

            byte[] key = Convert.FromBase64String(options.EncryptionKeyBase64);
            using var aes = new AesGcm(key, 16);

            var nonce = payload.AsSpan(1, 12);
            var tag = payload.AsSpan(13, 16);
            var ciphertext = payload.AsSpan(29);

            var plaintext = new byte[ciphertext.Length];

            try
            {
                aes.Decrypt(nonce, ciphertext, tag, plaintext);
            }
            catch (CryptographicException ex)
            {
                logger.LogWarning(ex, "[{MeshId}] Failed to decrypt incoming structurally mapped payload natively. Invalid bounds or authentication mismatch.", meshId);
                return null;
            }

            return serializer.DeserializeFromBytes<IMeshMessage>(plaintext);
        }

        // Legacy unformatted payload fallback structurally capturing standard native formats securely ensuring seamless backward compatibility
        return serializer.DeserializeFromBytes<IMeshMessage>(payload);
    }
}