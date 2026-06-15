namespace Ama.Enterprise.P2p.Models.Core;

using System;

/// <summary>
/// Configuration options explicitly enforcing data-in-transit wire formatting and optional generic cryptographic bounds.
/// </summary>
public sealed class WireEncoderOptions : IEquatable<WireEncoderOptions>
{
    /// <summary>
    /// Gets or sets a value indicating whether End-to-End AEAD encryption is explicitly enabled for outgoing payloads natively.
    /// </summary>
    public bool IsEncryptionEnabled { get; set; } = false;

    /// <summary>
    /// Gets or sets the Base64-encoded 256-bit (32 bytes) symmetric key utilized for generic AES-GCM cryptographic processing.
    /// </summary>
    public string? EncryptionKeyBase64 { get; set; }

    /// <inheritdoc />
    public bool Equals(WireEncoderOptions? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return IsEncryptionEnabled == other.IsEncryptionEnabled &&
               string.Equals(EncryptionKeyBase64, other.EncryptionKeyBase64, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as WireEncoderOptions);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(IsEncryptionEnabled, EncryptionKeyBase64);
}