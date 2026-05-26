namespace Ama.Enterprise.Licensing.Models;

using System;

/// <summary>
/// Configuration options for tracking the honor-based license setup explicitly.
/// </summary>
public sealed class LicenseOptions : IEquatable<LicenseOptions>
{
    /// <summary>
    /// Gets or sets the manual license string.
    /// </summary>
    public string? LicenseKey { get; set; }

    /// <summary>
    /// Gets or sets the path to the file containing the license string.
    /// </summary>
    public string? LicenseFilePath { get; set; }

    /// <summary>
    /// Gets or sets the public key in PEM format used for cryptographic license validation.
    /// </summary>
    public string? PublicKeyPem { get; set; }

    /// <inheritdoc />
    public bool Equals(LicenseOptions? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return string.Equals(LicenseKey, other.LicenseKey, StringComparison.Ordinal) &&
               string.Equals(LicenseFilePath, other.LicenseFilePath, StringComparison.Ordinal) &&
               string.Equals(PublicKeyPem, other.PublicKeyPem, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        return ReferenceEquals(this, obj) || (obj is LicenseOptions other && Equals(other));
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(LicenseKey, LicenseFilePath, PublicKeyPem);
    }
}