namespace Ama.Enterprise.Licensing.Models;

using System;
using System.Security.Cryptography.X509Certificates;

/// <summary>
/// Configuration options for tracking the honor-based license setup.
/// </summary>
public sealed class LicenseOptions : IEquatable<LicenseOptions>
{
    /// <summary>
    /// Gets or sets the declared license type, demonstrating acceptance of terms.
    /// </summary>
    public DeclaredLicenseType DeclaredLicenseType { get; set; } = DeclaredLicenseType.Unknown;

    /// <summary>
    /// Gets or sets the manual license string.
    /// </summary>
    public string? LicenseKey { get; set; }

    /// <summary>
    /// Gets or sets the path to the file containing the license string.
    /// </summary>
    public string? LicenseFilePath { get; set; }

    /// <summary>
    /// Gets or sets the path to the X.509 certificate file used for cryptographic validation.
    /// </summary>
    public string? CertificateFilePath { get; set; }

    /// <summary>
    /// Gets or sets the password for the configured X.509 certificate.
    /// </summary>
    public string? CertificatePassword { get; set; }

    /// <summary>
    /// Gets or sets the Base64 representation of the X.509 certificate used for cryptographic validation.
    /// </summary>
    public string? CertificateBase64 { get; set; }

    /// <summary>
    /// Gets or sets the PEM format representation of the X.509 certificate used for cryptographic validation.
    /// </summary>
    public string? CertificatePem { get; set; }

    /// <summary>
    /// Gets or sets the thumbprint of the X.509 certificate to load from the system store.
    /// </summary>
    public string? CertificateThumbprint { get; set; }

    /// <summary>
    /// Gets or sets the store name when loading the certificate from the system store. Defaults to My.
    /// </summary>
    public StoreName CertificateStoreName { get; set; } = StoreName.My;

    /// <summary>
    /// Gets or sets the store location when loading the certificate from the system store. Defaults to CurrentUser.
    /// </summary>
    public StoreLocation CertificateStoreLocation { get; set; } = StoreLocation.CurrentUser;

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

        return DeclaredLicenseType == other.DeclaredLicenseType &&
               string.Equals(LicenseKey, other.LicenseKey, StringComparison.Ordinal) &&
               string.Equals(LicenseFilePath, other.LicenseFilePath, StringComparison.Ordinal) &&
               string.Equals(CertificateFilePath, other.CertificateFilePath, StringComparison.Ordinal) &&
               string.Equals(CertificatePassword, other.CertificatePassword, StringComparison.Ordinal) &&
               string.Equals(CertificateBase64, other.CertificateBase64, StringComparison.Ordinal) &&
               string.Equals(CertificatePem, other.CertificatePem, StringComparison.Ordinal) &&
               string.Equals(CertificateThumbprint, other.CertificateThumbprint, StringComparison.Ordinal) &&
               CertificateStoreName == other.CertificateStoreName &&
               CertificateStoreLocation == other.CertificateStoreLocation;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        return ReferenceEquals(this, obj) || (obj is LicenseOptions other && Equals(other));
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(DeclaredLicenseType);
        hash.Add(LicenseKey);
        hash.Add(LicenseFilePath);
        hash.Add(CertificateFilePath);
        hash.Add(CertificatePassword);
        hash.Add(CertificateBase64);
        hash.Add(CertificatePem);
        hash.Add(CertificateThumbprint);
        hash.Add(CertificateStoreName);
        hash.Add(CertificateStoreLocation);
        return hash.ToHashCode();
    }
}