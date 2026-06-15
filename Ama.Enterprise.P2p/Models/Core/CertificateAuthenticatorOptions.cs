namespace Ama.Enterprise.P2p.Models.Core;

using System;
using System.Collections.Generic;
using System.Linq;

/// <summary>
/// Configuration options for validating peer certificates during P2P authentication.
/// </summary>
public sealed class CertificateAuthenticatorOptions : IEquatable<CertificateAuthenticatorOptions>
{
    /// <summary>
    /// Gets or sets the local certificate bytes used for the handshake data.
    /// </summary>
    public byte[]? LocalCertificateBytes { get; set; }

    /// <summary>
    /// Gets the set of explicitly allowed certificate thumbprints. 
    /// If populated, only certificates matching these thumbprints are accepted.
    /// </summary>
    public ISet<string> AllowedThumbprints { get; set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets a value indicating whether to validate the standard X.509 certificate chain.
    /// </summary>
    public bool ValidateCertificateChain { get; set; } = true;

    /// <summary>
    /// Gets a value indicating whether to allow unknown certificate authorities when validating the chain.
    /// </summary>
    public bool AllowUnknownCertificateAuthorities { get; set; } = false;

    /// <inheritdoc />
    public bool Equals(CertificateAuthenticatorOptions? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        bool localCertificatesEqual = (LocalCertificateBytes is null && other.LocalCertificateBytes is null) ||
                                      (LocalCertificateBytes is not null && other.LocalCertificateBytes is not null && LocalCertificateBytes.SequenceEqual(other.LocalCertificateBytes));
            
        return ValidateCertificateChain == other.ValidateCertificateChain &&
               AllowUnknownCertificateAuthorities == other.AllowUnknownCertificateAuthorities &&
               AllowedThumbprints.Count == other.AllowedThumbprints.Count &&
               AllowedThumbprints.All(other.AllowedThumbprints.Contains) &&
               localCertificatesEqual;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as CertificateAuthenticatorOptions);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(ValidateCertificateChain, AllowUnknownCertificateAuthorities, AllowedThumbprints.Count, LocalCertificateBytes?.Length ?? 0);
}