namespace Ama.Enterprise.Licensing.Services;

using System;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Ama.Enterprise.Licensing.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation tracking honor-based checks evaluating provided bounds.
/// </summary>
public sealed class HonorLicenseManager : ILicenseManager
{
    private readonly LicenseOptions options;
    private readonly ICertificateLoader certificateLoader;
    private readonly ILogger<HonorLicenseManager> logger;

    /// <inheritdoc />
    public string LicenseType { get; private set; } = "Unknown";

    /// <summary>
    /// Initializes a new instance of the <see cref="HonorLicenseManager"/> class.
    /// </summary>
    public HonorLicenseManager(
        IOptions<LicenseOptions> options, 
        ICertificateLoader certificateLoader,
        ILogger<HonorLicenseManager> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(certificateLoader);
        ArgumentNullException.ThrowIfNull(logger);

        this.options = options.Value;
        this.certificateLoader = certificateLoader;
        this.logger = logger;
    }

    /// <inheritdoc />
    public void ValidateLicense()
    {
        if (options.DeclaredLicenseType == DeclaredLicenseType.OpenSource)
        {
            LicenseType = "Open Source";
            logger.LogInformation("Open Source license terms accepted. Thank you for playing fair.");
            return;
        }

        if (options.DeclaredLicenseType == DeclaredLicenseType.Enterprise)
        {
            var key = options.LicenseKey;

            if (string.IsNullOrWhiteSpace(key) && !string.IsNullOrWhiteSpace(options.LicenseFilePath))
            {
                key = LoadLicenseFromFile(options.LicenseFilePath);
            }

            if (string.IsNullOrWhiteSpace(key))
            {
                LicenseType = "Unknown";
                logger.LogError("Enterprise license declared, but no valid license key or file path was provided.");
                return;
            }

            using var cert = GetConfiguredCertificate();

            if (VerifyLicense(key, cert))
            {
                LicenseType = "Enterprise";
                logger.LogInformation("Valid Enterprise license detected. Operating under: {LicenseType}.", LicenseType);
            }
            else
            {
                LicenseType = "Unknown";
                logger.LogError("Invalid Enterprise license key or certificate provided. Execution may be restricted.");
            }

            return;
        }

        LicenseType = "Unknown";
        logger.LogError("No valid license type declared. You must set DeclaredLicenseType to OpenSource or Enterprise to accept the terms of use.");
    }

    private string? LoadLicenseFromFile(string filePath)
    {
        try
        {
            if (File.Exists(filePath))
            {
                return File.ReadAllText(filePath).Trim();
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to read the specific license file.");
        }

        return null;
    }

    private X509Certificate2? GetConfiguredCertificate()
    {
        if (!string.IsNullOrWhiteSpace(options.CertificatePem))
        {
            return certificateLoader.LoadFromPem(options.CertificatePem);
        }

        if (!string.IsNullOrWhiteSpace(options.CertificateFilePath))
        {
            return certificateLoader.LoadFromFile(options.CertificateFilePath, options.CertificatePassword);
        }

        if (!string.IsNullOrWhiteSpace(options.CertificateBase64))
        {
            return certificateLoader.LoadFromBase64(options.CertificateBase64, options.CertificatePassword);
        }

        if (!string.IsNullOrWhiteSpace(options.CertificateThumbprint))
        {
            return certificateLoader.LoadFromStore(options.CertificateThumbprint, options.CertificateStoreName, options.CertificateStoreLocation);
        }

        return null;
    }

    private bool VerifyLicense(string licenseKey, X509Certificate2? certificate)
    {
        try
        {
            if (certificate is null)
            {
                return false;
            }

            var parts = licenseKey.Split('.');
            if (parts.Length != 2)
            {
                return false;
            }

            var payloadBytes = Convert.FromBase64String(parts[0]);
            var signatureBytes = Convert.FromBase64String(parts[1]);

            using var rsa = certificate.GetRSAPublicKey();
            if (rsa is null)
            {
                logger.LogWarning("The configured certificate does not contain an RSA public key structure.");
                return false;
            }

            return rsa.VerifyData(payloadBytes, signatureBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Cryptographic license validation failed.");
            return false;
        }
    }
}