namespace Ama.Enterprise.Licensing.Services;

using System;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Ama.Enterprise.Licensing.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation tracking honor-based checks evaluating provided bounds and explicit JSON constraints structurally.
/// </summary>
public sealed class HonorLicenseManager : ILicenseManager
{
    private readonly LicenseOptions options;
    private readonly ICertificateLoader certificateLoader;
    private readonly ILogger<HonorLicenseManager> logger;

    /// <inheritdoc />
    public string LicenseType { get; private set; } = "Unknown";

    /// <inheritdoc />
    public string? LicenseId { get; private set; }

    /// <inheritdoc />
    public string? CompanyName { get; private set; }

    /// <inheritdoc />
    public string? ContactEmail { get; private set; }

    /// <inheritdoc />
    public string? Tier { get; private set; }

    /// <inheritdoc />
    public DateTimeOffset? RegistrationDate { get; private set; }

    /// <inheritdoc />
    public DateTimeOffset? ExpirationDate { get; private set; }

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
        if (OperatingSystem.IsBrowser())
        {
            if (options.DeclaredLicenseType == DeclaredLicenseType.Enterprise || 
                !string.IsNullOrWhiteSpace(options.LicenseKey) || 
                !string.IsNullOrWhiteSpace(options.LicenseFilePath))
            {
                throw new InvalidOperationException("Enterprise licensing components detected in a client-side environment. Enterprise licenses must not be distributed or validated client-side.");
            }

            SetLicenseState("Client-Side");
            logger.LogInformation("Client-side environment detected. License validation is inherently delegated to the backend infrastructure.");
            return;
        }

        if (options.DeclaredLicenseType == DeclaredLicenseType.Community)
        {
            SetLicenseState("Community");
            logger.LogInformation("Community license terms accepted. Thank you for playing fair.");
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
                SetLicenseState("Unknown");
                logger.LogError("Enterprise license declared, but no valid license key or file path was provided.");
                return;
            }

            using var cert = GetConfiguredCertificate();
            var validPayload = VerifyLicense(key, cert);

            if (validPayload.HasValue)
            {
                LicenseType = "Enterprise";
                LicenseId = validPayload.Value.LicenseId;
                CompanyName = validPayload.Value.CompanyName;
                ContactEmail = validPayload.Value.ContactEmail;
                Tier = validPayload.Value.Tier;
                RegistrationDate = validPayload.Value.RegistrationDate;
                ExpirationDate = validPayload.Value.ExpirationDate;
                
                logger.LogInformation("Valid Enterprise license detected for {CompanyName}. Operating under: {LicenseType} Tier: {Tier}.", CompanyName ?? "Unknown Entity", LicenseType, Tier ?? "Standard");
            }
            else
            {
                SetLicenseState("Unknown");
                logger.LogError("Invalid Enterprise license key or certificate provided. Please update your license.");
            }

            return;
        }

        SetLicenseState("Unknown");
        logger.LogError("No valid license type declared. You must set DeclaredLicenseType to Community or Enterprise to accept the terms of use. Please use 'services.ConfigureAmaEnterpriseLicense' to configure a valid license.");
    }

    private void SetLicenseState(string licenseType)
    {
        LicenseType = licenseType;
        LicenseId = null;
        CompanyName = null;
        ContactEmail = null;
        Tier = null;
        RegistrationDate = null;
        ExpirationDate = null;
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

        var assembly = typeof(HonorLicenseManager).Assembly;
        
        return certificateLoader.LoadFromEmbeddedResource(assembly, "Ama.Enterprise.Licensing.ama-license.cer");
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CRDTPROJ0003:Avoid direct use of JSON serialization", Justification = "Licensing should only use Text JSON, do not mix with binary serialization")]
    private LicensePayload? VerifyLicense(string licenseKey, X509Certificate2? certificate)
    {
        try
        {
            if (certificate is null)
            {
                return null;
            }

            var parts = licenseKey.Split('.');
            if (parts.Length != 2)
            {
                return null;
            }

            var payloadBytes = Convert.FromBase64String(parts[0]);
            var signatureBytes = Convert.FromBase64String(parts[1]);

            using var rsa = certificate.GetRSAPublicKey();
            if (rsa is null)
            {
                logger.LogWarning("The configured certificate does not contain an RSA public key structure.");
                return null;
            }

            if (!rsa.VerifyData(payloadBytes, signatureBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1))
            {
                return null;
            }

            return JsonSerializer.Deserialize(payloadBytes, LicensingJsonContext.Default.LicensePayload);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Cryptographic license parsing or validation failed.");
            return null;
        }
    }
}